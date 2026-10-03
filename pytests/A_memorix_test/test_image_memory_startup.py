from contextvars import ContextVar
from io import BytesIO
from pathlib import Path
from threading import Event, get_ident
from types import SimpleNamespace
from typing import Any, AsyncIterator, Dict, Iterator, List, Tuple
from PIL import Image

import asyncio
import pytest
import pytest_asyncio

from src.A_memorix import host_service as host_module
from src.A_memorix.core.image.asset_store import ImageAssetStore
from src.A_memorix.core.image.runtime import ImageMemoryRuntime
from src.A_memorix.core.runtime import sdk_memory_kernel as kernel_module
from src.A_memorix.core.runtime.runtime_writer_lock import RuntimeWriterLock
from src.A_memorix.core.runtime.sdk_memory_kernel import SDKMemoryKernel
from src.A_memorix.core.storage import MetadataStore
from src.plugin_runtime.host.rpc_server import RPCServer
from src.plugin_runtime.runner.rpc_client import RPCClient
from src.plugin_runtime.transport.factory import create_transport_server


class _NoEmbedding:
    async def probe(self) -> Dict[str, Any]:
        raise AssertionError("Recovery must not probe embeddings")

    async def embed(self, image_bytes: bytes, *, mime_type: str, session_id: str = "") -> Dict[str, Any]:
        raise AssertionError("Recovery must not request embeddings")


@pytest.fixture
def memory(tmp_path: Path) -> Iterator[Tuple[MetadataStore, ImageMemoryRuntime]]:
    metadata = MetadataStore(data_dir=tmp_path / "metadata")
    metadata.connect()
    close = metadata.close
    try:
        yield metadata, ImageMemoryRuntime(
            metadata_store=metadata,
            asset_store=ImageAssetStore(tmp_path / "assets", max_bytes=1024 * 1024, max_pixels=1024),
            embedder=_NoEmbedding(),
            vector_root=tmp_path / "vectors",
            config={},
            persist_vector_store=lambda store, fingerprint: pytest.fail("Unexpected vector publication"),
        )
    finally:
        close()


@pytest_asyncio.fixture
async def held_recovery(memory, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[SimpleNamespace]:
    metadata, runtime = memory
    loop = asyncio.get_running_loop()
    state = SimpleNamespace(started=asyncio.Event(), release=Event(), events=[], error=None)
    reconcile = runtime.reconcile_assets

    def recover() -> Dict[str, Any]:
        state.events.append("worker_started")
        loop.call_soon_threadsafe(state.started.set)
        try:
            assert state.release.wait(timeout=20), "Recovery worker was not released"
            assert metadata.query("SELECT 1 AS alive") == [{"alive": 1}]
            if state.error is not None:
                raise state.error
            return reconcile()
        finally:
            state.events.append("worker_exited")

    monkeypatch.setattr(runtime, "reconcile_assets", recover)
    try:
        yield state
    finally:
        state.release.set()


@pytest_asyncio.fixture
async def startup_kernel(tmp_path, memory, held_recovery, monkeypatch) -> AsyncIterator[SDKMemoryKernel]:
    metadata, runtime = memory
    kernel = SDKMemoryKernel(plugin_root=tmp_path, config={"storage": {"data_dir": str(tmp_path.resolve())}})
    events = held_recovery.events
    close, release = metadata.close, kernel._runtime_writer_lock.release

    def close_metadata() -> None:
        events.append("metadata_close")
        close()

    def release_writer() -> None:
        if kernel._runtime_writer_lock.held:
            events.append("unlock")
        release()

    async def start_background() -> None:
        events.append("background")

    async def initialize() -> None:
        kernel.metadata_store, kernel.image_memory_runtime = metadata, runtime
        await runtime.recover_assets()
        kernel._initialized = True
        await kernel._start_background_tasks()

    monkeypatch.setattr(metadata, "close", close_metadata)
    monkeypatch.setattr(kernel._runtime_writer_lock, "release", release_writer)
    # The full production assembly order is covered in test_runtime_lifecycle_boundaries.
    monkeypatch.setattr(kernel, "_initialize_with_writer_lock", initialize, raising=False)
    monkeypatch.setattr(kernel, "_start_background_tasks", start_background)
    try:
        yield kernel
    finally:
        held_recovery.release.set()
        await asyncio.wait_for(kernel.shutdown(), timeout=5)


@pytest_asyncio.fixture
async def host_side(startup_kernel, held_recovery, monkeypatch) -> AsyncIterator[host_module.AMemorixHostService]:
    service = host_module.AMemorixHostService()
    monkeypatch.setattr(service, "_read_config", lambda: startup_kernel.config)
    monkeypatch.setattr(kernel_module, "SDKMemoryKernel", lambda **kwargs: startup_kernel)
    monkeypatch.setattr(host_module, "set_runtime_kernel", lambda kernel: None)
    try:
        yield service
    finally:
        held_recovery.release.set()
        await asyncio.wait_for(service.stop(), timeout=5)


@pytest.mark.asyncio
async def test_constructor_defers_reconciliation_and_recovery_copies_context_off_loop(memory, monkeypatch) -> None:
    metadata, original = memory
    context = ContextVar("image_recovery_test", default="missing")
    thread = get_ident()
    calls: List[Tuple[int, str]] = []

    def reconcile(runtime: ImageMemoryRuntime) -> Dict[str, Any]:
        calls.append((get_ident(), context.get()))
        assert runtime.metadata_store.query("SELECT 1 AS alive") == [{"alive": 1}]
        return {"removed_files": 2, "issues": []}

    monkeypatch.setattr(ImageMemoryRuntime, "reconcile_assets", reconcile)
    runtime = ImageMemoryRuntime(
        metadata_store=metadata, asset_store=original.asset_store, embedder=_NoEmbedding(),
        vector_root=original.vector_root, config={}, persist_vector_store=original.persist_vector_store,
    )
    assert calls == []
    assert runtime.recovery == {"removed_files": 0, "issues": []}
    token = context.set("startup-context")
    try:
        await runtime.recover_assets()
    finally:
        context.reset(token)
    assert len(calls) == 1
    assert calls[0][0] != thread and calls[0][1] == "startup-context"
    assert runtime.recovery == {"removed_files": 2, "issues": []}


@pytest.mark.asyncio
async def test_rpc_handshake_stays_responsive_during_recovery_beyond_ten_seconds(
    memory, held_recovery, startup_kernel, host_side,
) -> None:
    metadata, runtime = memory
    transport = create_transport_server()  # Named pipes on Windows; short default UDS elsewhere.
    server = RPCServer(transport, session_token="image-startup-test", host_version="1.0.0")
    client = RPCClient(transport.get_address(), "image-startup-test")
    publication = None
    try:
        await server.start()
        await host_side.start()
        await asyncio.wait_for(held_recovery.started.wait(), timeout=5)
        output = BytesIO()
        Image.new("RGB", (2, 2)).save(output, format="PNG")
        publication = asyncio.create_task(runtime.ingest(
            image_bytes=output.getvalue(), source_kind="chat", scope_type="chat", external_ref="startup", chat_id="test",
        ))
        # Keep the worker held beyond the real, unmodified RPC handshake budget.
        hold_started = asyncio.get_running_loop().time()
        assert await asyncio.wait_for(client.connect_and_handshake(), timeout=10.0)
        assert asyncio.get_running_loop().time() - hold_started < 10.0
        await asyncio.sleep(max(0.0, 10.1 - (asyncio.get_running_loop().time() - hold_started)))
        assert not held_recovery.release.is_set() and "worker_exited" not in held_recovery.events
        assert host_side._runtime_state == "migrating" and host_side._kernel is None
        assert not startup_kernel._initialized and "background" not in held_recovery.events
        assert not publication.done() and runtime._publication_lock.locked()
        assert metadata.query("SELECT * FROM image_assets") == []
        assert not list(runtime.asset_store.assets_root.iterdir())
        held_recovery.release.set()
        await asyncio.wait_for(host_side._startup_task, timeout=5)
        assert (await asyncio.wait_for(publication, timeout=5))["success"]
        assert host_side._runtime_state == "ready" and startup_kernel._initialized
        assert held_recovery.events.index("worker_exited") < held_recovery.events.index("background")
    finally:
        held_recovery.release.set()
        if publication is not None:
            await asyncio.gather(publication, return_exceptions=True)
        await host_side.stop()
        await client.disconnect()
        await server.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["initialize", "cancel", "stop", "reload"])
async def test_cancellation_keeps_database_and_writer_until_recovery_exits(
    operation, memory, held_recovery, startup_kernel, host_side,
) -> None:
    metadata, runtime = memory
    if operation == "initialize":
        startup = asyncio.create_task(startup_kernel.initialize())
    else:
        await host_side.start()
        startup = host_side._startup_task
    shutdown = None
    contender = RuntimeWriterLock(startup_kernel._runtime_writer_lock.path)
    try:
        await asyncio.wait_for(held_recovery.started.wait(), timeout=5)
        if operation == "reload":
            startup_kernel.config["plugin"] = {"enabled": False}
            shutdown = asyncio.create_task(host_side.reload())
        elif operation == "stop":
            shutdown = asyncio.create_task(host_side.stop())
        elif operation == "initialize":
            shutdown = asyncio.create_task(startup_kernel.shutdown())
        await asyncio.sleep(0)
        for _ in range(3):
            startup.cancel()
            await asyncio.sleep(0)
            assert not startup.done()
            assert shutdown is None or not shutdown.done()
            assert metadata.query("SELECT 1 AS alive") == [{"alive": 1}]
            assert startup_kernel._runtime_writer_lock.held and runtime._publication_lock.locked()
            assert held_recovery.events == ["worker_started"]
            with pytest.raises(RuntimeError, match="已有活动写者"):
                contender.acquire()
    finally:
        held_recovery.release.set()
        tasks = [startup] + ([shutdown] if shutdown is not None else [])
        results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5)
        contender.release()
    assert isinstance(results[0], asyncio.CancelledError)
    assert all(result is None for result in results[1:])
    assert held_recovery.events == ["worker_started", "worker_exited", "metadata_close", "unlock"]
    assert metadata._conn is None and startup_kernel.metadata_store is None
    assert startup_kernel.image_memory_runtime is None and not startup_kernel._initialized
    assert not startup_kernel._runtime_writer_lock.held
    assert host_side._runtime_state == "stopped" and host_side._kernel is None
    contender.acquire()
    contender.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["kernel", "host"])
async def test_recovery_failure_surfaces_without_ready_and_cleans_resources(
    entrypoint, memory, held_recovery, startup_kernel, host_side,
) -> None:
    held_recovery.error = OSError("image recovery failed")
    held_recovery.release.set()
    if entrypoint == "kernel":
        with pytest.raises(OSError, match="image recovery failed"):
            await asyncio.wait_for(startup_kernel.initialize(), timeout=5)
    else:
        await host_side.start()
        await asyncio.wait_for(host_side._startup_task, timeout=5)
        assert host_side._runtime_state == "failed" and host_side._kernel is None
        assert host_side._startup_error == "image recovery failed"
    assert held_recovery.events == ["worker_started", "worker_exited", "metadata_close", "unlock"]
    assert not startup_kernel._initialized and not startup_kernel._background_tasks
    assert startup_kernel.metadata_store is None and startup_kernel.image_memory_runtime is None
    assert memory[0]._conn is None and not startup_kernel._runtime_writer_lock.held
