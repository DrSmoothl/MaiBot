from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncio
import numpy as np
import pytest

from src.A_memorix.core.runtime import sdk_memory_kernel as kernel_module
from src.A_memorix.core.runtime.sdk_memory_kernel import SDKMemoryKernel


class _FakeEmbeddingManager:
    def __init__(self, dimension: int = 8) -> None:
        self.default_dimension = dimension
        self.encode_calls: list[Any] = []
        self.detect_calls = 0

    async def _detect_dimension(self) -> int:
        self.detect_calls += 1
        return self.default_dimension

    async def encode(self, text: Any, **kwargs: Any) -> np.ndarray:
        del kwargs
        self.encode_calls.append(text)

        def _encode_one(raw: Any) -> np.ndarray:
            content = str(raw or "")
            vector = np.zeros(self.default_dimension, dtype=np.float32)
            for index, byte in enumerate(content.encode("utf-8")):
                vector[index % self.default_dimension] += float((byte % 17) + 1)
            norm = float(np.linalg.norm(vector))
            if norm > 0:
                vector /= norm
            return vector

        if isinstance(text, (list, tuple)):
            return np.stack([_encode_one(item) for item in text]).astype(np.float32)
        return _encode_one(text).astype(np.float32)

    async def encode_batch(self, texts: Any, **kwargs: Any) -> np.ndarray:
        return await self.encode(texts, **kwargs)


def _kernel_config(data_dir: Path, dimension: int) -> dict[str, Any]:
    return {
        "storage": {"data_dir": str(data_dir.resolve())},
        "advanced": {"enable_auto_save": False},
        "embedding": {
            "dimension": dimension,
            "batch_size": 2,
            "paragraph_vector_backfill": {"enabled": False},
        },
        "retrieval": {
            "relation_vectorization": {"enabled": True},
            "sparse": {"enabled": False},
            "enable_ppr": False,
            "enable_parallel": False,
        },
    }


async def _fake_runtime_self_check(**kwargs: Any) -> dict[str, Any]:
    vector_store = kwargs["vector_store"]
    embedding_manager = kwargs["embedding_manager"]
    dimension = int(embedding_manager.default_dimension)
    return {
        "ok": int(vector_store.dimension) == dimension,
        "message": "ok",
        "configured_dimension": dimension,
        "requested_dimension": dimension,
        "vector_store_dimension": int(vector_store.dimension),
        "detected_dimension": dimension,
        "encoded_dimension": dimension,
        "elapsed_ms": 0.0,
        "sample_text": "test",
        "checked_at": 1_777_000_000.0,
    }


@pytest.mark.asyncio
async def test_runtime_admin_rebuild_all_vectors_replaces_existing_store(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_embedding_manager = _FakeEmbeddingManager(dimension=8)
    data_dir = tmp_path / "a_memorix_data"
    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: fake_embedding_manager,
    )
    monkeypatch.setattr(kernel_module, "run_embedding_runtime_self_check", _fake_runtime_self_check)

    kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root",
        config=_kernel_config(data_dir, fake_embedding_manager.default_dimension),
    )

    await kernel.initialize()
    assert kernel.metadata_store is not None
    assert kernel.vector_store is not None

    paragraph_hash = kernel.metadata_store.add_paragraph("用户喜欢蓝色围巾", source="test")
    entity_hash = kernel.metadata_store.add_entity("蓝色围巾")
    relation_hash = kernel.metadata_store.add_relation("用户", "喜欢", "蓝色围巾")
    inactive_relation_hash = kernel.metadata_store.add_relation("旧用户", "忘记", "旧围巾")
    kernel.metadata_store.mark_relations_inactive([inactive_relation_hash])
    old_summary_importer_store = kernel.summary_importer.vector_store
    old_profile_store = kernel.person_profile_service.vector_store
    old_episode_retriever = kernel.episode_retriever.retriever
    kernel.vector_store.add(
        np.ones((1, fake_embedding_manager.default_dimension), dtype=np.float32),
        ["stale-vector"],
    )
    kernel.vector_store.save()

    preview = await kernel.memory_runtime_admin(action="rebuild_all_vectors", dry_run=True)
    assert preview["success"] is True
    assert preview["counts"] == {"paragraphs": 1, "entities": 1, "relations": 1}

    result = await kernel.memory_runtime_admin(action="rebuild_all_vectors", batch_size=2)
    assert result["success"] is True
    assert result["done"] == 3
    assert result["failed"] == 0
    assert paragraph_hash in kernel.vector_store
    assert entity_hash in kernel.vector_store
    assert relation_hash in kernel.vector_store
    assert inactive_relation_hash not in kernel.vector_store
    assert "stale-vector" not in kernel.vector_store
    assert kernel.summary_importer.vector_store is kernel.vector_store
    assert kernel.person_profile_service.vector_store is kernel.vector_store
    assert kernel.episode_retriever.retriever is kernel.retriever
    assert kernel.summary_importer.vector_store is not old_summary_importer_store
    assert kernel.person_profile_service.vector_store is not old_profile_store
    assert kernel.episode_retriever.retriever is not old_episode_retriever

    config = await kernel.memory_runtime_admin(action="get_config")
    assert config["vector_rebuild_required"] is False
    assert config["stored_vector_dimension"] == fake_embedding_manager.default_dimension

    await kernel.shutdown()


@pytest.mark.asyncio
async def test_runtime_admin_rebuild_all_vectors_rejects_concurrent_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_embedding_manager = _FakeEmbeddingManager(dimension=8)
    data_dir = tmp_path / "a_memorix_data"
    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: fake_embedding_manager,
    )
    monkeypatch.setattr(kernel_module, "run_embedding_runtime_self_check", _fake_runtime_self_check)

    kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root",
        config=_kernel_config(data_dir, fake_embedding_manager.default_dimension),
    )

    await kernel.initialize()
    assert kernel.metadata_store is not None
    kernel.metadata_store.add_paragraph("并发重建测试", source="test")

    original_encode = kernel._encode_and_add_rebuild_vectors
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_encode(**kwargs: Any):
        if not started.is_set():
            started.set()
            await release.wait()
        return await original_encode(**kwargs)

    monkeypatch.setattr(kernel, "_encode_and_add_rebuild_vectors", _slow_encode)
    first_task = asyncio.create_task(kernel.memory_runtime_admin(action="rebuild_all_vectors", batch_size=1))
    try:
        await asyncio.wait_for(started.wait(), timeout=1.0)
        second = await kernel.memory_runtime_admin(action="rebuild_all_vectors", batch_size=1)
        assert second["success"] is False
        assert second["error"] == "vector_rebuild_running"
    finally:
        release.set()

    first = await first_task
    assert first["success"] is True

    await kernel.shutdown()


@pytest.mark.asyncio
async def test_initialize_dimension_mismatch_keeps_vector_store_empty_until_rebuild(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "a_memorix_data"
    first_embedding_manager = _FakeEmbeddingManager(dimension=8)
    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: first_embedding_manager,
    )
    monkeypatch.setattr(kernel_module, "run_embedding_runtime_self_check", _fake_runtime_self_check)

    first_kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root_first",
        config=_kernel_config(data_dir, first_embedding_manager.default_dimension),
    )
    await first_kernel.initialize()
    assert first_kernel.vector_store is not None
    first_kernel.vector_store.add(
        np.ones((1, first_embedding_manager.default_dimension), dtype=np.float32),
        ["old-dimension-vector"],
    )
    first_kernel.vector_store.save()
    await first_kernel.shutdown()

    second_embedding_manager = _FakeEmbeddingManager(dimension=12)
    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: second_embedding_manager,
    )

    second_kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root_second",
        config=_kernel_config(data_dir, second_embedding_manager.default_dimension),
    )
    await second_kernel.initialize()
    assert second_kernel.vector_store is not None
    assert second_kernel.vector_store.dimension == first_embedding_manager.default_dimension
    assert second_kernel.vector_store.num_vectors == 1
    assert "old-dimension-vector" in second_kernel.vector_store

    config = await second_kernel.memory_runtime_admin(action="get_config")
    assert config["vector_rebuild_required"] is False

    recover = await second_kernel.memory_runtime_admin(action="recover_embedding")
    assert recover["success"] is False
    assert recover["detail"] == "dimension_mismatch"

    config = await second_kernel.memory_runtime_admin(action="get_config")
    assert config["embedding_dimension"] == second_embedding_manager.default_dimension
    assert config["vector_rebuild_required"] is True
    assert config["stored_vector_dimension"] == first_embedding_manager.default_dimension
    assert config["embedding_degraded"] is True

    await second_kernel.shutdown()

    third_kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root_third",
        config=_kernel_config(data_dir, second_embedding_manager.default_dimension),
    )
    await third_kernel.initialize()
    assert third_kernel.vector_store is not None
    assert third_kernel.vector_store.dimension == first_embedding_manager.default_dimension

    config = await third_kernel.memory_runtime_admin(action="get_config")
    assert config["vector_rebuild_required"] is False

    recover = await third_kernel.memory_runtime_admin(action="recover_embedding")
    assert recover["detail"] == "dimension_mismatch"

    result = await third_kernel.memory_runtime_admin(action="rebuild_all_vectors", batch_size=2)
    assert result["success"] is True

    await third_kernel.shutdown()

    fourth_kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root_fourth",
        config=_kernel_config(data_dir, second_embedding_manager.default_dimension),
    )
    await fourth_kernel.initialize()

    config = await fourth_kernel.memory_runtime_admin(action="get_config")
    assert config["vector_rebuild_required"] is False
    assert config["stored_vector_dimension"] == second_embedding_manager.default_dimension

    await fourth_kernel.shutdown()


@pytest.mark.asyncio
async def test_initialize_defers_real_embedding_self_check(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_embedding_manager = _FakeEmbeddingManager(dimension=8)
    data_dir = tmp_path / "a_memorix_data"
    self_check_calls: list[dict[str, Any]] = []

    async def _recording_runtime_self_check(**kwargs: Any) -> dict[str, Any]:
        self_check_calls.append(kwargs)
        return await _fake_runtime_self_check(**kwargs)

    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: fake_embedding_manager,
    )
    monkeypatch.setattr(kernel_module, "run_embedding_runtime_self_check", _recording_runtime_self_check)

    kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root",
        config=_kernel_config(data_dir, fake_embedding_manager.default_dimension),
    )

    await kernel.initialize()
    try:
        assert fake_embedding_manager.encode_calls == []
        assert fake_embedding_manager.detect_calls == 0
        assert self_check_calls == []
        report = kernel._runtime_facade._runtime_self_check_report
        assert report["code"] == "startup_self_check_deferred"

        result = await kernel.memory_runtime_admin(action="self_check")
        assert result["success"] is True
        assert len(self_check_calls) == 1
    finally:
        await kernel.shutdown()


@pytest.mark.asyncio
async def test_deferred_self_check_marks_dimension_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_embedding_manager = _FakeEmbeddingManager(dimension=12)
    data_dir = tmp_path / "a_memorix_data"

    async def _dimension_mismatch_self_check(**kwargs: Any) -> dict[str, Any]:
        vector_store = kwargs["vector_store"]
        return {
            "ok": False,
            "message": "dimension mismatch",
            "configured_dimension": 12,
            "requested_dimension": 12,
            "vector_store_dimension": int(vector_store.dimension),
            "detected_dimension": 12,
            "encoded_dimension": 12,
            "elapsed_ms": 0.0,
            "sample_text": "test",
            "checked_at": 1_777_000_000.0,
        }

    monkeypatch.setattr(
        kernel_module,
        "create_embedding_api_adapter",
        lambda **kwargs: fake_embedding_manager,
    )
    monkeypatch.setattr(kernel_module, "run_embedding_runtime_self_check", _dimension_mismatch_self_check)

    kernel = SDKMemoryKernel(
        plugin_root=tmp_path / "plugin_root",
        config=_kernel_config(data_dir, 8),
    )

    await kernel.initialize()
    try:
        assert kernel.vector_store is not None
        assert kernel.vector_store.dimension == 8
        assert kernel._is_embedding_degraded() is False

        result = await kernel.memory_runtime_admin(action="self_check")
        assert result["success"] is False

        config = await kernel.memory_runtime_admin(action="get_config")
        assert config["embedding_dimension"] == 12
        assert config["vector_rebuild_required"] is True
        assert config["stored_vector_dimension"] == 8
        assert config["embedding_degraded"] is True
    finally:
        await kernel.shutdown()
