"""文件权限、配额、传输恢复与实际 HTTP 下载边界的回归验证。"""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from aiohttp import web
from sqlmodel import SQLModel, create_engine

import asyncio
import hashlib
import threading
import time

import pytest

from src.files.config import FilesConfig
from src.files.models import FileAsset
from src.files.network import download_chunks, validate_url
from src.files.service import FileService, disk_call
from src.files.store import FileBusyError, FileLimitError, FileStore


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'files.db'}", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    value = FileStore(tmp_path / "blobs", engine, FilesConfig(max_file_mib=1))
    yield value
    engine.dispose()


def upload(store, content=b"hello", name="a.txt", sid="a"):
    asset = store.begin_upload(sid, name)
    store.append(asset.file_ref, asset.transfer_id, content)
    return store.finish(asset.file_ref, asset.transfer_id)


def test_upload_authorization_and_snapshot(store):
    asset = upload(store)
    assert store.read_bytes(asset.file_ref, "a") == b"hello"
    assert asset.sha256 == hashlib.sha256(b"hello").hexdigest()
    with pytest.raises(PermissionError):
        store.read_bytes(asset.file_ref, "b")
    assert store.list("b") == []
    assert "source" not in store.public(asset)
    assert "session_ids" not in store.list("a")[0]
    store.grant(asset.file_ref, "b")
    assert store.read_bytes(asset.file_ref, "b") == b"hello"
    store.lease(asset.file_ref, "a")
    with pytest.raises(FileBusyError):
        store.remove(asset.file_ref)


def test_actual_size_limit_releases_reservation(store):
    asset = store.begin_upload("a", "binary.bin")
    with pytest.raises(FileLimitError):
        store.append(asset.file_ref, asset.transfer_id, b"x" * (1024 ** 2 + 1))
    store.fail(asset.file_ref, asset.transfer_id, "oversize")
    assert store.usage()["reserved_bytes"] == 0
    assert not store.path(asset.file_ref, partial=True).exists()
    with pytest.raises(PermissionError):
        store.append(asset.file_ref, asset.transfer_id, b"late write")


def test_quota_atomic_across_threads(store):
    # 使用小配额以并发验证实际预留逻辑，不创建 GiB 测试文件。
    store.config = store.config.model_copy(update={"max_storage_gib": 2 / 1024, "max_concurrent_transfers": 8})
    def begin(index):
        try:
            return store.begin_upload("a", f"{index}.txt")
        except FileLimitError:
            return None
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(begin, range(6)))
    assert sum(asset is not None for asset in results) == 2
    assert store.usage()["reserved_bytes"] == 2 * 1024 ** 2


def test_metadata_registration_does_not_download(store):
    first = store.register_remote("a", "m1", "f.txt", 100, "text/plain", {"file_id": "platform"}, "key")
    again = store.register_remote("a", "m2", "f.txt", 100, "text/plain", {"file_id": "platform"}, "key")
    assert first.file_ref == again.file_ref
    assert first.file_ref != "platform"
    assert store.usage()["used_bytes"] == store.usage()["reserved_bytes"] == 0
    assert list(store.root.iterdir()) == []


def test_cleanup_preserves_originals_and_active_downloads(store):
    original = upload(store)
    remote = store.register_remote("a", "m", "cache.txt", 0, "", {}, "remote")
    transfer = store.begin_download(remote.file_ref, "a")
    store.append(remote.file_ref, transfer.transfer_id, b"cache")
    remote = store.finish(remote.file_ref, transfer.transfer_id)
    active = store.begin_upload("a", "active.txt")
    with store.transaction() as session:
        for asset in (original, remote):
            record = session.get(FileAsset, asset.file_ref)
            record.accessed_at = time.time() - 31 * 86400
            session.add(record)
    assert store.cleanup() == {"expired": 1, "interrupted": 0}
    assert store.read_bytes(original.file_ref, "a") == b"hello"
    assert store.get(remote.file_ref, "a").state == "remote"
    assert store.get(active.file_ref, "a").state == "uploading"
    assert store.cleanup(startup=True)["interrupted"] == 1
    assert store.usage()["reserved_bytes"] == 0


def test_text_pagination_binary_rejection_and_quota(store):
    service = FileService(store, enabled_provider=lambda: True)
    asset = upload(store, "中文 test".encode())
    page = service.read_text(asset.file_ref, "a", offset=1, limit=2)
    assert page["text"] == "文 " and page["next_offset"] == 3 and page["untrusted_content"]
    with pytest.raises(PermissionError):
        service.read_text(asset.file_ref, "b")
    binary = upload(store, b"a\x00b")
    with pytest.raises(ValueError, match="二进制"):
        service.read_text(binary.file_ref, "a")
    pdf = upload(store, b"%PDF", "a.pdf")
    with pytest.raises(ValueError, match="仅解析"):
        service.read_text(pdf.file_ref, "a")


@pytest.mark.parametrize("url", ["file:///etc/passwd", "http://127.0.0.1/x", "http://[::1]/", "http://169.254.169.254/", "https://user:pass@example.com/"])
def test_ssrf_rejected(url):
    with pytest.raises((ValueError, PermissionError)):
        validate_url(url, [])


@pytest.mark.asyncio
async def test_stream_download_and_redirect_revalidation():
    app = web.Application()
    async def content(request):
        return web.Response(body=b"streamed file")
    async def redirect(request):
        raise web.HTTPFound("http://127.0.0.1:1/private")
    app.router.add_get("/content", content)
    app.router.add_get("/redirect", redirect)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    origin = f"http://127.0.0.1:{runner.addresses[0][1]}"
    try:
        assert b"".join([chunk async for chunk in download_chunks(origin + "/content", [origin], 5)]) == b"streamed file"
        with pytest.raises(PermissionError):
            _ = [chunk async for chunk in download_chunks(origin + "/redirect", [origin], 5)]
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_cancellation_waits_for_disk_write():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    def write():
        entered.set()
        release.wait(2)
        finished.set()
    task = asyncio.create_task(disk_call(write))
    await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()


@pytest.mark.asyncio
async def test_failed_download_cleans_partial_and_can_retry(store, monkeypatch):
    service = FileService(store, enabled_provider=lambda: True)
    asset = store.register_remote("a", "m", "a.txt", 0, "", {}, "source")
    async def resolve(source):
        return "https://example.com/file"
    async def interrupted(*args):
        yield b"partial"
        raise OSError("connection lost")
    monkeypatch.setattr(service, "resolve_url", resolve)
    monkeypatch.setattr("src.files.service.download_chunks", interrupted)
    with pytest.raises(OSError):
        await service.ensure(asset.file_ref, "a")
    assert store.get(asset.file_ref, "a").state == "failed"
    assert store.usage()["reserved_bytes"] == 0
    assert not store.path(asset.file_ref, partial=True).exists()
    retry = store.begin_download(asset.file_ref, "a")
    assert retry.transfer_id


def test_token_purpose_expiry_and_revocation(store):
    service = FileService(store, enabled_provider=lambda: True)
    token = service.ticket("plugin", plugin_id="p", session_id="a")
    with pytest.raises(PermissionError):
        service.claims(token, "download")
    assert service.claims(token, "plugin")["session_id"] == "a"
    service.revoke(token)
    with pytest.raises(PermissionError):
        service.claims(token, "plugin")
    expired = service.ticket("download", lifetime=-1, file_ref="f")
    with pytest.raises(PermissionError):
        service.claims(expired, "download")


def test_reader_protects_file_until_response_closes(store):
    asset = upload(store)
    with store.reader(asset.file_ref) as handle:
        with pytest.raises(FileBusyError):
            store.remove(asset.file_ref)
        assert handle.read() == b"hello"
    assert store.get(asset.file_ref, "a").open_readers == 0
    store.remove(asset.file_ref)
    with pytest.raises(FileNotFoundError):
        store.get(asset.file_ref, "a")


def test_startup_removes_orphans_but_keeps_unrelated_files(store):
    orphan = store.root / ("a" * 32 + ".part")
    orphan.write_bytes(b"interrupted before commit")
    unrelated = store.root / "notes.txt"
    unrelated.write_text("manual notes")
    store.cleanup(startup=True)
    assert not orphan.exists() and unrelated.exists()


def test_migration_creates_tables_idempotently(tmp_path):
    from src.common.database.migrations.v45_to_v46 import migrate_v45_to_v46

    engine = create_engine(f"sqlite:///{tmp_path / 'migration.db'}")
    with engine.begin() as connection:
        context = SimpleNamespace(connection=connection)
        migrate_v45_to_v46(context)
        migrate_v45_to_v46(context)
        tables = {row[0] for row in connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"file_assets", "file_bindings", "file_parses"}.issubset(tables)
    engine.dispose()


def test_reader_limit_changes_without_losing_active_readers(store):
    service = FileService(store, enabled_provider=lambda: True)
    store.config = store.config.model_copy(update={"max_concurrent_transfers": 2})
    assert service.acquire_read() and service.acquire_read()
    store.config = store.config.model_copy(update={"max_concurrent_transfers": 1})
    assert not service.acquire_read()
    service.release_read()
    assert not service.acquire_read()
    service.release_read()
    assert service.acquire_read()
    store.config = store.config.model_copy(update={"max_concurrent_transfers": 3})
    assert service.acquire_read() and service.acquire_read()
    assert not service.acquire_read()
    for _ in range(3):
        service.release_read()
