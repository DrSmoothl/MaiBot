from io import BytesIO
from pathlib import Path

import errno
import zipfile

import pytest

from src.webui.routers import data_transfer


def test_export_keeps_business_data_and_excludes_runtime_locks(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    memory_dir = data_dir / "a-memorix"
    config_dir.mkdir()
    memory_dir.mkdir(parents=True)
    (config_dir / "bot_config.toml").write_bytes(b"config")
    (memory_dir / "metadata.db").write_bytes(b"memory")
    for directory in (data_dir, memory_dir):
        (directory / ".a_memorix_runtime_writer.lock").write_bytes(b"runtime lock")
    monkeypatch.setattr(data_transfer, "_EXPORT_DIRS", {"config": config_dir, "data": data_dir})
    monkeypatch.setattr(data_transfer, "_TRANSFER_TEMP_DIR", tmp_path / "export")
    monkeypatch.setattr(data_transfer, "_jobs", {})
    job = data_transfer._new_job("export")
    data_transfer._run_export_job(job.job_id, data_transfer.DataExportRequest())
    assert job.status == "completed"
    with zipfile.ZipFile(job.file_path) as archive:
        assert archive.read("data/a-memorix/metadata.db") == b"memory"
        assert archive.read("config/bot_config.toml") == b"config"
        assert not any(name.endswith(".a_memorix_runtime_writer.lock") for name in archive.namelist())


def test_read_permission_error_identifies_source_file(monkeypatch):
    source_path = Path("data/unreadable.db")

    class LockedSource(BytesIO):
        def read(self, size=-1):
            raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(Path, "open", lambda *_: LockedSource())
    with zipfile.ZipFile(BytesIO(), "w") as archive:
        with pytest.raises(PermissionError) as error:
            data_transfer._write_archive_file(
                archive, source_path, "data/unreadable.db", data_transfer._TransferJob("test", "export"),
            )
    assert error.value.filename == str(source_path)
    assert "data/unreadable.db" in str(error.value)
