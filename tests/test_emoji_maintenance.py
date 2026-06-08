from pathlib import Path
from types import SimpleNamespace
from typing import Any

import asyncio
import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.emoji_system import emoji_manager as emoji_module  # noqa: E402


class _Result:
    def __init__(self, records: list[Any]) -> None:
        self._records = records

    def all(self) -> list[Any]:
        return self._records


class _Session:
    def __init__(self, records: list[Any]) -> None:
        self.records = records
        self.deleted: list[Any] = []

    def __enter__(self) -> "_Session":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def exec(self, _statement: Any) -> _Result:
        return _Result(self.records)

    def delete(self, record: Any) -> None:
        self.deleted.append(record)


def _build_emoji_record(
    record_id: int,
    full_path: Path | str,
    *,
    image_hash: str = "emoji_hash",
    is_registered: bool = True,
    is_banned: bool = False,
    no_file_flag: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=record_id,
        image_type=emoji_module.ImageType.EMOJI,
        image_hash=image_hash,
        description="开心",
        full_path=str(full_path),
        query_count=0,
        is_registered=is_registered,
        is_banned=is_banned,
        no_file_flag=no_file_flag,
        register_time=None,
        last_used_time=None,
    )


def test_load_removes_registered_records_when_file_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    available_file = tmp_path / "available.png"
    missing_file = tmp_path / "missing.png"
    available_file.write_bytes(b"available")

    available_record = _build_emoji_record(1, available_file, image_hash="available_hash")
    missing_record = _build_emoji_record(2, missing_file, image_hash="missing_hash")
    session = _Session(records=[available_record, missing_record])
    monkeypatch.setattr(emoji_module, "get_db_session", lambda: session)

    manager = emoji_module.EmojiManager()
    try:
        manager.load_emojis_from_db()

        assert [emoji.file_hash for emoji in manager.emojis] == ["available_hash"]
        assert manager._emoji_num == 1
        assert session.deleted == [missing_record]
    finally:
        manager.shutdown()


def test_integrity_preserves_unknown_files_and_removes_missing_registered_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(emoji_module, "EMOJI_DIR", tmp_path)
    new_file = tmp_path / "new.png"
    missing_file = tmp_path / "missing.png"
    new_file.write_bytes(b"new")

    missing_record = _build_emoji_record(1, missing_file, image_hash="missing_hash")
    session = _Session(records=[missing_record])
    monkeypatch.setattr(emoji_module, "get_db_session", lambda: session)

    manager = emoji_module.EmojiManager()
    try:
        manager.check_emoji_file_integrity()

        assert new_file.exists()
        assert session.deleted == [missing_record]
        assert manager.emojis == []
    finally:
        manager.shutdown()


@pytest.mark.asyncio
async def test_periodic_maintenance_scans_unregistered_records_before_integrity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(emoji_module, "EMOJI_DIR", tmp_path)
    registered_file = tmp_path / "registered.png"
    unregistered_file = tmp_path / "unregistered.png"
    registered_file.write_bytes(b"registered")
    unregistered_file.write_bytes(b"unregistered")
    session = _Session(
        records=[
            _build_emoji_record(1, registered_file, image_hash="registered_hash"),
            _build_emoji_record(2, unregistered_file, image_hash="unregistered_hash", is_registered=False),
        ]
    )
    monkeypatch.setattr(emoji_module, "get_db_session", lambda: session)

    monkeypatch.setattr(emoji_module.global_config.emoji, "steal_emoji", True)
    monkeypatch.setattr(emoji_module.global_config.emoji, "check_interval", 0)
    monkeypatch.setattr(emoji_module.global_config.emoji, "max_reg_num", 10)
    monkeypatch.setattr(emoji_module.global_config.emoji, "do_replace", False)

    events: list[tuple[str, str]] = []
    first_check = asyncio.Event()

    manager = emoji_module.EmojiManager()

    async def _register_emoji_by_filename(path: Path | str) -> emoji_module.EmojiRegisterStatus:
        emoji_path = Path(path)
        events.append(("scan", emoji_path.name))
        registered_emoji = type("_Emoji", (), {"full_path": emoji_path.absolute().resolve()})()
        manager.emojis.append(registered_emoji)
        manager._emoji_num = len(manager.emojis)
        return "registered"

    def _check_emoji_file_integrity() -> None:
        events.append(("check", ""))
        first_check.set()

    monkeypatch.setattr(manager, "register_emoji_by_filename", _register_emoji_by_filename)
    monkeypatch.setattr(manager, "check_emoji_file_integrity", _check_emoji_file_integrity)

    task = asyncio.create_task(manager.periodic_emoji_maintenance())
    try:
        await asyncio.wait_for(first_check.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        manager.shutdown()

    assert events[0] == ("scan", "unregistered.png")
    assert events[1] == ("check", "")
    assert ("scan", "registered.png") not in events


@pytest.mark.asyncio
async def test_ensure_emoji_saved_rejects_oversized_collected_emoji(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(emoji_module.global_config.emoji, "max_emoji_size_mb", 0.000001)

    manager = emoji_module.EmojiManager()
    try:
        with pytest.raises(ValueError, match="表情包大小超过收集上限"):
            await manager.ensure_emoji_saved(b"oversized")
    finally:
        manager.shutdown()


@pytest.mark.asyncio
async def test_periodic_maintenance_skips_oversized_collected_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(emoji_module, "EMOJI_DIR", tmp_path)
    monkeypatch.setattr(emoji_module.global_config.emoji, "max_emoji_size_mb", 0.000001)
    monkeypatch.setattr(emoji_module.global_config.emoji, "steal_emoji", True)
    monkeypatch.setattr(emoji_module.global_config.emoji, "check_interval", 0)
    monkeypatch.setattr(emoji_module.global_config.emoji, "max_reg_num", 10)
    monkeypatch.setattr(emoji_module.global_config.emoji, "do_replace", False)
    monkeypatch.setattr(emoji_module, "get_db_session", lambda: _Session(records=[]))

    oversized_file = tmp_path / "oversized.png"
    oversized_file.write_bytes(b"oversized")

    events: list[str] = []
    first_check = asyncio.Event()
    manager = emoji_module.EmojiManager()

    async def _register_emoji_by_filename(path: Path | str) -> emoji_module.EmojiRegisterStatus:
        events.append(Path(path).name)
        return "registered"

    def _check_emoji_file_integrity() -> None:
        first_check.set()

    monkeypatch.setattr(manager, "register_emoji_by_filename", _register_emoji_by_filename)
    monkeypatch.setattr(manager, "check_emoji_file_integrity", _check_emoji_file_integrity)

    task = asyncio.create_task(manager.periodic_emoji_maintenance())
    try:
        await asyncio.wait_for(first_check.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        manager.shutdown()

    assert events == []
