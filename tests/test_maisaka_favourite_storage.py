from pathlib import Path

import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.maisaka import favourite_storage as storage


def test_favourite_storage_saves_text_and_rejects_duplicate_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage, "FAVOURITE_ROOT", tmp_path)
    monkeypatch.setattr(storage, "_get_isolated_by_chat_config", lambda: False)

    save_result = storage.save_text_content(
        chat_id="chat-1",
        filename="note",
        description="一条有用的收藏",
        text="以后可以引用的内容",
    )

    assert save_result.pool.key == "share"
    assert save_result.item["name"] == "note.txt"
    assert (tmp_path / "share" / "note.txt").read_text(encoding="utf-8") == "以后可以引用的内容"

    with pytest.raises(storage.FavouriteStorageError, match="重名"):
        storage.save_text_content(
            chat_id="chat-1",
            filename="note.txt",
            description="重复收藏",
            text="不同内容",
        )


def test_favourite_storage_browse_and_get_by_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage, "FAVOURITE_ROOT", tmp_path)
    monkeypatch.setattr(storage, "_get_isolated_by_chat_config", lambda: False)
    first = storage.save_text_content(
        chat_id="chat-1",
        filename="alpha.txt",
        description="猫猫梗图说明",
        text="alpha",
    )
    storage.save_text_content(
        chat_id="chat-1",
        filename="beta.txt",
        description="普通文本",
        text="beta",
    )

    browse_result = storage.browse_current_pool(chat_id="chat-1", query="猫猫", page=1, page_size=5)
    assert browse_result["total"] == 1
    assert browse_result["items"][0]["id"] == first.item["id"]

    resolved_item = storage.find_content_by_id(first.item["id"], preferred_chat_id="chat-1")
    assert storage.read_text_item(resolved_item) == "alpha"


def test_favourite_storage_uses_isolated_chat_pool(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(storage, "FAVOURITE_ROOT", tmp_path)
    monkeypatch.setattr(storage, "_get_isolated_by_chat_config", lambda: True)

    save_result = storage.save_text_content(
        chat_id="qq/group:123",
        filename="memory.txt",
        description="隔离池测试",
        text="isolated",
    )

    assert save_result.pool.key.startswith("qq_group_123")
    assert save_result.pool.chat_id == "qq/group:123"
    assert (tmp_path / save_result.pool.key / "manifest.json").exists()
