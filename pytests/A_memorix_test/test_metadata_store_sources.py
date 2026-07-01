from pathlib import Path

from src.A_memorix.core.storage.metadata_store import MetadataStore


def test_get_all_sources_ignores_soft_deleted_paragraphs(tmp_path: Path) -> None:
    store = MetadataStore(data_dir=tmp_path)
    store.connect()
    try:
        live_hash = store.add_paragraph("Alice 喜欢地图", source="live-source")
        deleted_hash = store.add_paragraph("Bob 喜欢咖啡", source="deleted-source")

        assert live_hash
        store.mark_as_deleted([deleted_hash], "paragraph")

        sources = store.get_all_sources()
    finally:
        store.close()

    assert [item["source"] for item in sources] == ["live-source"]
    assert sources[0]["count"] == 1


def test_add_paragraph_merges_chat_metadata_when_content_exists(tmp_path: Path) -> None:
    store = MetadataStore(data_dir=tmp_path)
    store.connect()
    try:
        original_hash = store.add_paragraph(
            "Alice 持有地图",
            source="web_import:demo.txt",
            metadata={"import_context": {"batch": "plain"}},
        )
        relation_hash = store.add_relation("Alice", "持有", "地图", source_paragraph=original_hash)

        rebound_hash = store.add_paragraph(
            "Alice 持有地图",
            source="web_import:demo.txt",
            metadata={"chat_id": "chat-1", "import_context": {"mode": "bound"}},
        )
        rebound_relation_hash = store.add_relation("Alice", "持有", "地图", source_paragraph=rebound_hash)

        second_rebound_hash = store.add_paragraph(
            "Alice 持有地图",
            source="web_import:demo.txt",
            metadata={"chat_id": "chat-2"},
        )

        paragraph = store.get_paragraph(original_hash)
        relation_paragraphs = store.get_paragraphs_by_relation_hashes([relation_hash])[relation_hash]
    finally:
        store.close()

    assert original_hash == rebound_hash == second_rebound_hash
    assert relation_hash == rebound_relation_hash
    assert paragraph is not None
    assert paragraph["metadata"]["import_context"] == {"batch": "plain", "mode": "bound"}
    assert paragraph["metadata"]["chat_id"] == "chat-2"
    assert paragraph["metadata"]["chat_ids"] == ["chat-1", "chat-2"]
    assert relation_paragraphs[0]["metadata"]["chat_ids"] == ["chat-1", "chat-2"]
