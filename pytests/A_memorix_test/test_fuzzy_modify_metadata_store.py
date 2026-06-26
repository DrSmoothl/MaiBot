from src.A_memorix.core.storage.metadata_store import MetadataStore, SCHEMA_VERSION


def test_fuzzy_modify_plan_and_superseded_metadata(tmp_path):
    store = MetadataStore(data_dir=tmp_path)
    store.connect()
    try:
        assert SCHEMA_VERSION == 14

        paragraph_hash = store.add_paragraph(
            "小明喜欢咖啡",
            source="person_fact:person-1",
            metadata={"person_ids": ["person-1"], "nested": {"old": True}},
        )
        relation_hash = store.add_relation(
            "小明",
            "喜欢",
            "咖啡",
            source_paragraph=paragraph_hash,
            metadata={"source_type": "person_fact"},
        )

        paragraph_meta = store.update_paragraph_metadata(
            paragraph_hash,
            {
                "nested": {"new": True},
                "memory_change": {
                    "change_id": "fuzzy-test",
                    "change_type": "superseded",
                    "valid_to": 1000.0,
                },
            },
        )
        assert paragraph_meta is not None
        assert paragraph_meta["nested"] == {"old": True, "new": True}
        assert paragraph_meta["memory_change"]["change_type"] == "superseded"

        relation_meta = store.update_relation_metadata(
            relation_hash,
            {"memory_change": {"change_id": "fuzzy-test", "valid_to": 1000.0}},
        )
        assert relation_meta is not None
        assert relation_meta["source_type"] == "person_fact"
        assert relation_meta["memory_change"]["change_id"] == "fuzzy-test"
        store.mark_relations_inactive([relation_hash], inactive_since=1000.0)
        assert store.get_relation_status_batch([relation_hash])[relation_hash]["is_inactive"] is True

        plan = store.create_fuzzy_modify_plan(
            request_text="把小明喜欢咖啡改成喜欢茶",
            scope="person_profile",
            target_person_id="person-1",
            plan={"operations": [{"action": "mark_superseded", "hash": paragraph_hash}]},
            preview={"candidates": [{"hash": paragraph_hash}]},
            confidence=0.92,
            requested_by="pytest",
        )
        fetched = store.get_fuzzy_modify_plan(plan["plan_id"])
        assert fetched is not None
        assert fetched["status"] == "awaiting_confirmation"
        assert fetched["preview"]["candidates"][0]["hash"] == paragraph_hash

        updated = store.update_fuzzy_modify_plan(
            plan["plan_id"],
            status="executed",
            execution={"stored_ids": ["new-hash"]},
            executed_at=1001.0,
        )
        assert updated is not None
        assert updated["status"] == "executed"
        assert updated["execution"]["stored_ids"] == ["new-hash"]
        listed = store.list_fuzzy_modify_plans(statuses=["executed"])
        assert [item["plan_id"] for item in listed] == [plan["plan_id"]]
    finally:
        store.close()
