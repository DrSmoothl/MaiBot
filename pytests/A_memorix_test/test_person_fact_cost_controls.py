from typing import Any, Dict, List, Optional

import pytest

from src.A_memorix.core.runtime.sdk_memory_kernel import SDKMemoryKernel
from src.A_memorix.core.storage.metadata_store import MetadataStore


class _FakeMetadataStore:
    def __init__(self) -> None:
        self.pending: List[Dict[str, Any]] = []
        self.profile_refreshes: List[Dict[str, str]] = []
        self.external_refs: List[Dict[str, Any]] = []
        self.paragraphs: List[Dict[str, Any]] = []

    def get_external_memory_ref(self, external_id: str) -> Optional[Dict[str, Any]]:
        del external_id
        return None

    def add_paragraph(self, **kwargs: Any) -> str:
        self.paragraphs.append(dict(kwargs))
        return f"paragraph-{len(self.paragraphs)}"

    def add_entity(self, *, name: str, source_paragraph: str) -> str:
        return f"entity:{name}:{source_paragraph}"

    def upsert_external_memory_ref(self, **kwargs: Any) -> None:
        self.external_refs.append(dict(kwargs))

    def enqueue_episode_pending(self, paragraph_hash: str, source: str | None = None) -> None:
        self.pending.append({"paragraph_hash": paragraph_hash, "source": source})

    def enqueue_person_profile_refresh(
        self,
        *,
        person_id: str,
        reason: str = "",
        source_query_tool_id: str = "",
    ) -> Dict[str, Any]:
        payload = {
            "person_id": person_id,
            "reason": reason,
            "source_query_tool_id": source_query_tool_id,
        }
        self.profile_refreshes.append(payload)
        return payload


class _FakeVectorResult:
    @staticmethod
    async def upsert_relation_with_vector(**kwargs: Any) -> Dict[str, Any]:
        del kwargs
        raise AssertionError("relations are not expected in this test")


async def _fake_initialize() -> None:
    return None


def _build_kernel(tmp_path, config: Dict[str, Any]) -> tuple[SDKMemoryKernel, _FakeMetadataStore]:
    kernel = SDKMemoryKernel(plugin_root=tmp_path, config=config)
    metadata_store = _FakeMetadataStore()
    kernel.metadata_store = metadata_store  # type: ignore[assignment]
    kernel.vector_store = object()  # type: ignore[assignment]
    kernel.graph_store = object()  # type: ignore[assignment]
    kernel.embedding_manager = object()
    kernel.relation_write_service = _FakeVectorResult()  # type: ignore[assignment]
    kernel.initialize = _fake_initialize  # type: ignore[method-assign]
    kernel._persist = lambda *args, **kwargs: None  # type: ignore[method-assign]

    async def fake_vector_write(**kwargs: Any) -> Dict[str, Any]:
        del kwargs
        return {}

    async def fake_entity_vector(*args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        return True

    async def fail_episode_processing(**kwargs: Any) -> Dict[str, Any]:
        del kwargs
        raise AssertionError("ingest_text must not process episode pending synchronously")

    async def fail_profile_refresh(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        del args, kwargs
        raise AssertionError("ingest_text must enqueue profile refresh instead of refreshing immediately")

    kernel._write_paragraph_vector_or_enqueue = fake_vector_write  # type: ignore[method-assign]
    kernel._ensure_entity_vector = fake_entity_vector  # type: ignore[method-assign]
    kernel.process_episode_pending_batch = fail_episode_processing  # type: ignore[method-assign]
    kernel.refresh_person_profile = fail_profile_refresh  # type: ignore[method-assign]
    return kernel, metadata_store


@pytest.mark.asyncio
async def test_person_fact_ingest_skips_episode_and_debounces_profile_refresh(tmp_path) -> None:
    kernel, metadata_store = _build_kernel(tmp_path, config={})

    result = await kernel.ingest_text(
        external_id="fact-1",
        source_type="person_fact",
        text="测试用户喜欢猫。",
        person_ids=["person-1"],
        participants=["测试用户"],
    )

    assert result["stored_ids"] == ["paragraph-1"]
    assert metadata_store.pending == []
    assert metadata_store.profile_refreshes == [
        {
            "person_id": "person-1",
            "reason": "person_fact",
            "source_query_tool_id": "",
        }
    ]


@pytest.mark.asyncio
async def test_memory_ingest_enqueues_episode_without_synchronous_processing(tmp_path) -> None:
    kernel, metadata_store = _build_kernel(tmp_path, config={})

    result = await kernel.ingest_text(
        external_id="memory-1",
        source_type="memory",
        text="用户今天讨论了绿色围巾。",
        chat_id="session-1",
    )

    assert result["stored_ids"] == ["paragraph-1"]
    assert metadata_store.pending == [
        {
            "paragraph_hash": "paragraph-1",
            "source": "memory:session-1",
        }
    ]
    assert metadata_store.profile_refreshes == []


@pytest.mark.asyncio
async def test_episode_generation_disabled_skips_all_auto_episode_enqueue(tmp_path) -> None:
    kernel, metadata_store = _build_kernel(
        tmp_path,
        config={"episode": {"generation_enabled": False}},
    )

    await kernel.ingest_text(
        external_id="memory-1",
        source_type="memory",
        text="用户今天讨论了绿色围巾。",
        chat_id="session-1",
    )

    assert metadata_store.pending == []


def test_person_profile_refresh_queue_debounce_and_retry_backoff(tmp_path) -> None:
    store = MetadataStore(data_dir=tmp_path)
    store.connect()
    try:
        first = store.enqueue_person_profile_refresh(person_id="person-1", reason="first")
        second = store.enqueue_person_profile_refresh(person_id="person-1", reason="second")

        assert first is not None
        assert second is not None
        assert second["reason"] == "second"
        assert store.fetch_person_profile_refresh_batch(limit=10, debounce_seconds=3600) == []

        ready = store.fetch_person_profile_refresh_batch(limit=10, debounce_seconds=0)
        assert [row["person_id"] for row in ready] == ["person-1"]

        assert store.mark_person_profile_refresh_running("person-1", requested_at=ready[0]["requested_at"])
        assert store.mark_person_profile_refresh_failed(
            "person-1",
            "boom",
            requested_at=ready[0]["requested_at"],
        )
        assert (
            store.fetch_person_profile_refresh_batch(
                limit=10,
                max_retry=3,
                retry_backoff_seconds=3600,
            )
            == []
        )
        retry_ready = store.fetch_person_profile_refresh_batch(
            limit=10,
            max_retry=3,
            retry_backoff_seconds=0,
        )
        assert [row["person_id"] for row in retry_ready] == ["person-1"]
        assert retry_ready[0]["retry_count"] == 1
    finally:
        store.close()
