from typing import Any, Dict, List

import pytest

from src.A_memorix.core.runtime.sdk_memory_kernel import SDKMemoryKernel
from src.A_memorix.core.utils.episode_service import EpisodeService
from src.A_memorix.core.utils.summary_importer import SummaryImportResult


class _FakeSummaryImporter:
    async def import_from_stream(self, **kwargs: Any) -> SummaryImportResult:
        del kwargs
        return SummaryImportResult(
            success=True,
            detail="ok",
            paragraph_hash="new-summary-hash",
            source="chat_summary:session-1",
        )


class _MissingHashSummaryImporter:
    async def import_from_stream(self, **kwargs: Any) -> SummaryImportResult:
        del kwargs
        return SummaryImportResult(success=True, detail="ok")


class _PendingMetadataStore:
    def __init__(self) -> None:
        self.pending: List[Dict[str, Any]] = []

    def enqueue_episode_pending(
        self,
        paragraph_hash: str,
        source: str | None = None,
        created_at: float | None = None,
    ) -> None:
        self.pending.append(
            {
                "paragraph_hash": paragraph_hash,
                "source": source,
                "created_at": created_at,
            }
        )


class _EpisodeMetadataStore:
    def __init__(self) -> None:
        self.paragraphs: Dict[str, Dict[str, Any]] = {
            "new-summary-hash": {
                "content": "用户提到自己买了绿色围巾。",
                "source": "chat_summary:session-1",
                "created_at": 100.0,
                "event_time": 100.0,
            }
        }
        for index in range(45):
            self.paragraphs[f"old-summary-{index}"] = {
                "content": f"历史摘要 {index}",
                "source": "chat_summary:session-1",
                "created_at": float(index),
                "event_time": float(index),
            }
        self.stored_payloads: List[Dict[str, Any]] = []
        self.bound_paragraphs: List[tuple[str, List[str]]] = []

    def get_paragraph(self, paragraph_hash: str) -> Dict[str, Any] | None:
        return self.paragraphs.get(paragraph_hash)

    def get_paragraph_entities(self, paragraph_hash: str) -> List[Dict[str, Any]]:
        del paragraph_hash
        return []

    def upsert_episode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.stored_payloads.append(dict(payload))
        return dict(payload)

    def bind_episode_paragraphs(self, episode_id: str, paragraph_hashes: List[str]) -> None:
        self.bound_paragraphs.append((episode_id, list(paragraph_hashes)))


class _FakeSegmentationService:
    def __init__(self) -> None:
        self.calls: List[List[Dict[str, Any]]] = []

    async def segment(self, **kwargs: Any) -> Dict[str, Any]:
        paragraphs = list(kwargs.get("paragraphs") or [])
        self.calls.append(paragraphs)
        hashes = [str(item.get("hash", "") or "") for item in paragraphs]
        return {
            "episodes": [
                {
                    "title": "绿色围巾",
                    "summary": "用户提到自己买了绿色围巾。",
                    "paragraph_hashes": hashes,
                    "participants": [],
                    "keywords": ["绿色围巾"],
                    "time_confidence": 1.0,
                    "llm_confidence": 0.9,
                }
            ],
            "segmentation_model": "fake",
            "segmentation_version": "test",
        }


@pytest.mark.asyncio
async def test_auto_chat_summary_enqueues_new_paragraph_without_source_rebuild(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    kernel = SDKMemoryKernel(plugin_root=tmp_path, config={})
    metadata_store = _PendingMetadataStore()
    rebuild_calls: List[List[str]] = []
    persist_calls = 0

    async def fake_initialize() -> None:
        return None

    async def fake_rebuild_episodes_for_sources(sources: List[str]) -> Dict[str, Any]:
        rebuild_calls.append(list(sources))
        return {"rebuilt": 0, "items": [], "failures": [], "sources": list(sources)}

    def fake_persist() -> None:
        nonlocal persist_calls
        persist_calls += 1

    monkeypatch.setattr(kernel, "initialize", fake_initialize)
    monkeypatch.setattr(kernel, "rebuild_episodes_for_sources", fake_rebuild_episodes_for_sources)
    monkeypatch.setattr(kernel, "_persist", fake_persist)
    kernel.summary_importer = _FakeSummaryImporter()
    kernel.metadata_store = metadata_store

    result = await kernel.summarize_chat_stream(chat_id="session-1")

    assert result["success"] is True
    assert result["stored_ids"] == ["new-summary-hash"]
    assert result["episode_pending_ids"] == ["new-summary-hash"]
    assert metadata_store.pending == [
        {
            "paragraph_hash": "new-summary-hash",
            "source": "chat_summary:session-1",
            "created_at": None,
        }
    ]
    assert rebuild_calls == []
    assert persist_calls == 1


@pytest.mark.asyncio
async def test_auto_chat_summary_requires_paragraph_hash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    kernel = SDKMemoryKernel(plugin_root=tmp_path, config={})

    async def fake_initialize() -> None:
        return None

    monkeypatch.setattr(kernel, "initialize", fake_initialize)
    kernel.summary_importer = _MissingHashSummaryImporter()
    kernel.metadata_store = _PendingMetadataStore()

    with pytest.raises(RuntimeError, match="paragraph_hash"):
        await kernel.summarize_chat_stream(chat_id="session-1")


@pytest.mark.asyncio
async def test_incremental_pending_keeps_new_summary_episode_quality() -> None:
    metadata_store = _EpisodeMetadataStore()
    segmentation_service = _FakeSegmentationService()
    episode_service = EpisodeService(
        metadata_store=metadata_store,
        plugin_config={
            "episode": {
                "max_paragraphs_per_call": 20,
                "max_chars_per_call": 6000,
                "source_time_window_hours": 24,
            }
        },
        segmentation_service=segmentation_service,
    )

    result = await episode_service.process_pending_rows(
        [
            {
                "paragraph_hash": "new-summary-hash",
                "source": "chat_summary:session-1",
                "created_at": 100.0,
            }
        ]
    )

    # 旧整源重建在 45 条历史摘要 + 1 条新增摘要时至少会按 20 条一组调用 3 次；
    # 增量 pending 只处理新增摘要，调用量降为 1 次。
    assert len(segmentation_service.calls) == 1
    assert len(segmentation_service.calls[0]) == 1
    assert segmentation_service.calls[0][0]["hash"] == "new-summary-hash"
    assert result["episode_count"] == 1
    assert result["done_hashes"] == ["new-summary-hash"]
    assert metadata_store.stored_payloads
    assert metadata_store.stored_payloads[0]["summary"] == "用户提到自己买了绿色围巾。"
    assert metadata_store.stored_payloads[0]["evidence_ids"] == ["new-summary-hash"]
