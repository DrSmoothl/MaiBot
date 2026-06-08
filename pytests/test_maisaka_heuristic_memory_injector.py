from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from src.chat.message_receive.chat_manager import BotChatSession
from src.chat.message_receive.message import SessionMessage
from src.common.data_models.mai_message_data_model import GroupInfo, MessageInfo, UserInfo
from src.common.data_models.message_component_data_model import AtComponent, MessageSequence, TextComponent
from src.maisaka import heuristic_memory_injector as injector_module
from src.maisaka.heuristic_memory_injector import HeuristicMemoryInjector
from src.services.memory_service import MemoryHit, MemorySearchResult


def _build_config(
    *,
    enabled: bool = True,
    window_size: int = 3,
    limit: int = 2,
    max_chars: int = 900,
    min_interval: int = 180,
    min_new_messages: int = 6,
    cache_ttl: int = 300,
    cross_chat: bool = False,
    group_to_private: bool = False,
    private_to_group: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        a_memorix=SimpleNamespace(
            integration=SimpleNamespace(
                heuristic_memory_recall_enabled=enabled,
                heuristic_memory_recall_window_size=window_size,
                heuristic_memory_recall_limit=limit,
                heuristic_memory_recall_max_chars=max_chars,
                heuristic_memory_recall_min_interval_seconds=min_interval,
                heuristic_memory_recall_min_new_messages=min_new_messages,
                heuristic_memory_recall_cache_ttl_seconds=cache_ttl,
                heuristic_memory_cross_chat_enabled=cross_chat,
                heuristic_memory_group_to_private_enabled=group_to_private,
                heuristic_memory_private_to_group_enabled=private_to_group,
            )
        )
    )


def _build_session(*, session_id: str = "session-1", group_id: str = "group-1") -> BotChatSession:
    return BotChatSession(
        session_id=session_id,
        platform="qq",
        user_id="alice" if not group_id else None,
        user_nickname="Alice" if not group_id else None,
        group_id=group_id or None,
        group_name="测试群" if group_id else None,
    )


def _build_message(
    index: int,
    *,
    session_id: str = "session-1",
    user_id: str = "alice",
    group_id: str = "group-1",
    components: list[Any] | None = None,
) -> SessionMessage:
    message = SessionMessage(
        message_id=f"m{index}",
        timestamp=datetime(2026, 1, 1) + timedelta(minutes=index),
        platform="qq",
    )
    message.message_info = MessageInfo(
        user_info=UserInfo(
            user_id=user_id,
            user_nickname=user_id.title(),
        ),
        group_info=GroupInfo(group_id=group_id, group_name="测试群") if group_id else None,
    )
    message.raw_message = MessageSequence(components or [TextComponent(f"第 {index} 条消息，正在讨论记忆系统")])
    message.processed_plain_text = f"第 {index} 条消息，正在讨论记忆系统"
    message.session_id = session_id
    return message


def _patch_session_and_messages(
    monkeypatch: pytest.MonkeyPatch,
    *,
    session: BotChatSession,
    messages: list[SessionMessage],
    total_count: int | None = None,
) -> None:
    monkeypatch.setattr(
        injector_module.chat_manager,
        "get_existing_session_by_session_id",
        lambda session_id: session if session_id == session.session_id else None,
    )
    monkeypatch.setattr(
        injector_module.chat_manager,
        "get_session_name",
        lambda session_id: "测试群" if session_id == session.session_id else None,
    )
    monkeypatch.setattr(injector_module, "count_messages", lambda **kwargs: total_count or len(messages))
    monkeypatch.setattr(injector_module, "find_messages", lambda **kwargs: list(messages))


@pytest.mark.asyncio
async def test_disabled_does_not_call_llm_or_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(injector_module, "global_config", _build_config(enabled=False))
    session = _build_session()
    messages = [_build_message(index) for index in range(1, 4)]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages)

    async def fail_generate(prompt: str) -> Any:
        _ = prompt
        raise AssertionError("默认关闭时不应调用 LLM")

    async def fail_search(query: str, **kwargs: Any) -> MemorySearchResult:
        _ = query
        _ = kwargs
        raise AssertionError("默认关闭时不应调用记忆检索")

    monkeypatch.setattr(injector._impression_client, "generate_response", fail_generate)
    monkeypatch.setattr(injector_module.memory_service, "search", fail_search)

    result = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert result == ""


@pytest.mark.asyncio
async def test_not_enough_window_messages_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(injector_module, "global_config", _build_config(enabled=True, window_size=5))
    session = _build_session()
    messages = [_build_message(index) for index in range(1, 4)]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages)

    result = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert result == ""


@pytest.mark.asyncio
async def test_min_new_messages_gate_skips_after_first_recall(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(
        injector_module,
        "global_config",
        _build_config(enabled=True, window_size=3, min_interval=0, min_new_messages=6, cache_ttl=0),
    )
    session = _build_session()
    messages = [_build_message(index) for index in range(1, 4)]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages, total_count=10)
    calls = {"llm": 0}

    async def fake_generate(prompt: str) -> Any:
        _ = prompt
        calls["llm"] += 1
        return SimpleNamespace(response="当前群聊在讨论记忆系统的启发式拉起。")

    async def fake_search(query: str, **kwargs: Any) -> MemorySearchResult:
        _ = query
        _ = kwargs
        return MemorySearchResult(
            hits=[
                MemoryHit(
                    content="当前群之前讨论过启发式记忆边界。",
                    hit_type="paragraph",
                    metadata={"source_type": "chat_summary", "chat_id": session.session_id},
                )
            ]
        )

    monkeypatch.setattr(injector._impression_client, "generate_response", fake_generate)
    monkeypatch.setattr(injector_module.memory_service, "search", fake_search)

    first = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])
    monkeypatch.setattr(injector_module, "count_messages", lambda **kwargs: 15)
    second = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert "启发式记忆-内部参考" in first
    assert second == ""
    assert calls["llm"] == 1


@pytest.mark.asyncio
async def test_min_interval_gate_skips_after_first_recall(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(
        injector_module,
        "global_config",
        _build_config(enabled=True, window_size=3, min_interval=180, min_new_messages=1, cache_ttl=0),
    )
    session = _build_session()
    messages = [_build_message(index) for index in range(1, 4)]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages, total_count=10)
    now = {"value": 1000.0}
    calls = {"llm": 0}
    monkeypatch.setattr(injector_module, "time", lambda: now["value"])

    async def fake_generate(prompt: str) -> Any:
        _ = prompt
        calls["llm"] += 1
        return SimpleNamespace(response="当前群聊在继续讨论记忆自然拉起。")

    async def fake_search(query: str, **kwargs: Any) -> MemorySearchResult:
        _ = query
        _ = kwargs
        return MemorySearchResult(
            hits=[
                MemoryHit(
                    content="当前群聊之前讨论过触发频率。",
                    hit_type="paragraph",
                    metadata={"source_type": "chat_summary", "chat_id": session.session_id},
                )
            ]
        )

    monkeypatch.setattr(injector._impression_client, "generate_response", fake_generate)
    monkeypatch.setattr(injector_module.memory_service, "search", fake_search)

    first = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])
    now["value"] = 1100.0
    monkeypatch.setattr(injector_module, "count_messages", lambda **kwargs: 30)
    second = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert "启发式记忆-内部参考" in first
    assert second == ""
    assert calls["llm"] == 1


@pytest.mark.asyncio
async def test_filters_memory_boundaries_and_active_person(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(
        injector_module,
        "global_config",
        _build_config(enabled=True, window_size=3, limit=3, min_interval=0, min_new_messages=1, cache_ttl=0),
    )
    session = _build_session()
    bob_person_id = injector_module.get_person_id("qq", "bob")
    messages = [
        _build_message(1, user_id="alice"),
        _build_message(2, user_id="bob"),
        _build_message(3, user_id="alice", components=[AtComponent("bob", target_user_nickname="Bob")]),
    ]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages)

    async def fake_generate(prompt: str) -> Any:
        assert "最近消息" in prompt
        return SimpleNamespace(response="当前群聊在讨论记忆边界，也提到了 Bob。")

    async def fake_search(query: str, **kwargs: Any) -> MemorySearchResult:
        _ = query
        _ = kwargs
        return MemorySearchResult(
            hits=[
                MemoryHit(
                    content="当前群聊以前讨论过自然拉起。",
                    hit_type="paragraph",
                    metadata={"source_type": "chat_summary", "chat_id": session.session_id},
                ),
                MemoryHit(
                    content="其他群聊的记忆不应默认出现。",
                    hit_type="paragraph",
                    metadata={"source_type": "chat_summary", "chat_id": "other-session"},
                ),
                MemoryHit(
                    content="Bob 之前说过他关心记忆边界。",
                    hit_type="paragraph",
                    metadata={"source_type": "person_fact", "person_id": bob_person_id},
                ),
                MemoryHit(
                    content="未知来源的内容不应注入。",
                    hit_type="paragraph",
                    metadata={},
                ),
            ]
        )

    monkeypatch.setattr(injector._impression_client, "generate_response", fake_generate)
    monkeypatch.setattr(injector_module.memory_service, "search", fake_search)

    result = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert "当前群聊以前讨论过自然拉起" in result
    assert "Bob 之前说过" in result
    assert "其他群聊" not in result
    assert "未知来源" not in result


@pytest.mark.asyncio
async def test_resolves_hash_only_scope_before_filtering(monkeypatch: pytest.MonkeyPatch) -> None:
    injector = HeuristicMemoryInjector()
    monkeypatch.setattr(
        injector_module,
        "global_config",
        _build_config(enabled=True, window_size=3, limit=3, min_interval=0, min_new_messages=1, cache_ttl=0),
    )
    session = _build_session()
    bob_person_id = injector_module.get_person_id("qq", "bob")
    messages = [
        _build_message(1, user_id="alice"),
        _build_message(2, user_id="bob"),
        _build_message(3, user_id="alice", components=[AtComponent("bob", target_user_nickname="Bob")]),
    ]
    _patch_session_and_messages(monkeypatch, session=session, messages=messages)

    async def fake_generate(prompt: str) -> Any:
        _ = prompt
        return SimpleNamespace(response="当前群聊在讨论记忆边界，也提到了 Bob。")

    async def fake_search(query: str, **kwargs: Any) -> MemorySearchResult:
        _ = query
        _ = kwargs
        return MemorySearchResult(
            hits=[
                MemoryHit(content="hash-only 当前群聊摘要", hit_type="paragraph", source="paragraph_search", hash_value="h1"),
                MemoryHit(content="hash-only 其他群聊摘要", hit_type="paragraph", source="sparse_bm25", hash_value="h2"),
                MemoryHit(content="hash-only Bob 人物事实", hit_type="paragraph", source="paragraph_search", hash_value="h3"),
            ]
        )

    async def fake_delete_admin(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["action"] == "preview"
        assert kwargs["mode"] == "paragraph"
        return {
            "success": True,
            "items": [
                {"item_type": "paragraph", "item_hash": "h1", "source": f"chat_summary:{session.session_id}"},
                {"item_type": "paragraph", "item_hash": "h2", "source": "chat_summary:other-session"},
                {"item_type": "paragraph", "item_hash": "h3", "source": f"person_fact:{bob_person_id}"},
            ],
        }

    monkeypatch.setattr(injector._impression_client, "generate_response", fake_generate)
    monkeypatch.setattr(injector_module.memory_service, "search", fake_search)
    monkeypatch.setattr(injector_module.memory_service, "delete_admin", fake_delete_admin)

    result = await injector.build_injection_message(session_id=session.session_id, anchor_message=messages[-1])

    assert "hash-only 当前群聊摘要" in result
    assert "hash-only Bob 人物事实" in result
    assert "hash-only 其他群聊摘要" not in result


