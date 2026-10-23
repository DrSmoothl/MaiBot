"""滚动状态压缩、裁切事务和工具协议边界。"""

from datetime import datetime
from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import AsyncMock, Mock
import json

import pytest

from src.chat.message_receive.message import SessionMessage
from src.common.data_models.llm_service_data_models import LLMResponseResult
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.config.model_configs import ModelInfo
from src.llm_models.model_client.base_client import ResponseRequest
from src.llm_models.payload_content.tool_option import ToolCall, ToolOption
from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.llm_models.payload_content.context_item import (
    ContextItemBuilder,
    ContextItemMeta,
    RoleType,
    ContextToolCall,
    FunctionCallItem,
    ReasoningItem,
    get_item_text,
)
from src.maisaka.chat_loop_service import MaisakaChatLoopService
from src.maisaka.context import compaction
from src.maisaka.context.compaction import ContextSummaryMessage, ConversationState, PlannerRequestSnapshot, build_context_summary
from src.maisaka.context.messages import (
    InternalContextMessage,
    ModelOutputContextMessage,
    ReferenceMessage,
    SessionBackedMessage,
    ToolResultMediaMessage,
    ToolResultMessage,
)
from src.maisaka.context.post_processor import process_chat_history_after_cycle
from src.maisaka.reasoning_engine import MaisakaReasoningEngine

NOW = datetime(2026, 10, 22, 12)


def message(text: str) -> InternalContextMessage:
    return InternalContextMessage(raw_message=MessageSequence([TextComponent(text)]), visible_text=text, timestamp=NOW)


def chat_message(index: int, text: str = "聊天消息") -> SessionBackedMessage:
    source = "user" if index % 2 == 0 else "guided_reply"
    original = SessionMessage(str(index), NOW, "test", received_at=NOW)
    original.session_id = "test-compaction"
    original.message_info = MessageInfo(UserInfo(source, source))
    original.raw_message = MessageSequence([TextComponent(text)])
    return SessionBackedMessage.from_session_message(
        original, raw_message=original.raw_message, visible_text=text, source_kind=source,
    )


def state(**updates: List[str]) -> Dict[str, List[str]]:
    return {**{key: [] for key in compaction.STATE_LABELS}, "topics": ["配置机器人"], **updates}


def tool_turn(turn_id: str = "turn", call_id: str = "call"):
    call = ModelOutputContextMessage(
        output_item=FunctionCallItem(
            meta=ContextItemMeta.create(logical_turn_id=turn_id),
            tool_call=ContextToolCall.create(call_id=call_id, func_name="configure", args={"port": 3000}),
        )
    )
    output = ToolResultMessage(
        content="连接失败：端口不一致",
        timestamp=NOW,
        tool_call_id=call_id,
        logical_turn_id=turn_id,
        tool_name="configure",
        success=False,
    )
    return call, output


def snapshot(history=()):
    return PlannerRequestSnapshot(
        ResponseRequest(
            model_info=ModelInfo(model_type="llm", name="planner-real", model_identifier="remote-model", api_provider="test",
                                 extra_params={"reasoning_effort": "high"}),
            context_items=[ContextItemBuilder().set_role(RoleType.System).add_text_content("原始 Planner 前缀").build()],
            tool_options=[ToolOption(name="configure", description="配置", parameters_schema_override={"type": "object"})],
            temperature=0.8, max_tokens=8192, extra_params={"reasoning_effort": "high"},
        ), tuple(history),
    )


def runtime(history):
    return SimpleNamespace(
        _chat_history=history,
        _chat_loop_service=SimpleNamespace(planner_request_snapshot=snapshot(history)),
        _max_context_size=2,
        session_id="test-compaction",
        log_prefix="测试群",
        _get_pending_wait_tool_call_ids=lambda: set(),
        _monitor_run_id="test",
        _log_history_trimmed=Mock(),
        _trigger_trimmed_history_learning=AsyncMock(),
    )


def test_summary_is_always_sent_without_consuming_recent_window():
    summary = ContextSummaryMessage(content="仍在等端口配置答案", timestamp=NOW, remaining_uses_value=None)
    history = [summary, *[chat_message(index) for index in range(6)]]
    selected, _ = MaisakaChatLoopService.select_llm_context_messages(history, max_context_size=1)
    assert selected == [summary, *history[-5:]]
    assert all(summary.consume_once() for _ in range(4))
    assert summary not in MaisakaChatLoopService._filter_history_for_request_kind(history, request_kind="replyer")


def test_long_messages_do_not_trigger_below_message_count_threshold():
    history = [chat_message(index, str(index) * 90000) for index in range(3)]
    result = process_chat_history_after_cycle(history, max_context_size=100)
    assert result.removed_messages == []
    assert result.history == history


@pytest.mark.parametrize("context_size, expected_count", [(40, 8), (60, 12), (2, 5)])
def test_trim_retains_twenty_percent_of_configured_context_with_five_chat_message_minimum(context_size, expected_count):
    history = [chat_message(index) for index in range(max(12, context_size * 2 + 1))]
    result = process_chat_history_after_cycle(history, max_context_size=context_size)
    assert result.history == history[-expected_count:]
    assert result.removed_messages == history[:-expected_count]


@pytest.mark.parametrize("context_size, threshold", [(40, 48), (60, 72), (7, 9)])
def test_trim_triggers_exactly_at_one_hundred_twenty_percent(context_size, threshold):
    history = [chat_message(index) for index in range(threshold)]
    before = process_chat_history_after_cycle(history[:-1], max_context_size=context_size)
    assert before.history == history[:-1]
    assert before.removed_messages == []
    reached = process_chat_history_after_cycle(history, max_context_size=context_size)
    expected_count = max(5, int(context_size * 0.2))
    assert reached.history == history[-expected_count:]
    assert reached.removed_messages == history[:-expected_count]


def test_internal_items_do_not_count_toward_trigger_or_retention_target():
    chats = [chat_message(index) for index in range(48)]
    history = list(chats[:-1])
    for index in range(60):
        call, output = tool_turn(f"turn-{index}", f"call-{index}")
        reasoning = ModelOutputContextMessage(ReasoningItem(
            meta=ContextItemMeta.create(), summary_parts=("内部思考",),
        ))
        media = ToolResultMediaMessage(
            raw_message=MessageSequence([TextComponent("工具图片")]), visible_text="工具图片",
            timestamp=NOW, tool_call_id=f"call-{index}",
        )
        history.extend([reasoning, call, output, media, message("内部资料")])
    history.extend(ModelOutputContextMessage(item) for item in LLMResponseResult.from_portable_output(
        response="尚未实际发送的模型正文",
    ).output_items)
    assert all(not item.count_in_context for item in history[len(chats) - 1:])
    before = process_chat_history_after_cycle(history, max_context_size=40)
    assert before.history == history
    assert before.remaining_context_count == 47
    reached = process_chat_history_after_cycle([*history, chats[-1]], max_context_size=40)
    assert [item for item in reached.history if item.count_in_context] == chats[-8:]
    assert reached.remaining_context_count == 8


def test_context_selection_counts_only_real_messages():
    chats = [chat_message(index) for index in range(7)]
    history = []
    for chat in chats:
        history.extend([chat, message("内部资料")])
    selected, _ = MaisakaChatLoopService.select_llm_context_messages(history, max_context_size=1)
    assert [item for item in selected if item.count_in_context] == chats[-5:]
    assert selected == history[-10:]


@pytest.mark.parametrize("chat_count", [3, 8])
def test_trim_preserves_recent_real_user_and_bot_messages_without_counting_tools(chat_count):
    summary = ContextSummaryMessage(content="旧摘要", timestamp=NOW)
    originals = [chat_message(index, "超长聊天" * 100) for index in range(chat_count)]
    history = [summary, message("旧内部资料")]
    for index, original in enumerate(originals):
        call, output = tool_turn(f"turn-{index}", f"call-{index}")
        history.extend([original, call, output, message("内部补充")])
    result = process_chat_history_after_cycle(history, max_context_size=1)
    retained_chats = [item for item in result.history if isinstance(item, SessionBackedMessage)]
    assert retained_chats == originals[-5:]
    assert {item.source_kind for item in retained_chats} == {"user", "guided_reply"}
    assert result.history[0] is summary
    for original in retained_chats:
        index = int(original.message_id)
        assert any(isinstance(item, ToolResultMessage) and item.tool_call_id == f"call-{index}" for item in result.history)
    repeated = process_chat_history_after_cycle(result.history, max_context_size=1)
    assert repeated.history == result.history
    assert repeated.removed_messages == []


def test_cut_keeps_complete_tool_turn_with_its_user_anchor():
    call, output = tool_turn()
    history = [*[chat_message(index) for index in range(7)], call, output,
               *[chat_message(index) for index in range(7, 12)]]
    result = process_chat_history_after_cycle(history, max_context_size=1)
    assert result.removed_messages == history[:-5]
    assert result.history == history[-5:]


def test_pending_tool_turn_and_latest_message_are_never_cut():
    call, _ = tool_turn()
    anchor = chat_message(6, "等待配置")
    latest = [chat_message(index) for index in range(7, 12)]
    history = [*[chat_message(index) for index in range(6)], anchor, call, *latest]
    result = process_chat_history_after_cycle(history, max_context_size=1, pending_call_ids={"call"})
    assert result.removed_messages == history[:6]
    assert result.history == [anchor, call, *latest]
    assert result.remaining_context_count == 6


def test_tool_media_is_compacted_with_the_call_and_result():
    call, output = tool_turn()
    media = ToolResultMediaMessage(
        raw_message=MessageSequence([TextComponent("工具图片")]),
        visible_text="工具图片", timestamp=NOW, tool_call_id="call",
    )
    history = [*[chat_message(index) for index in range(7)], call, output, media,
               *[chat_message(index) for index in range(7, 12)]]
    result = process_chat_history_after_cycle(history, max_context_size=2)
    assert result.removed_messages == history[:-5]
    assert result.history == history[-5:]


def test_pre_request_compaction_does_not_consume_new_one_shot_references():
    reference = ReferenceMessage(content="新工具提示", timestamp=NOW)
    result = process_chat_history_after_cycle(
        [message("新消息"), reference], max_context_size=2, consume_references=False
    )
    assert reference in result.history
    assert reference.remaining_uses_value == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_choice", [None, "none"])
@pytest.mark.parametrize("source_context", ["", "session_ids=['A', 'B']"], ids=["ordinary", "focus"])
async def test_snapshot_prefix_tools_and_model_preserved_and_only_delta_appended(
    monkeypatch, tool_choice, source_context,
):
    covered = message("快照中已出现")
    call, output = tool_turn()
    old_omitted = message("甲" * 25000 + "最末一句未答")
    recent = message("还没找到入口")
    original = snapshot([covered])
    original.request.tool_choice = tool_choice
    captured = []

    class Client:
        def __init__(self, *, task_name, **kwargs):
            assert task_name == "planner"

        async def generate_response_from_snapshot(self, request):
            captured.append(request)
            return LLMResponseResult.from_portable_output(response=json.dumps(state(pending=["尚未回答"])))

    monkeypatch.setattr(compaction, "LLMServiceClient", Client)
    monkeypatch.setattr(compaction, "save_compaction_prompt_preview", Mock())
    prompt_loader = Mock(wraps=compaction.load_prompt)
    monkeypatch.setattr(compaction, "load_prompt", prompt_loader)
    previous = ContextSummaryMessage(content="旧摘要", timestamp=NOW)
    result = await build_context_summary(
        [old_omitted, covered], planner_snapshot=original, previous_summary=previous,
        recent_messages=[recent, call, output], session_id="test", max_summary_chars=6000,
        source_context=source_context,
    )
    assert len(captured) == 1
    request = captured[0]
    assert request.context_items[:-1] == original.request.context_items
    assert request.tool_options == original.request.tool_options
    assert request.tool_choice == tool_choice
    assert original.request.tool_choice == tool_choice
    assert request.model_info == original.request.model_info
    assert request.extra_params == {"reasoning_effort": "high"}
    assert request.temperature == 0.8
    assert request.max_tokens == 8192
    tail = get_item_text(request.context_items[-1])
    prompt_loader.assert_called_once_with(
        "context_compaction_focus" if source_context else "context_compaction", max_summary_chars=6000,
    )
    assert ("<conversation_sources>" in tail) == bool(source_context)
    assert ("Focus" in tail) == bool(source_context)
    assert "快照中已出现" not in tail
    assert all(text in tail for text in ["旧摘要", "最末一句未答", "还没找到入口", '"success": false', '"port": 3000'])
    assert "尚未回答" in result.content
    assert result.remaining_uses_value is None


@pytest.mark.asyncio
async def test_compaction_rejects_forbidden_tool_calls(monkeypatch):
    result = LLMResponseResult.from_portable_output(response=json.dumps(state()), tool_calls=[ToolCall("x", "configure", {})])
    client = SimpleNamespace(generate_response_from_snapshot=AsyncMock(return_value=result))
    monkeypatch.setattr(compaction, "LLMServiceClient", lambda **kwargs: client)
    monkeypatch.setattr(compaction, "save_compaction_prompt_preview", Mock())
    with pytest.raises(ValueError, match="禁止的工具调用"):
        await build_context_summary([message("问题")], planner_snapshot=snapshot(), previous_summary=None,
                                    recent_messages=[], session_id="test", max_summary_chars=6000)


@pytest.mark.asyncio
async def test_no_snapshot_defers_cut_and_learning(monkeypatch):
    original = [chat_message(index) for index in range(6)]
    active = runtime(original)
    active._chat_loop_service.planner_request_snapshot = None
    builder = AsyncMock()
    monkeypatch.setattr("src.maisaka.reasoning_engine.build_context_summary", builder)
    await MaisakaReasoningEngine(active)._post_process_chat_history_after_cycle(SimpleNamespace(time_records={}, cycle_id=1))
    assert active._chat_history is original
    builder.assert_not_awaited()
    active._trigger_trimmed_history_learning.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    ["{}", "not json", json.dumps(state(background=["超长" * 5000]))],
    ids=["missing-fields", "invalid-json", "oversized"],
)
async def test_invalid_or_oversized_summary_is_rejected(monkeypatch, response):
    client = SimpleNamespace(
        generate_response_from_snapshot=AsyncMock(
            return_value=LLMResponseResult.from_portable_output(response=response),
        )
    )
    monkeypatch.setattr(compaction, "LLMServiceClient", lambda **kwargs: client)
    monkeypatch.setattr(compaction, "save_compaction_prompt_preview", Mock())
    with pytest.raises(ValueError):
        await build_context_summary(
            [message("原始问题")],
            planner_snapshot=snapshot(),
            previous_summary=None,
            recent_messages=[],
            session_id="test",
            max_summary_chars=6000,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [ValueError("摘要无效"), TimeoutError("硬超时"), RuntimeError("服务商重试耗尽")])
@pytest.mark.parametrize("keep_previous", [False, True])
async def test_failed_compaction_discards_batch_and_does_not_retry(monkeypatch, error, keep_previous):
    original = [chat_message(index) for index in range(6)]
    previous = ContextSummaryMessage(content="上一份有效摘要", timestamp=NOW)
    active = runtime([*([previous] if keep_previous else []), *original])
    engine = MaisakaReasoningEngine(active)
    builder = AsyncMock(side_effect=error)
    monkeypatch.setattr("src.maisaka.reasoning_engine.build_context_summary", builder)
    monkeypatch.setattr("src.maisaka.reasoning_engine.emit_flow_step", AsyncMock())
    await engine._post_process_chat_history_after_cycle(SimpleNamespace(time_records={}, cycle_id=1))
    assert active._chat_history == [*([previous] if keep_previous else []), *original[-5:]]
    assert active._chat_loop_service.planner_request_snapshot is None
    assert active._trigger_trimmed_history_learning.await_args.args[0] == original[:-5]
    await engine._post_process_chat_history_after_cycle(SimpleNamespace(time_records={}, cycle_id=2))
    builder.assert_awaited_once()
    active._trigger_trimmed_history_learning.assert_awaited_once()


@pytest.mark.asyncio
async def test_success_replaces_single_summary_and_learns_only_originals(monkeypatch):
    previous = ContextSummaryMessage(content="旧问题", timestamp=NOW, remaining_uses_value=None)
    originals = [chat_message(index) for index in range(6)]
    active = runtime([previous, *originals])
    summary = ContextSummaryMessage(content="仍未解答的旧问题", timestamp=NOW, remaining_uses_value=None)
    builder = AsyncMock(return_value=summary)
    monkeypatch.setattr("src.maisaka.reasoning_engine.build_context_summary", builder)
    monkeypatch.setattr("src.maisaka.reasoning_engine.emit_flow_step", AsyncMock())
    await MaisakaReasoningEngine(active)._post_process_chat_history_after_cycle(
        SimpleNamespace(time_records={}, cycle_id=1)
    )
    assert active._chat_history == [summary, *originals[-5:]]
    assert active._chat_loop_service.planner_request_snapshot is None
    assert builder.await_args.kwargs["previous_summary"] is previous
    assert active._trigger_trimmed_history_learning.await_args.args[0] == originals[:-5]


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_clear_during_compression_does_not_resurrect_old_history(monkeypatch, fail):
    active = runtime([chat_message(index) for index in range(6)])

    async def build(*args, **kwargs):
        active._chat_history.clear()
        if fail:
            raise ValueError("摘要无效")
        return ContextSummaryMessage(content="旧摘要", timestamp=NOW)

    monkeypatch.setattr("src.maisaka.reasoning_engine.build_context_summary", build)
    await MaisakaReasoningEngine(active)._post_process_chat_history_after_cycle(
        SimpleNamespace(time_records={}, cycle_id=1)
    )
    assert active._chat_history == []
    active._trigger_trimmed_history_learning.assert_not_awaited()


def test_state_requires_all_fields_and_rejects_unstructured_filler():
    assert "未完事项" in ConversationState.model_validate(state(pending=["缺少答案"])).render()
    with pytest.raises(ValueError):
        ConversationState.model_validate({"summary": "这次是轻松闲聊"})


def test_cache_diagnostics_survive_prompt_preview_normalization():
    from src.maisaka.display.prompt_cli_renderer import PromptCLIVisualizer
    metadata = {"prompt_cache_hit_tokens": 0, "prompt_cache_miss_tokens": 8561,
                "prompt_cache_reported": True, "tool_choice": "none", "request_source": "planner_snapshot"}
    assert PromptCLIVisualizer._normalize_preview_metadata(metadata) == metadata
