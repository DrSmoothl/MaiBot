from datetime import datetime
from types import SimpleNamespace

import asyncio
import pytest

from src.chat.heart_flow.heartFC_utils import CycleDetail
from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.utils.utils_config import ChatConfigUtils
from src.config.config import global_config
from src.core.tooling import ToolAvailabilityContext, ToolExecutionResult, ToolInvocation
from src.llm_models.payload_content.tool_option import ToolCall
from src.maisaka import reasoning_engine as reasoning_engine_module
from src.maisaka.builtin_tool import get_builtin_tools, get_timing_tools
from src.maisaka.builtin_tool.context import BuiltinToolRuntimeContext
from src.maisaka.builtin_tool.no_action import handle_tool as handle_no_action_tool
from src.maisaka.chat_loop_service import ChatResponse, MaisakaChatLoopService
from src.maisaka.context.messages import (
    AssistantMessage,
    ReferenceMessage,
    ReferenceMessageType,
    SessionBackedMessage,
    TIMING_GATE_INVALID_TOOL_HINT_SOURCE,
    ToolResultMessage,
)
from src.maisaka.context.post_processor import HistoryPostProcessResult
from src.maisaka.reasoning_engine import MaisakaReasoningEngine
from src.maisaka.runtime import MaisakaHeartFlowChatting


def _build_chat_response(tool_calls: list[ToolCall]) -> ChatResponse:
    return ChatResponse(
        content="The model returned an invalid timing tool.",
        tool_calls=tool_calls,
        request_messages=[],
        raw_message=AssistantMessage(
            content="",
            timestamp=datetime.now(),
            source_kind="perception",
        ),
        selected_history_count=1,
        tool_count=len(tool_calls),
        prompt_tokens=10,
        built_message_count=1,
        completion_tokens=3,
        total_tokens=13,
        prompt_section=None,
    )


def _build_runtime_stub(*, is_group_chat: bool) -> SimpleNamespace:
    return SimpleNamespace(
        _force_next_timing_continue=False,
        _chat_history=[],
        session_id="test-session",
        chat_stream=SimpleNamespace(
            session_id="test-session",
            stream_id="test-stream",
            is_group_session=is_group_chat,
            group_id="group-1" if is_group_chat else "",
            user_id="user-1",
            platform="qq",
        ),
        _chat_loop_service=SimpleNamespace(build_prompt_template_context=lambda: {}),
        log_prefix="[test]",
        stopped=False,
    )


def test_timing_gate_tools_expose_wait_in_all_chat_types() -> None:
    private_tool_names = {
        tool_definition["name"]
        for tool_definition in get_timing_tools(ToolAvailabilityContext(is_group_chat=False))
    }
    group_tool_names = {
        tool_definition["name"]
        for tool_definition in get_timing_tools(ToolAvailabilityContext(is_group_chat=True))
    }

    assert private_tool_names == {"continue", "no_action", "wait"}
    assert group_tool_names == {"continue", "no_action", "wait"}


def test_no_action_is_available_to_planner_and_timing_gate() -> None:
    planner_tool_names = {tool_definition["name"] for tool_definition in get_builtin_tools()}
    timing_tool_names = {
        tool_definition["name"]
        for tool_definition in get_timing_tools(ToolAvailabilityContext(is_group_chat=True))
    }

    assert "no_action" in planner_tool_names
    assert "no_action" in timing_tool_names


@pytest.mark.asyncio
async def test_timing_gate_invalid_tool_defaults_to_no_action(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = _build_runtime_stub(is_group_chat=True)

    def _enter_stop_state() -> None:
        runtime.stopped = True

    runtime._enter_stop_state = _enter_stop_state
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    call_count = 0

    async def _fake_timing_gate_sub_agent(**kwargs: object) -> ChatResponse:
        nonlocal call_count
        del kwargs
        call_count += 1
        return _build_chat_response([
            ToolCall(call_id="invalid-timing-tool", func_name="finish", args={}),
        ])

    async def _fail_invoke_tool_call(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("invalid timing tools must not be executed")

    monkeypatch.setattr(engine, "_run_timing_gate_sub_agent", _fake_timing_gate_sub_agent)
    monkeypatch.setattr(engine, "_invoke_tool_call", _fail_invoke_tool_call)

    action, response, tool_results, tool_monitor_results = await engine._run_timing_gate(object())  # type: ignore[arg-type]

    assert action == "no_action"
    assert call_count == 3
    assert response.tool_calls[0].func_name == "finish"
    assert runtime.stopped is True
    assert tool_monitor_results == []
    assert len(runtime._chat_history) == 1
    assert runtime._chat_history[0].source == TIMING_GATE_INVALID_TOOL_HINT_SOURCE
    assert "finish" in runtime._chat_history[0].processed_plain_text
    assert tool_results == [
        "- retry [非法 Timing 工具]: 返回了 finish，将重试 (1/3)",
        "- retry [非法 Timing 工具]: 返回了 finish，将重试 (2/3)",
        "- no_action [非法 Timing 工具]: 返回了 finish，已停止本轮并等待新消息",
    ]


@pytest.mark.asyncio
async def test_timing_gate_invalid_tool_retries_until_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = _build_runtime_stub(is_group_chat=True)

    def _enter_stop_state() -> None:
        runtime.stopped = True

    runtime._enter_stop_state = _enter_stop_state
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]
    responses = [
        _build_chat_response([ToolCall(call_id="invalid-timing-tool", func_name="finish", args={})]),
        _build_chat_response([ToolCall(call_id="valid-timing-tool", func_name="continue", args={})]),
    ]

    async def _fake_timing_gate_sub_agent(**kwargs: object) -> ChatResponse:
        del kwargs
        return responses.pop(0)

    async def _fake_invoke_tool_call(
        tool_call: ToolCall,
        latest_thought: str,
        anchor_message: object,
        *,
        append_history: bool = True,
        store_record: bool = True,
    ) -> tuple[ToolInvocation, ToolExecutionResult, None]:
        del latest_thought, anchor_message, append_history, store_record
        return (
            ToolInvocation(tool_name=tool_call.func_name, call_id=tool_call.call_id),
            ToolExecutionResult(
                tool_name=tool_call.func_name,
                success=True,
                content="继续执行主流程",
                metadata={"timing_action": "continue"},
            ),
            None,
        )

    monkeypatch.setattr(engine, "_run_timing_gate_sub_agent", _fake_timing_gate_sub_agent)
    monkeypatch.setattr(engine, "_invoke_tool_call", _fake_invoke_tool_call)

    action, response, tool_results, tool_monitor_results = await engine._run_timing_gate(object())  # type: ignore[arg-type]

    assert action == "continue"
    assert response.tool_calls[0].func_name == "continue"
    assert runtime.stopped is False
    assert len(runtime._chat_history) == 2
    assert all(message.source != TIMING_GATE_INVALID_TOOL_HINT_SOURCE for message in runtime._chat_history)
    assert tool_results == [
        "- retry [非法 Timing 工具]: 返回了 finish，将重试 (1/3)",
        "- continue [成功]: 继续执行主流程",
    ]
    assert tool_monitor_results[0]["tool_name"] == "continue"


@pytest.mark.asyncio
async def test_timing_gate_group_chat_allows_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = _build_runtime_stub(is_group_chat=True)
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    async def _fake_timing_gate_sub_agent(**kwargs: object) -> ChatResponse:
        tool_definitions = kwargs["tool_definitions"]
        assert {tool_definition["name"] for tool_definition in tool_definitions} == {"continue", "no_action", "wait"}
        return _build_chat_response([
            ToolCall(call_id="group-wait", func_name="wait", args={"seconds": 3}),
        ])

    async def _fake_invoke_tool_call(
        tool_call: ToolCall,
        latest_thought: str,
        anchor_message: object,
        *,
        append_history: bool = True,
        store_record: bool = True,
    ) -> tuple[ToolInvocation, ToolExecutionResult, None]:
        del latest_thought, anchor_message, append_history, store_record
        return (
            ToolInvocation(tool_name=tool_call.func_name, call_id=tool_call.call_id),
            ToolExecutionResult(
                tool_name=tool_call.func_name,
                success=True,
                content="等待 3 秒后重新判断",
                metadata={"timing_action": "wait"},
            ),
            None,
        )

    monkeypatch.setattr(engine, "_run_timing_gate_sub_agent", _fake_timing_gate_sub_agent)
    monkeypatch.setattr(engine, "_invoke_tool_call", _fake_invoke_tool_call)

    action, _, tool_results, _ = await engine._run_timing_gate(object())  # type: ignore[arg-type]

    assert action == "wait"
    assert tool_results == ["- wait [成功]: 等待 3 秒后重新判断"]


def test_timing_gate_invalid_tool_hint_keeps_only_latest() -> None:
    old_hint = SimpleNamespace(source=TIMING_GATE_INVALID_TOOL_HINT_SOURCE)
    runtime = SimpleNamespace(_chat_history=[old_hint])
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    engine._append_timing_gate_invalid_tool_hint("finish")
    engine._append_timing_gate_invalid_tool_hint("reply")

    assert len(runtime._chat_history) == 1
    hint_message = runtime._chat_history[0]
    assert hint_message.source == TIMING_GATE_INVALID_TOOL_HINT_SOURCE
    assert "reply" in hint_message.processed_plain_text
    assert "finish" not in hint_message.processed_plain_text


def test_timing_gate_invalid_tool_hint_only_visible_to_timing_gate() -> None:
    runtime = SimpleNamespace(_chat_history=[])
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]
    engine._append_timing_gate_invalid_tool_hint("finish")
    hint_message = runtime._chat_history[0]

    timing_history = MaisakaChatLoopService._filter_history_for_request_kind(
        [hint_message],
        request_kind="timing_gate",
    )
    planner_history = MaisakaChatLoopService._filter_history_for_request_kind(
        [hint_message],
        request_kind="planner",
    )

    assert timing_history == [hint_message]
    assert planner_history == []


def test_planner_no_tool_hint_inserted_after_latest_user_and_hidden_from_timing_gate() -> None:
    first_user = SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("first")]),
        visible_text="first",
        timestamp=datetime.now(),
        source_kind="user",
    )
    latest_user = SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("latest")]),
        visible_text="latest",
        timestamp=datetime.now(),
        source_kind="user",
    )
    assistant_message = AssistantMessage(content="analysis only", timestamp=datetime.now())
    runtime = SimpleNamespace(_chat_history=[first_user, latest_user, assistant_message])
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    engine._insert_planner_no_tool_hint(1)

    hint_message = runtime._chat_history[2]
    assert isinstance(hint_message, ReferenceMessage)
    assert hint_message.reference_type == ReferenceMessageType.PLANNER_TOOL_HINT
    assert "必须调用至少一个当前可用工具" in hint_message.content

    timing_history = MaisakaChatLoopService._filter_history_for_request_kind(
        runtime._chat_history,
        request_kind="timing_gate",
    )
    planner_history = MaisakaChatLoopService._filter_history_for_request_kind(
        runtime._chat_history,
        request_kind="planner",
    )

    assert hint_message not in timing_history
    assert hint_message in planner_history

    engine._insert_planner_no_tool_hint(2)
    planner_hints = [
        message
        for message in runtime._chat_history
        if isinstance(message, ReferenceMessage)
        and message.reference_type == ReferenceMessageType.PLANNER_TOOL_HINT
    ]
    assert len(planner_hints) == 1
    assert "严厉提醒" in planner_hints[0].content


def test_planner_continuation_skips_timing_until_finish() -> None:
    runtime = SimpleNamespace(
        _planner_continuation_active=False,
        _force_next_timing_continue=True,
        _force_next_timing_message_id="message-1",
        _force_next_timing_reason="测试触发",
        log_prefix="[test]",
    )

    def _is_planner_continuation_active() -> bool:
        return runtime._planner_continuation_active

    def _start_planner_continuation() -> None:
        runtime._planner_continuation_active = True

    def _finish_planner_continuation() -> None:
        runtime._planner_continuation_active = False

    def _consume_force_next_timing_continue_reason() -> str | None:
        if not runtime._force_next_timing_continue:
            return None
        runtime._force_next_timing_continue = False
        runtime._force_next_timing_message_id = ""
        runtime._force_next_timing_reason = ""
        return "forced timing consumed"

    runtime._is_planner_continuation_active = _is_planner_continuation_active
    runtime._start_planner_continuation = _start_planner_continuation
    runtime._finish_planner_continuation = _finish_planner_continuation
    runtime._consume_force_next_timing_continue_reason = _consume_force_next_timing_continue_reason
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    assert engine._should_run_initial_timing_gate() is True

    engine._start_planner_continuation()

    assert engine._should_run_initial_timing_gate() is False
    assert runtime._planner_continuation_active is True
    assert runtime._force_next_timing_continue is False
    assert runtime._force_next_timing_message_id == ""
    assert runtime._force_next_timing_reason == ""

    engine._finish_planner_continuation()

    assert engine._should_run_initial_timing_gate() is True


@pytest.mark.asyncio
async def test_planner_no_action_tool_keeps_planner_continuation() -> None:
    runtime = SimpleNamespace(_planner_continuation_active=True, stopped=False)

    def _enter_stop_state() -> None:
        runtime.stopped = True

    runtime._enter_stop_state = _enter_stop_state
    tool_ctx = BuiltinToolRuntimeContext(engine=SimpleNamespace(), runtime=runtime)  # type: ignore[arg-type]

    result = await handle_no_action_tool(
        tool_ctx,
        ToolInvocation(tool_name="no_action", call_id="no-action-call"),
    )

    assert result.success is True
    assert result.metadata["pause_execution"] is True
    assert runtime.stopped is True
    assert runtime._planner_continuation_active is True


def test_planner_no_tool_retry_escalates_to_finish_after_three_attempts() -> None:
    latest_user = SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("latest")]),
        visible_text="latest",
        timestamp=datetime.now(),
        source_kind="user",
    )
    runtime = SimpleNamespace(
        _chat_history=[latest_user],
        _planner_continuation_active=True,
        log_prefix="[test]",
        stopped=False,
    )

    def _enter_stop_state() -> None:
        runtime.stopped = True

    def _finish_planner_continuation() -> None:
        runtime._planner_continuation_active = False

    runtime._enter_stop_state = _enter_stop_state
    runtime._finish_planner_continuation = _finish_planner_continuation
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]
    planner_extra_lines: list[str] = []

    no_tool_count, reason, detail, should_finish = engine._handle_planner_no_tool_retry(
        0,
        planner_extra_lines,
    )
    planner_hints = [
        message
        for message in runtime._chat_history
        if isinstance(message, ReferenceMessage)
        and message.reference_type == ReferenceMessageType.PLANNER_TOOL_HINT
    ]
    assert no_tool_count == 1
    assert reason == "planner_missing_tool_retry"
    assert "第 1 次没有调用工具" in detail
    assert should_finish is False
    assert runtime.stopped is False
    assert len(planner_hints) == 1
    assert "必须调用至少一个当前可用工具" in planner_hints[0].content

    no_tool_count, reason, detail, should_finish = engine._handle_planner_no_tool_retry(
        no_tool_count,
        planner_extra_lines,
    )
    planner_hints = [
        message
        for message in runtime._chat_history
        if isinstance(message, ReferenceMessage)
        and message.reference_type == ReferenceMessageType.PLANNER_TOOL_HINT
    ]
    assert no_tool_count == 2
    assert reason == "planner_missing_tool_retry"
    assert "第 2 次没有调用工具" in detail
    assert should_finish is False
    assert runtime.stopped is False
    assert len(planner_hints) == 1
    assert "严厉提醒" in planner_hints[0].content

    no_tool_count, reason, detail, should_finish = engine._handle_planner_no_tool_retry(
        no_tool_count,
        planner_extra_lines,
    )
    planner_hints = [
        message
        for message in runtime._chat_history
        if isinstance(message, ReferenceMessage)
        and message.reference_type == ReferenceMessageType.PLANNER_TOOL_HINT
    ]
    assert no_tool_count == 3
    assert reason == "finish"
    assert "连续 3 次没有调用工具" in detail
    assert should_finish is True
    assert runtime.stopped is True
    assert runtime._planner_continuation_active is False
    assert planner_hints == []
    assert planner_extra_lines[-1] == "状态：连续 3 次未调用工具，已视为 finish"


def test_timing_gate_context_filters_non_timing_actions() -> None:
    reply_call = ToolCall(call_id="reply-call", func_name="reply", args={})
    query_call = ToolCall(call_id="query-call", func_name="query_memory", args={})
    continue_call = ToolCall(call_id="continue-call", func_name="continue", args={})
    wait_call = ToolCall(call_id="wait-call", func_name="wait", args={"seconds": 3})

    action_only_message = AssistantMessage(
        content="",
        timestamp=datetime.now(),
        tool_calls=[reply_call],
    )
    mixed_message = AssistantMessage(
        content="mixed message",
        timestamp=datetime.now(),
        tool_calls=[query_call, continue_call, wait_call],
    )
    selected_history = [
        action_only_message,
        ToolResultMessage(
            content="reply result",
            timestamp=datetime.now(),
            tool_call_id="reply-call",
            tool_name="reply",
        ),
        mixed_message,
        ToolResultMessage(
            content="query result",
            timestamp=datetime.now(),
            tool_call_id="query-call",
            tool_name="query_memory",
        ),
        ToolResultMessage(
            content="continue result",
            timestamp=datetime.now(),
            tool_call_id="continue-call",
            tool_name="continue",
        ),
        ToolResultMessage(
            content="wait result",
            timestamp=datetime.now(),
            tool_call_id="wait-call",
            tool_name="wait",
        ),
    ]

    timing_history = MaisakaChatLoopService._filter_history_for_request_kind(
        selected_history,
        request_kind="timing_gate",
    )

    assert action_only_message not in timing_history
    assert all(
        not (isinstance(message, ToolResultMessage) and message.tool_name in {"reply", "query_memory"})
        for message in timing_history
    )
    filtered_mixed_message = next(message for message in timing_history if isinstance(message, AssistantMessage))
    assert filtered_mixed_message.content == "mixed message"
    assert [tool_call.func_name for tool_call in filtered_mixed_message.tool_calls] == ["continue", "wait"]
    assert [
        message.tool_name
        for message in timing_history
        if isinstance(message, ToolResultMessage)
    ] == ["continue", "wait"]


def test_forced_timing_trigger_bypasses_message_frequency_threshold() -> None:
    runtime = SimpleNamespace(
        session_id="test-session",
        chat_stream=SimpleNamespace(is_group_session=True),
        _STATE_WAIT="wait",
        _agent_state="stop",
        _message_turn_scheduled=False,
        _internal_turn_queue=asyncio.Queue(),
        _has_pending_messages=lambda: True,
        _get_pending_message_count=lambda: 1,
        _is_reply_frequency_silent=lambda: False,
        _has_forced_timing_trigger=lambda: True,
        _cancel_deferred_message_turn_task=lambda: None,
    )

    def _fail_get_message_trigger_threshold() -> int:
        raise AssertionError("@/提及必回不应被普通聊天频率阈值拦住")

    runtime._get_message_trigger_threshold = _fail_get_message_trigger_threshold

    MaisakaHeartFlowChatting._schedule_message_turn(runtime)  # type: ignore[arg-type]

    assert runtime._message_turn_scheduled is True
    assert runtime._internal_turn_queue.get_nowait() == "message"


def test_zero_reply_frequency_keeps_effective_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "test-session"
    runtime.chat_stream = SimpleNamespace(is_group_session=True)
    runtime._talk_frequency_adjust = 1.0

    monkeypatch.setattr(global_config.chat, "talk_value", 0.0)
    monkeypatch.setattr(
        ChatConfigUtils,
        "get_talk_value",
        staticmethod(lambda session_id, is_group_chat=None: 1.0),
    )

    assert runtime._get_effective_reply_frequency() == 0.0
    assert runtime._is_reply_frequency_silent() is True


def test_zero_reply_frequency_schedules_silent_turn_before_forced_trigger() -> None:
    runtime = SimpleNamespace(
        session_id="test-session",
        chat_stream=SimpleNamespace(is_group_session=True),
        _STATE_WAIT="wait",
        _agent_state="stop",
        _message_turn_scheduled=False,
        _internal_turn_queue=asyncio.Queue(),
        _has_pending_messages=lambda: True,
        _get_pending_message_count=lambda: 1,
        _is_reply_frequency_silent=lambda: True,
        _cancel_deferred_message_turn_task=lambda: None,
    )

    def _fail_has_forced_timing_trigger() -> bool:
        raise AssertionError("回复频率为 0 时不应进入 @/提及强制触发分支")

    runtime._has_forced_timing_trigger = _fail_has_forced_timing_trigger

    MaisakaHeartFlowChatting._schedule_message_turn(runtime)  # type: ignore[arg-type]

    assert runtime._message_turn_scheduled is True
    assert runtime._internal_turn_queue.get_nowait() == "message"


@pytest.mark.asyncio
async def test_silent_post_process_skips_mid_term_summary_but_keeps_learning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed_messages = [SimpleNamespace(count_in_context=True)]
    final_history = [SimpleNamespace(count_in_context=True)]
    process_result = HistoryPostProcessResult(
        history=final_history,
        removed_messages=removed_messages,
        removed_count=1,
        changed_count=1,
        remaining_context_count=1,
    )
    trim_logs: list[tuple[int, int]] = []
    learning_messages: list[object] = []

    async def _fake_trigger_learning(messages: object) -> None:
        learning_messages.append(messages)

    runtime = SimpleNamespace(
        _chat_history=[SimpleNamespace(count_in_context=True)],
        _max_context_size=1,
        log_prefix="[test]",
        session_id="test-session",
        _log_history_trimmed=lambda removed_count, remaining_count: trim_logs.append(
            (removed_count, remaining_count)
        ),
        _trigger_trimmed_history_learning=_fake_trigger_learning,
    )
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]
    scheduled_coroutines: list[object] = []

    def _fake_create_task(coro: object) -> SimpleNamespace:
        scheduled_coroutines.append(coro)
        return SimpleNamespace()

    async def _fail_build_mid_term_memory_message(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("静默模式裁切历史不应生成中期记忆摘要")

    monkeypatch.setattr(
        reasoning_engine_module,
        "process_chat_history_after_cycle",
        lambda *args, **kwargs: process_result,
    )
    monkeypatch.setattr(
        reasoning_engine_module,
        "build_mid_term_memory_message",
        _fail_build_mid_term_memory_message,
    )
    monkeypatch.setattr(reasoning_engine_module.asyncio, "create_task", _fake_create_task)

    await engine._post_process_chat_history_after_cycle(
        CycleDetail(cycle_id=1),
        enable_mid_term_memory=False,
    )
    for coro in scheduled_coroutines:
        await coro

    assert runtime._chat_history == final_history
    assert trim_logs == [(1, 1)]
    assert learning_messages == [removed_messages]


def test_finish_tool_is_not_written_back_to_history() -> None:
    finish_call = ToolCall(call_id="finish-call", func_name="finish", args={})
    reply_call = ToolCall(call_id="reply-call", func_name="reply", args={})
    assistant_message = AssistantMessage(
        content="当前不需要继续回复。",
        timestamp=datetime.now(),
        tool_calls=[finish_call, reply_call],
    )
    runtime = SimpleNamespace(_chat_history=[assistant_message])
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    engine._append_tool_execution_result(
        finish_call,
        ToolExecutionResult(
            tool_name="finish",
            success=True,
            content="当前对话循环已结束本轮思考，等待新的消息到来。",
        ),
    )

    assert runtime._chat_history == [assistant_message]
    assert [tool_call.func_name for tool_call in assistant_message.tool_calls] == ["reply"]


def test_finish_tool_removes_empty_assistant_history_message() -> None:
    finish_call = ToolCall(call_id="finish-call", func_name="finish", args={})
    assistant_message = AssistantMessage(
        content="",
        timestamp=datetime.now(),
        tool_calls=[finish_call],
    )
    runtime = SimpleNamespace(_chat_history=[assistant_message])
    engine = MaisakaReasoningEngine(runtime)  # type: ignore[arg-type]

    engine._append_tool_execution_result(
        finish_call,
        ToolExecutionResult(tool_name="finish", success=True),
    )

    assert runtime._chat_history == []


def test_timing_gate_head_trim_keeps_short_history() -> None:
    messages = [
        AssistantMessage(content="第一条消息", timestamp=datetime.now()),
        AssistantMessage(content="第二条消息", timestamp=datetime.now()),
    ]

    trimmed_messages = MaisakaHeartFlowChatting._drop_head_context_messages(
        messages,
        drop_context_count=3,
    )

    assert trimmed_messages == messages


def test_timing_gate_head_trim_keeps_history_within_config_limit() -> None:
    messages = [
        AssistantMessage(content=f"消息 {index}", timestamp=datetime.now())
        for index in range(10)
    ]

    trimmed_messages = MaisakaHeartFlowChatting._drop_head_context_messages(
        messages,
        drop_context_count=7,
        trim_threshold_context_count=10,
    )

    assert trimmed_messages == messages


def test_timing_gate_head_trim_applies_after_config_limit_exceeded() -> None:
    messages = [
        AssistantMessage(content=f"消息 {index}", timestamp=datetime.now())
        for index in range(11)
    ]

    trimmed_messages = MaisakaHeartFlowChatting._drop_head_context_messages(
        messages,
        drop_context_count=7,
        trim_threshold_context_count=10,
    )

    assert trimmed_messages == messages[7:]
