"""普通 Planner 正文交接、参数暂存及共用回复服务的回归测试。"""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, Mock

import asyncio
import pytest

from src.chat.message_receive.message import SessionMessage
from src.common.data_models.llm_service_data_models import LLMResponseResult
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.data_models.reply_generation_data_models import LLMCompletionResult, ReplyGenerationResult
from src.common.i18n import use_locale
from src.config.config import global_config
from src.core.tooling import ToolExecutionResult, ToolInvocation, ToolRegistry
from src.llm_models.payload_content.tool_option import ToolCall
from src.maisaka import reasoning_engine as engine_module
from src.maisaka.builtin_tool import reply as reply_module
from src.maisaka.builtin_tool.context import BuiltinToolRuntimeContext, PostProcessedReplyMessage
from src.maisaka.builtin_tool.provider import MaisakaBuiltinToolProvider
from src.maisaka.chat_loop_service import ChatResponse, MaisakaChatLoopService
from src.maisaka.context.messages import InternalContextMessage, SessionBackedMessage, ToolResultMessage
from src.maisaka.mode_policy import is_idle_cycle_reason
from src.maisaka.quote.counter import ReplyQuoteCounter
from src.maisaka.reasoning_engine import CycleRuntimeState, MaisakaReasoningEngine


def _message(message_id: str) -> SessionMessage:
    message = SessionMessage(message_id, datetime.now(), "test", received_at=datetime.now())
    message.session_id = "chat"
    message.message_info = MessageInfo(UserInfo("user", "User"))
    message.raw_message = MessageSequence([TextComponent(message_id)])
    message.processed_plain_text = message_id
    return message


def _history(message: SessionMessage) -> SessionBackedMessage:
    return SessionBackedMessage.from_session_message(
        message, raw_message=message.raw_message, visible_text=message.processed_plain_text
    )


def _response(content: str = "安慰对方，简短回应。", calls: Optional[List[ToolCall]] = None) -> ChatResponse:
    result = LLMResponseResult.from_portable_output(
        response=content, reasoning="不能作为回复指导的原生推理", tool_calls=calls or []
    )
    return ChatResponse(
        output_items=result.output_items,
        request_messages=[],
        selected_history_count=1,
        tool_count=len(calls or []),
        prompt_tokens=0,
        built_message_count=0,
        completion_tokens=0,
        total_tokens=0,
    )


@pytest.fixture
def engine(monkeypatch) -> MaisakaReasoningEngine:
    monkeypatch.setattr(global_config.experimental, "passive_replyer", True)
    runtime = SimpleNamespace(
        session_id="chat",
        log_prefix="[test]",
        _chat_history=[_history(_message("original"))],
        chat_stream=SimpleNamespace(platform="test", is_group_session=True, group_id="group", user_id=""),
        _end_planner_continuation=Mock(),
        _reset_consecutive_wait_count=Mock(),
        _enter_stop_state=Mock(),
        _update_stage_status=Mock(),
        expand_message_id_aliases=lambda args: args,
        is_action_tool_currently_available=lambda name: name != "reply",
        _tool_registry=ToolRegistry(),
        update_deferred_tool_specs=Mock(),
        set_current_action_tool_names=Mock(),
        sync_discovered_deferred_tools_with_context=Mock(),
        get_discovered_deferred_tool_specs=lambda: [],
        build_deferred_tools_reminder=lambda: "",
        _max_context_size=100,
        _cycle_counter=1,
        _chat_loop_service=SimpleNamespace(select_llm_context_messages=lambda *a, **kw: ([], 0)),
    )
    result = MaisakaReasoningEngine(runtime)
    runtime._reasoning_engine = result
    runtime._tool_registry.register_provider(MaisakaBuiltinToolProvider(result.build_builtin_tool_handlers()))
    result._emit_planner_progress = AsyncMock()
    result._record_tool_execution_effects = AsyncMock()
    result._append_tool_execution_result = Mock()
    return result


async def _act(engine: MaisakaReasoningEngine, response: ChatResponse, state: CycleRuntimeState) -> bool:
    return await engine._handle_planner_response_actions(
        response=response,
        cycle_detail=SimpleNamespace(time_records={}),
        state=state,
        planner_extra_lines=state.planner_extra_lines,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("success", [True, False])
async def test_text_executes_once_preserves_guidance_and_ends(engine, monkeypatch, success):
    response = _response()
    engine._last_reasoning_content = response.content
    target = engine._runtime._chat_history[0].original_message
    state = CycleRuntimeState(reply_target_message=target)
    execute = AsyncMock(
        return_value=ToolExecutionResult(
            tool_name="reply",
            success=success,
            content="已发送" if success else "",
            error_message="发送失败" if not success else "",
            metadata={"send_results": [{"success": True}]},
        )
    )
    monkeypatch.setattr(engine_module, "execute_reply", execute)
    engine._reply_options = {"attach_at": ["original"], "plugin_options": {"test.extension": {"enabled": True}}}

    assert await _act(engine, response, state)

    execute.assert_awaited_once()
    arguments = execute.call_args.args[1]
    assert arguments["reply_reference"] == response.content == "安慰对方，简短回应。"
    assert arguments["attach_at"] == ["original"]
    assert arguments["plugin_options"] == {"test.extension": {"enabled": True}}
    assert execute.call_args.kwargs["target_message"] is target
    assert not engine._reply_options
    assert state.cycle_end.reason == ("planner_reply_end" if success else "planner_reply_failed")
    assert not is_idle_cycle_reason(state.cycle_end.reason)
    assert state.tool_monitor_results[0]["tool_call_source"] == "planner_text"
    if not success:
        assert "部分消息已发送" in state.cycle_end.detail
    assert not any(isinstance(message, ToolResultMessage) for message in engine._runtime._chat_history)
    assert not response.tool_calls
    engine._runtime._enter_stop_state.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["query_memory", "stop", "wait"])
async def test_text_with_tools_never_starts_reply(engine, monkeypatch, tool_name):
    execute = AsyncMock()
    monkeypatch.setattr(engine_module, "execute_reply", execute)
    pause = tool_name in {"stop", "wait"}
    engine._handle_tool_calls = AsyncMock(return_value=(pause, tool_name if pause else "", [], []))
    state = CycleRuntimeState()
    assert await _act(engine, _response(calls=[ToolCall("call", tool_name, {})]), state) is pause
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reasoning_only_does_not_reply(engine, monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(engine_module, "execute_reply", execute)
    state = CycleRuntimeState()
    assert await _act(engine, _response(""), state)
    execute.assert_not_awaited()
    assert state.cycle_end.reason == "planner_no_tool_end"


@pytest.mark.asyncio
async def test_missing_real_target_is_an_explicit_failure(engine, monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(engine_module, "execute_reply", execute)
    state = CycleRuntimeState()
    assert await _act(engine, _response(), state)
    execute.assert_not_awaited()
    assert state.cycle_end.reason == "planner_reply_failed"
    assert "没有可回复的真实消息" in state.cycle_end.detail


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_reply_exception_ends_without_retry_but_cancellation_propagates(engine, monkeypatch, cancelled):
    error = asyncio.CancelledError() if cancelled else ValueError("引用配置无效")
    execute = AsyncMock(side_effect=error)
    monkeypatch.setattr(engine_module, "execute_reply", execute)
    state = CycleRuntimeState(reply_target_message=engine._runtime._chat_history[0].original_message)
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await _act(engine, _response(), state)
    else:
        assert await _act(engine, _response(), state)
        assert state.cycle_end.reason == "planner_reply_failed"
        assert "ValueError: 引用配置无效" in state.cycle_end.detail
        engine._runtime._enter_stop_state.assert_called_once()
    execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_valid_target", [True, False])
async def test_reply_target_is_fixed_before_request_and_refreshed_next_round(engine, monkeypatch, has_valid_target):
    engine._build_action_tool_definitions = AsyncMock(return_value=([], ""))
    engine._refresh_jargon_reference_message = Mock(return_value=None)
    engine._build_planner_injected_user_messages = AsyncMock(return_value=[])
    engine._emit_flow_step = AsyncMock()
    monkeypatch.setattr(engine_module, "resolve_enable_visual_planner", lambda: True)
    original = engine._runtime._chat_history[0].original_message
    later = _message("later")
    notification = _message("notification")
    notification.is_notify = True
    foreign_message = _message("foreign")
    foreign_message.session_id = "another-chat"
    notice = InternalContextMessage(
        raw_message=MessageSequence([TextComponent("内部通知")]), visible_text="内部通知", timestamp=datetime.now()
    )
    invalid_targets = [_history(notification), _history(foreign_message), notice]
    if not has_valid_target:
        engine._runtime._chat_history.clear()
    engine._runtime._chat_history.extend(invalid_targets)

    async def request(**kwargs):
        engine._runtime._chat_history.extend([_history(later), *invalid_targets])
        return _response()

    engine._run_interruptible_planner = request
    first, second = CycleRuntimeState(), CycleRuntimeState()
    for state in (first, second):
        await engine._run_planner_request(
            trigger_message=original, source_messages=[original], round_index=0, round_text="test", state=state
        )
    assert first.reply_target_message is (original if has_valid_target else None)
    assert second.reply_target_message is later
    # 使用真实引用快照和引用规则，不能 mock 掉此次报错所在的调用链。
    counter = ReplyQuoteCounter("chat")
    counter.record(original)
    counter.record(later)
    engine._runtime.get_reply_quote_context = counter.snapshot
    engine._runtime.chat_stream.account_id = "bot"
    monkeypatch.setattr(global_config.chat.reply_style, "group_quote_frequency", "occasionally")
    for state in (first, second):
        if state.reply_target_message is not None:
            _, probability = await reply_module._resolve_reply_quote(
                BuiltinToolRuntimeContext(engine, engine._runtime), "回复正文", state.reply_target_message
            )
            assert 0 <= probability <= 1


@pytest.mark.asyncio
async def test_normal_tools_keep_options_but_focus_provider_keeps_reply(engine):
    definitions, _ = await engine._build_action_tool_definitions()
    by_name = {item["name"]: item for item in definitions}
    assert "reply" not in by_name
    assert "set_reply_options" in by_name
    properties = by_name["set_reply_options"]["parameters_schema"]["properties"]
    assert "attach_pic" in properties
    assert "reply_reference" not in properties
    assert "msg_id" not in properties
    assert "与不调用任何工具时一致" not in by_name["stop"]["description"]
    # Focus 直接使用 Provider，不经过普通模式的定义转换。
    focus_spec = await engine._runtime._tool_registry.get_tool_spec("reply")
    assert focus_spec.parameters_schema["required"] == ["msg_id", "reply_reference"]
    assert not await engine._runtime._tool_registry.has_tool("set_reply_options")


@pytest.mark.asyncio
async def test_options_replace_validate_and_clear_without_sending(engine, monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(engine_module, "execute_reply", execute)

    async def configure(arguments: Dict[str, Any]):
        return await engine._handle_tool_calls(
            [ToolCall("options", "set_reply_options", arguments)], "准备附件", SimpleNamespace(), CycleRuntimeState()
        )

    _, _, _, results = await configure({"attach_pic": [{"msg_id": "original", "index": 0}]})
    assert results[0]["success"]
    assert "attach_pic" in engine._reply_options
    _, _, _, results = await configure({"reply_reference": "不得在工具里触发回复"})
    assert not results[0]["success"]
    await configure({"reply_style": "简短表达"})
    assert engine._reply_options == {"reply_style": "简短表达"}
    await configure({})
    assert not engine._reply_options
    await configure({"reply_style": "正常回复"})
    engine._end_planner_no_tool_cycle([], status_line="stop")
    assert not engine._reply_options
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_reply_call_cannot_send_in_passive_mode(engine, monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(reply_module, "execute_reply", execute)
    _, _, _, results = await engine._handle_tool_calls(
        [ToolCall("old", "reply", {"msg_id": "original", "reply_reference": "旧指令"})],
        "",
        SimpleNamespace(),
        CycleRuntimeState(),
    )
    assert not results[0]["success"]
    assert "直接输出" in results[0]["summary"]
    execute.assert_not_awaited()


@pytest.mark.parametrize("locale", ["zh-CN", "en-US", "ja-JP"])
@pytest.mark.parametrize("enabled", [False, True])
def test_prompt_and_tail_reminder_follow_experimental_switch(monkeypatch, locale, enabled):
    monkeypatch.setattr(global_config.experimental, "passive_replyer", enabled)
    monkeypatch.setattr(engine_module.global_config.experimental, "focus_mode", False)
    service = MaisakaChatLoopService(is_group_chat=True)
    with use_locale(locale):
        prompt = service._build_chat_system_prompt()
        reminder = service._build_planner_final_user_reminder()
    assert ("reply_reference" in prompt) is (not enabled)
    assert ("set_reply_options" in prompt) is enabled
    assert "stop()" in prompt and "wait()" in prompt
    if enabled:
        assert "stop" in reminder and "wait" in reminder
    else:
        assert "reply" in reminder


@pytest.mark.asyncio
async def test_disabled_mode_restores_reply_tool_and_does_not_send_text(engine, monkeypatch):
    monkeypatch.setattr(global_config.experimental, "passive_replyer", False)
    definitions, _ = await engine._build_action_tool_definitions()
    names = {item["name"] for item in definitions}
    assert "reply" in names
    assert "set_reply_options" not in names
    execute = AsyncMock(return_value=ToolExecutionResult(tool_name="reply", success=True))
    monkeypatch.setattr(reply_module, "execute_reply", execute)
    passive_execute = AsyncMock()
    monkeypatch.setattr(engine_module, "execute_reply", passive_execute)
    state = CycleRuntimeState()
    assert await _act(engine, _response(), state)
    assert state.cycle_end.reason == "planner_no_tool_end"
    passive_execute.assert_not_awaited()
    _, _, _, results = await engine._handle_tool_calls(
        [ToolCall("normal", "reply", {"msg_id": "original", "reply_reference": "回复指导"})],
        "", SimpleNamespace(), CycleRuntimeState(),
    )
    assert results[0]["success"]
    execute.assert_awaited_once()
    _, _, _, results = await engine._handle_tool_calls(
        [ToolCall("options", "set_reply_options", {})], "", SimpleNamespace(), CycleRuntimeState(),
    )
    assert not results[0]["success"]
    assert not engine._reply_options


@pytest.mark.parametrize("enabled", [False, True])
def test_focus_prompt_ignores_passive_replyer_switch(monkeypatch, enabled):
    from src.maisaka.focus.group_runtime import FocusChatLoopService

    monkeypatch.setattr(global_config.experimental, "passive_replyer", enabled)
    service = FocusChatLoopService(is_group_chat=True)
    service.passive_replyer_override = enabled
    assert service._get_chat_prompt_name() == "maisaka_chat_focus"
    prompt = service._build_chat_system_prompt()
    assert "reply_reference" in prompt
    assert "set_reply_options" not in prompt


@pytest.mark.parametrize("enabled", [False, True])
def test_running_turn_keeps_prompt_and_behavior_when_switch_changes(engine, monkeypatch, enabled):
    monkeypatch.setattr(global_config.experimental, "focus_mode", False)
    engine._turn_passive_replyer = enabled
    service = MaisakaChatLoopService(is_group_chat=True)
    service.passive_replyer_override = enabled
    monkeypatch.setattr(global_config.experimental, "passive_replyer", not enabled)
    assert engine.passive_replyer_enabled is enabled
    assert service._get_chat_prompt_name() == ("maisaka_chat_passive" if enabled else "maisaka_chat")
    engine._turn_passive_replyer = None
    service.passive_replyer_override = None
    assert engine.passive_replyer_enabled is (not enabled)
    assert service._get_chat_prompt_name() == ("maisaka_chat" if enabled else "maisaka_chat_passive")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["", "empty_receipt", "exception"])
async def test_shared_reply_service_preserves_extensions_attachments_and_send_count(engine, monkeypatch, failure):
    monkeypatch.setattr(global_config.chat, "enable_reply_at", True)
    monkeypatch.setattr(global_config.emoji, "use_new_send_logic", True)
    target = engine._runtime._chat_history[0].original_message
    generation = ReplyGenerationResult(success=True, completion=LLMCompletionResult(response_text="实际回复"))
    generator = SimpleNamespace(generate_reply_with_context=AsyncMock(return_value=(True, generation)))
    monkeypatch.setattr(reply_module.replyer_manager, "get_replyer", lambda **kwargs: generator)
    extension = SimpleNamespace(
        context=SimpleNamespace(),
        targets=["test.ext"],
        parameters={"test.ext": {"mode": "image"}},
        reply_id="reply-id",
        extra_prompt="插件提示",
        prepare=AsyncMock(),
        transform=AsyncMock(side_effect=lambda text, items: items),
        ensure_active=Mock(),
    )
    monkeypatch.setattr(reply_module, "ReplyExtensionExecution", lambda **kwargs: extension)
    monkeypatch.setattr(reply_module, "_invoke_before_post_process_hook", AsyncMock(return_value=("实际回复", {})))
    monkeypatch.setattr(reply_module, "_resolve_reply_quote", AsyncMock(return_value=(False, None)))
    monkeypatch.setattr(reply_module, "is_bot_self", lambda *args: False)
    context = BuiltinToolRuntimeContext(engine, engine._runtime)
    process = AsyncMock(
        return_value=[
            PostProcessedReplyMessage(MessageSequence([TextComponent("第一段")])),
            PostProcessedReplyMessage(MessageSequence([TextComponent("第二段")])),
        ]
    )
    monkeypatch.setattr(context, "post_process_reply_message_items_async", process)
    second_result = {"": _message("sent-2"), "empty_receipt": None, "exception": RuntimeError("发送失败")}[failure]
    send = AsyncMock(side_effect=[_message("sent-1"), second_result])
    monkeypatch.setattr(reply_module.send_service, "_send_to_target_with_message", send)
    engine._runtime.record_planner_reply = Mock()
    arguments = {
        "msg_id": target.message_id,
        "reply_reference": "指导正文",
        "attach_at": [target.message_id],
        "attach_pic": [{"msg_id": target.message_id, "index": 0}],
        "attach_emoji": 1,
        "plugin_options": {"test.ext": {"mode": "image"}},
    }

    result = await reply_module.execute_reply(context, arguments, call_id="execution", target_message=target)

    assert result.success is (not failure)
    generator.generate_reply_with_context.assert_awaited_once()
    request = generator.generate_reply_with_context.call_args.kwargs
    assert request["reply_message"] is target
    for key in ("reply_reference", "attach_at", "attach_pic", "attach_emoji", "plugin_options"):
        assert request["reply_tool_args"][key] == arguments[key]
    extension.prepare.assert_awaited_once_with(arguments["plugin_options"])
    extension.transform.assert_awaited_once()
    assert process.call_args.args[1] == arguments
    assert send.await_count == 2
    assert result.metadata["sent_message_ids"] == (["sent-1"] if failure else ["sent-1", "sent-2"])
    assert result.metadata["send_results"][0]["success"]
    if failure:
        engine._runtime.record_planner_reply.assert_not_called()
    else:
        engine._runtime.record_planner_reply.assert_called_once()


@pytest.mark.asyncio
async def test_focus_reply_adapter_preserves_arguments_and_execution_id(engine, monkeypatch):
    execute = AsyncMock(return_value=ToolExecutionResult(tool_name="reply", success=True))
    monkeypatch.setattr(reply_module, "execute_reply", execute)
    arguments = {"msg_id": "original", "reply_reference": "Focus 指导", "attach_pic": []}
    await reply_module.handle_tool(
        BuiltinToolRuntimeContext(engine, engine._runtime),
        ToolInvocation(tool_name="reply", arguments=arguments, call_id="focus-call"),
    )
    assert execute.call_args.args[1] == arguments
    assert execute.call_args.kwargs == {"call_id": "focus-call"}
