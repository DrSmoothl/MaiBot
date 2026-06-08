from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable

import inspect

from rich.panel import Panel
from rich.text import Text

import pytest

from src.chat.replyer import maisaka_generator as replyer_module
from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.data_models.reply_generation_data_models import (
    GenerationMetrics,
    LLMCompletionResult,
    ReplyGenerationResult,
    build_reply_monitor_detail,
)
from src.core.tooling import ToolExecutionResult, ToolInvocation
from src.maisaka.builtin_tool.context import BuiltinToolRuntimeContext
from src.maisaka.builtin_tool import reply as reply_tool_module
from src.maisaka.builtin_tool import send_emoji as send_emoji_tool_module
from src.maisaka.context.messages import SessionBackedMessage
from src.maisaka.monitor.events import emit_planner_finalized
from src.maisaka.reasoning_engine import MaisakaReasoningEngine
from src.maisaka import runtime as runtime_module
from src.maisaka.runtime import MaisakaHeartFlowChatting


def _build_reply_tool_ctx(chat_history: list[Any]) -> BuiltinToolRuntimeContext:
    target_message = SimpleNamespace(
        message_id="msg-1",
        message_info=SimpleNamespace(
            user_info=SimpleNamespace(
                user_cardname="测试用户",
                user_nickname="测试用户",
                user_id="user-1",
            )
        ),
    )
    runtime = SimpleNamespace(
        find_source_message_by_id=lambda message_id: target_message if message_id == "msg-1" else None,
        log_prefix="[test]",
        chat_stream=SimpleNamespace(platform=reply_tool_module.CLI_PLATFORM_NAME),
        session_id="session-1",
        _chat_history=chat_history,
        _clear_force_continue_until_reply=lambda: None,
        _record_reply_sent=lambda: None,
        run_sub_agent=None,
    )
    engine = SimpleNamespace(_get_runtime_manager=lambda: None)
    return BuiltinToolRuntimeContext(engine=engine, runtime=runtime)


def _build_reply_target_history_message() -> SessionBackedMessage:
    return SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("[测试用户] 问一下长记忆")]),
        visible_text="[测试用户] 问一下长记忆",
        timestamp=datetime.now(),
        message_id="msg-1",
        source_kind="user",
    )


def test_runtime_maps_expression_config_flags_to_correct_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_chat_stream = SimpleNamespace(
        is_group_session=True,
        group_id="group-1",
        user_id="user-1",
        platform="test",
    )

    monkeypatch.setattr(
        runtime_module.chat_manager,
        "get_session_by_session_id",
        lambda session_id: fake_chat_stream,
    )
    monkeypatch.setattr(runtime_module.chat_manager, "get_session_name", lambda session_id: "测试会话")
    monkeypatch.setattr(
        runtime_module.ExpressionConfigUtils,
        "get_expression_config_for_chat",
        staticmethod(lambda session_id: (True, False)),
    )
    monkeypatch.setattr(
        runtime_module.JargonConfigUtils,
        "get_jargon_config_for_chat",
        staticmethod(lambda session_id: (True, True)),
    )
    monkeypatch.setattr(runtime_module, "ExpressionLearner", lambda session_id: SimpleNamespace())
    monkeypatch.setattr(runtime_module, "JargonMiner", lambda session_id, session_name: SimpleNamespace())
    monkeypatch.setattr(runtime_module, "MaisakaReasoningEngine", lambda runtime: SimpleNamespace())
    monkeypatch.setattr(runtime_module, "ToolRegistry", lambda: SimpleNamespace())
    monkeypatch.setattr(runtime_module, "ReplyEffectTracker", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(MaisakaHeartFlowChatting, "_register_tool_providers", lambda self: None)
    monkeypatch.setattr(MaisakaHeartFlowChatting, "_emit_monitor_session_start", lambda self: None)

    runtime = MaisakaHeartFlowChatting("session-1")

    assert runtime._enable_expression_use is True
    assert runtime._enable_expression_learning is False
    assert runtime._enable_jargon_use is True
    assert runtime._enable_jargon_learning is True


class _FakeLLMResult:
    def __init__(self) -> None:
        self.response = "测试回复"
        self.reasoning = "先理解上下文，再给出自然回复。"
        self.model_name = "fake-model"
        self.tool_calls = []
        self.prompt_tokens = 12
        self.completion_tokens = 7
        self.total_tokens = 19


async def _call_message_factory(message_factory: Callable[..., Any], client: object) -> list[Any]:
    result = message_factory(client)
    if inspect.isawaitable(result):
        return await result
    return result


def test_reply_monitor_detail_displays_selected_expression_details() -> None:
    result = ReplyGenerationResult(
        selected_expression_ids=[212, 254],
        selected_expression_details=[
            {"id": 212, "situation": "有人开玩笑", "style": "轻松吐槽", "count": 1},
            {"id": 254, "situation": "气氛沉默", "style": "主动接话", "count": 1},
        ],
    )

    detail = build_reply_monitor_detail(result)
    expression_section = next(
        section
        for section in detail["extra_sections"]
        if section["title"] == "已选表达方式"
    )

    assert expression_section["content"] == (
        "212：当有人开玩笑时，可以自然地用轻松吐槽这种表达习惯。\n"
        "254：当气氛沉默时，可以自然地用主动接话这种表达习惯。"
    )


class _FakeLegacyLLMServiceClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args
        del kwargs

    async def generate_response_with_messages(
        self,
        *,
        message_factory: Callable[[object], list[Any]],
        options: Any = None,
    ) -> _FakeLLMResult:
        del options
        assert await _call_message_factory(message_factory, object())
        return _FakeLLMResult()


class _FakeMultimodalLLMServiceClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args
        del kwargs

    async def generate_response_with_messages(
        self,
        *,
        message_factory: Callable[[object], list[Any]],
        options: Any = None,
    ) -> _FakeLLMResult:
        del options
        assert await _call_message_factory(message_factory, object())
        return _FakeLLMResult()


class _FakeReplyerHookManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def invoke_hook(self, hook_name: str, **kwargs: Any) -> SimpleNamespace:
        self.calls.append((hook_name, dict(kwargs)))
        if hook_name == "maisaka.replyer.before_request":
            modified_kwargs = dict(kwargs)
            reply_tool_args = dict(modified_kwargs.get("reply_tool_args") or {})
            reply_tool_args["hook_added"] = "yes"
            modified_kwargs["reply_tool_args"] = reply_tool_args
            return SimpleNamespace(kwargs=modified_kwargs, aborted=False)
        return SimpleNamespace(kwargs=dict(kwargs), aborted=False)


class _FakeReplyerRoutingHookManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def invoke_hook(self, hook_name: str, **kwargs: Any) -> SimpleNamespace:
        self.calls.append((hook_name, dict(kwargs)))
        if hook_name == "maisaka.replyer.before_request":
            reply_tool_args = dict(kwargs.get("reply_tool_args") or {})
            if reply_tool_args.get("thinking_level") == "deep":
                kwargs["task_name"] = "planner"
                kwargs["model_name"] = "Qwen3.5-397B-A17B"
                kwargs["extra_prompt"] = "请更细致地理解上下文后再回复。"
        return SimpleNamespace(kwargs=dict(kwargs), aborted=False)


class _FakeReplyerMessageRewriteHookManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def invoke_hook(self, hook_name: str, **kwargs: Any) -> SimpleNamespace:
        self.calls.append((hook_name, dict(kwargs)))
        if hook_name == "maisaka.replyer.before_model_request":
            messages = list(kwargs.get("messages") or [])
            messages.insert(1, {"role": "user", "content": "注入的第一条 user marker"})
            kwargs["messages"] = messages
        return SimpleNamespace(kwargs=dict(kwargs), aborted=False)


class _FakeRoutingLLMServiceClient:
    task_names: list[str] = []
    model_names: list[str] = []
    prompt_texts: list[str] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args
        self.task_names.append(str(kwargs.get("task_name") or ""))

    async def generate_response_with_messages(
        self,
        *,
        message_factory: Callable[[object], list[Any]],
        options: Any = None,
    ) -> _FakeLLMResult:
        self.model_names.append(str(getattr(options, "model_name", "") or ""))
        messages = await _call_message_factory(message_factory, object())
        self.prompt_texts.append("\n".join(message.get_text_content() for message in messages))
        return _FakeLLMResult()


@pytest.mark.asyncio
async def test_legacy_and_multimodal_replyer_monitor_detail_have_same_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(replyer_module, "LLMServiceClient", _FakeLegacyLLMServiceClient)
    monkeypatch.setattr(replyer_module, "load_prompt", lambda *args, **kwargs: "legacy prompt")

    legacy_generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_legacy",
        enable_visual_message=False,
    )
    multimodal_generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_multi",
        llm_client_cls=_FakeMultimodalLLMServiceClient,
        load_prompt_func=lambda *args, **kwargs: "multi prompt",
        enable_visual_message=True,
    )

    legacy_success, legacy_result = await legacy_generator.generate_reply_with_context(
        stream_id="session-legacy",
        chat_history=[],
        reply_reason="测试原因",
    )
    multimodal_success, multimodal_result = await multimodal_generator.generate_reply_with_context(
        stream_id="session-multi",
        chat_history=[],
        reply_reason="测试原因",
    )

    assert legacy_success is True
    assert multimodal_success is True
    assert legacy_result.monitor_detail is not None
    assert multimodal_result.monitor_detail is not None
    assert set(legacy_result.monitor_detail.keys()) == set(multimodal_result.monitor_detail.keys())
    assert set(legacy_result.monitor_detail["metrics"].keys()) == set(multimodal_result.monitor_detail["metrics"].keys())
    assert legacy_result.monitor_detail["metrics"]["prompt_tokens"] == 12
    assert legacy_result.monitor_detail["metrics"]["completion_tokens"] == 7
    assert legacy_result.monitor_detail["metrics"]["total_tokens"] == 19


@pytest.mark.asyncio
async def test_replyer_hooks_receive_reply_tool_args(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(replyer_module, "LLMServiceClient", _FakeLegacyLLMServiceClient)
    monkeypatch.setattr(replyer_module, "load_prompt", lambda *args, **kwargs: "reply prompt")

    fake_hook_manager = _FakeReplyerHookManager()
    generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_reply_tool_args",
        enable_visual_message=False,
    )
    monkeypatch.setattr(generator, "_get_runtime_manager", lambda: fake_hook_manager)

    success, _ = await generator.generate_reply_with_context(
        stream_id="session-reply-tool-args",
        chat_history=[],
        reply_reason="测试原因",
        reply_tool_args={"route": "fast"},
    )

    assert success is True
    before_call = fake_hook_manager.calls[0]
    before_model_call = fake_hook_manager.calls[1]
    after_call = fake_hook_manager.calls[2]
    assert before_call[0] == "maisaka.replyer.before_request"
    assert before_call[1]["reply_tool_args"] == {"route": "fast"}
    assert before_model_call[0] == "maisaka.replyer.before_model_request"
    assert before_model_call[1]["reply_tool_args"] == {"route": "fast", "hook_added": "yes"}
    assert after_call[0] == "maisaka.replyer.after_response"
    assert after_call[1]["reply_tool_args"] == {"route": "fast", "hook_added": "yes"}


@pytest.mark.asyncio
async def test_replyer_before_request_can_route_task_and_append_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeRoutingLLMServiceClient.task_names = []
    _FakeRoutingLLMServiceClient.model_names = []
    _FakeRoutingLLMServiceClient.prompt_texts = []
    monkeypatch.setattr(replyer_module, "LLMServiceClient", _FakeRoutingLLMServiceClient)
    monkeypatch.setattr(replyer_module, "load_prompt", lambda *args, **kwargs: "reply prompt")

    fake_hook_manager = _FakeReplyerRoutingHookManager()
    generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_reply_routing",
        enable_visual_message=False,
    )
    monkeypatch.setattr(generator, "_get_runtime_manager", lambda: fake_hook_manager)

    success, _ = await generator.generate_reply_with_context(
        stream_id="session-reply-routing",
        chat_history=[],
        reply_reason="测试原因",
        reply_tool_args={"thinking_level": "deep"},
    )

    assert success is True
    assert _FakeRoutingLLMServiceClient.task_names == ["replyer", "planner"]
    assert _FakeRoutingLLMServiceClient.model_names == ["Qwen3.5-397B-A17B"]
    assert "请更细致地理解上下文后再回复。" in _FakeRoutingLLMServiceClient.prompt_texts[-1]
    assert fake_hook_manager.calls[-1][1]["task_name"] == "planner"
    assert fake_hook_manager.calls[-1][1]["requested_model_name"] == "Qwen3.5-397B-A17B"


@pytest.mark.asyncio
async def test_replyer_before_model_request_can_rewrite_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeRoutingLLMServiceClient.task_names = []
    _FakeRoutingLLMServiceClient.model_names = []
    _FakeRoutingLLMServiceClient.prompt_texts = []
    monkeypatch.setattr(replyer_module, "LLMServiceClient", _FakeRoutingLLMServiceClient)
    monkeypatch.setattr(replyer_module, "load_prompt", lambda *args, **kwargs: "reply prompt")

    fake_hook_manager = _FakeReplyerMessageRewriteHookManager()
    generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_reply_message_rewrite",
        enable_visual_message=False,
    )
    monkeypatch.setattr(generator, "_get_runtime_manager", lambda: fake_hook_manager)

    success, result = await generator.generate_reply_with_context(
        stream_id="session-reply-message-rewrite",
        chat_history=[],
        reply_reason="测试原因",
    )

    assert success is True
    assert fake_hook_manager.calls[1][0] == "maisaka.replyer.before_model_request"
    assert result.request_messages[1]["role"] == "user"
    assert result.request_messages[1]["content"] == "注入的第一条 user marker"
    assert "注入的第一条 user marker" in _FakeRoutingLLMServiceClient.prompt_texts[-1]


def test_legacy_replyer_builds_message_sequence_like_multimodal() -> None:
    legacy_generator = replyer_module.MaisakaReplyGenerator(
        chat_stream=None,
        request_type="test_legacy",
        load_prompt_func=lambda *args, **kwargs: "legacy prompt",
        enable_visual_message=False,
    )
    alice_message = SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("[Alice]你好")]),
        visible_text="[Alice]你好",
        timestamp=datetime.now(),
        source_kind="user",
    )
    bob_message = SessionBackedMessage(
        raw_message=MessageSequence([TextComponent("[Bob]在吗")]),
        visible_text="[Bob]在吗",
        timestamp=datetime.now(),
        source_kind="user",
    )
    request_messages = legacy_generator._build_request_messages(
        chat_history=[alice_message, bob_message],
        reply_message=None,
        reply_reason="测试原因",
        stream_id="session-legacy",
    )

    assert len(request_messages) == 4
    assert request_messages[0].role.value == "system"
    assert request_messages[0].get_text_content() == "legacy prompt"
    assert request_messages[1].role.value == "user"
    assert request_messages[1].get_text_content() == "[Alice]你好"
    assert request_messages[2].role.value == "user"
    assert request_messages[2].get_text_content() == "[Bob]在吗"
    assert request_messages[3].role.value == "user"
    assert "当前时间：" in request_messages[3].get_text_content()
    assert "【回复信息参考】" in request_messages[3].get_text_content()
    assert "【最新推理】\n测试原因" in request_messages[3].get_text_content()
    assert "请自然地回复。" in request_messages[3].get_text_content()


@pytest.mark.asyncio
async def test_reply_tool_puts_monitor_detail_into_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_monitor_detail = {
        "prompt_text": "reply prompt",
        "reasoning_text": "reply reasoning",
        "output_text": "reply output",
        "metrics": {"model_name": "fake-model", "total_tokens": 10},
    }
    fake_reply_result = ReplyGenerationResult(
        success=True,
        completion=LLMCompletionResult(response_text="测试回复"),
        metrics=GenerationMetrics(overall_ms=11.5),
        monitor_detail=fake_monitor_detail,
    )

    class _FakeReplyer:
        async def generate_reply_with_context(self, **kwargs: Any) -> tuple[bool, ReplyGenerationResult]:
            del kwargs
            return True, fake_reply_result

    monkeypatch.setattr(reply_tool_module.replyer_manager, "get_replyer", lambda **kwargs: _FakeReplyer())
    monkeypatch.setattr(reply_tool_module, "render_cli_message", lambda text: text)

    target_message = SimpleNamespace(
        message_id="msg-1",
        message_info=SimpleNamespace(
            user_info=SimpleNamespace(
                user_cardname="测试用户",
                user_nickname="测试用户",
                user_id="user-1",
            )
        ),
    )
    runtime = SimpleNamespace(
        find_source_message_by_id=lambda message_id: target_message if message_id == "msg-1" else None,
        log_prefix="[test]",
        chat_stream=SimpleNamespace(platform=reply_tool_module.CLI_PLATFORM_NAME),
        session_id="session-1",
        _chat_history=[],
        _clear_force_continue_until_reply=lambda: None,
        _record_reply_sent=lambda: None,
        run_sub_agent=None,
    )
    engine = SimpleNamespace(_get_runtime_manager=lambda: None)
    tool_ctx = BuiltinToolRuntimeContext(engine=engine, runtime=runtime)
    invocation = ToolInvocation(tool_name="reply", arguments={"msg_id": "msg-1", "set_quote": True})

    result = await reply_tool_module.handle_tool(tool_ctx, invocation)

    assert result.success is True
    assert result.metadata["monitor_detail"] == fake_monitor_detail


@pytest.mark.asyncio
async def test_reply_tool_drops_legacy_reference_info_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_reply_result = ReplyGenerationResult(
        success=True,
        completion=LLMCompletionResult(response_text="测试回复"),
        metrics=GenerationMetrics(overall_ms=11.5),
        monitor_detail={},
    )

    class _FakeReplyer:
        async def generate_reply_with_context(self, **kwargs: Any) -> tuple[bool, ReplyGenerationResult]:
            captured.update(kwargs)
            return True, fake_reply_result

    monkeypatch.setattr(reply_tool_module.replyer_manager, "get_replyer", lambda **kwargs: _FakeReplyer())
    monkeypatch.setattr(reply_tool_module, "render_cli_message", lambda text: text)

    target_message = SimpleNamespace(
        message_id="msg-1",
        message_info=SimpleNamespace(
            user_info=SimpleNamespace(
                user_cardname="测试用户",
                user_nickname="测试用户",
                user_id="user-1",
            )
        ),
    )
    runtime = SimpleNamespace(
        find_source_message_by_id=lambda message_id: target_message if message_id == "msg-1" else None,
        log_prefix="[test]",
        chat_stream=SimpleNamespace(platform=reply_tool_module.CLI_PLATFORM_NAME),
        session_id="session-1",
        _chat_history=[],
        _clear_force_continue_until_reply=lambda: None,
        _record_reply_sent=lambda: None,
        run_sub_agent=None,
    )
    engine = SimpleNamespace(_get_runtime_manager=lambda: None)
    tool_ctx = BuiltinToolRuntimeContext(engine=engine, runtime=runtime)
    invocation = ToolInvocation(
        tool_name="reply",
        arguments={"msg_id": "msg-1", "reference_info": "已有参考"},
    )

    result = await reply_tool_module.handle_tool(tool_ctx, invocation)

    assert result.success is True
    assert "reference_info" not in captured
    assert "reference_info" not in captured["reply_tool_args"]


@pytest.mark.asyncio
async def test_send_emoji_tool_puts_monitor_detail_into_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_build_emoji_candidate_message(emojis: list[Any]) -> object:
        assert emojis
        return SimpleNamespace()

    async def _fake_send_emoji_for_maisaka(**kwargs: Any) -> Any:
        selected_emoji, matched_emotion = await kwargs["emoji_selector"](
            kwargs["requested_emotion"],
            kwargs["reasoning"],
            kwargs["context_texts"],
            2,
        )
        assert selected_emoji is not None
        return SimpleNamespace(
            success=True,
            message="已发送表情包：开心",
            emoji_base64="ZW1vamk=",
            description="开心",
            emotions=["开心", "可爱"],
            matched_emotion=matched_emotion or "开心",
            sent_message=None,
        )

    monkeypatch.setattr(send_emoji_tool_module, "_build_emoji_candidate_message", _fake_build_emoji_candidate_message)
    monkeypatch.setattr(send_emoji_tool_module, "send_emoji_for_maisaka", _fake_send_emoji_for_maisaka)

    async def _fake_render_emoji_selection_system_prompt(**kwargs: Any) -> str:
        return f"测试表情选择提示：{kwargs['grid_rows']}x{kwargs['grid_columns']} / {kwargs['emoji_count']}"

    monkeypatch.setattr(
        send_emoji_tool_module,
        "_render_emoji_selection_system_prompt",
        _fake_render_emoji_selection_system_prompt,
    )
    monkeypatch.setattr(
        send_emoji_tool_module.emoji_manager,
        "emojis",
        [
            SimpleNamespace(description="开心,可爱", emotion=["开心", "可爱"]),
            SimpleNamespace(description="难过", emotion=["难过"]),
        ],
    )

    async def _fake_run_sub_agent(**kwargs: Any) -> Any:
        del kwargs
        return SimpleNamespace(
            content='{"emoji_index": 1, "reason": "更贴合当前语气"}',
            prompt_tokens=9,
            completion_tokens=6,
            total_tokens=15,
        )

    runtime = SimpleNamespace(
        _chat_history=[],
        log_prefix="[test]",
        session_id="session-emoji",
        run_sub_agent=_fake_run_sub_agent,
    )
    engine = SimpleNamespace(last_reasoning_content="用户刚刚表达了开心情绪")
    tool_ctx = BuiltinToolRuntimeContext(engine=engine, runtime=runtime)
    invocation = ToolInvocation(tool_name="send_emoji", arguments={"emotion": "开心"})

    result = await send_emoji_tool_module.handle_tool(tool_ctx, invocation)

    assert result.success is True
    assert result.metadata["monitor_detail"]["prompt_text"]
    assert result.metadata["monitor_detail"]["reasoning_text"] == "更贴合当前语气"
    assert result.metadata["monitor_detail"]["metrics"]["total_tokens"] == 15
    assert any(
        section["title"] == "表情发送结果"
        for section in result.metadata["monitor_detail"]["extra_sections"]
    )


@pytest.mark.asyncio
async def test_emit_planner_finalized_broadcasts_new_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _fake_broadcast(event: str, data: dict[str, Any]) -> None:
        captured["event"] = event
        captured["data"] = data

    monkeypatch.setattr("src.maisaka.monitor_events._broadcast", _fake_broadcast)

    await emit_planner_finalized(
        session_id="session-1",
        cycle_id=3,
        timing_request_messages=[{"role": "user", "content": "先看看要不要继续"}],
        timing_selected_history_count=3,
        timing_tool_count=1,
        timing_action="continue",
        timing_content="继续",
        timing_tool_calls=[SimpleNamespace(call_id="timing-call-1", func_name="continue", args={})],
        timing_tool_results=["- continue [成功]: 继续执行"],
        timing_prompt_tokens=40,
        timing_completion_tokens=5,
        timing_total_tokens=45,
        timing_duration_ms=11.2,
        planner_request_messages=[{"role": "user", "content": "你好"}],
        planner_selected_history_count=5,
        planner_tool_count=2,
        planner_content="先查询再回复",
        planner_tool_calls=[SimpleNamespace(call_id="call-1", func_name="reply", args={"msg_id": "m1"})],
        planner_prompt_tokens=100,
        planner_completion_tokens=30,
        planner_total_tokens=130,
        planner_duration_ms=88.5,
        tools=[
            {
                "tool_call_id": "call-1",
                "tool_name": "reply",
                "tool_args": {"msg_id": "m1"},
                "success": True,
                "duration_ms": 22.0,
                "summary": "- reply [成功]: 已回复",
                "detail": {"output_text": "测试回复"},
            }
        ],
        time_records={"planner": 0.1, "tool_calls": 0.2},
        agent_state="stop",
    )

    assert captured["event"] == "planner.finalized"
    payload = captured["data"]
    assert payload["timing_gate"]["result"]["action"] == "continue"
    assert payload["timing_gate"]["result"]["tool_results"] == ["- continue [成功]: 继续执行"]
    assert payload["request"]["messages"][0]["content"] == "你好"
    assert payload["request"]["tool_count"] == 2
    assert payload["planner"]["tool_calls"][0]["id"] == "call-1"
    assert payload["tools"][0]["detail"]["output_text"] == "测试回复"
    assert payload["final_state"]["agent_state"] == "stop"


@pytest.mark.asyncio
async def test_emit_planner_finalized_supports_timing_only_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def _fake_broadcast(event: str, data: dict[str, Any]) -> None:
        captured["event"] = event
        captured["data"] = data

    monkeypatch.setattr("src.maisaka.monitor_events._broadcast", _fake_broadcast)

    await emit_planner_finalized(
        session_id="session-2",
        cycle_id=7,
        timing_request_messages=[{"role": "user", "content": "先别回"}],
        timing_selected_history_count=2,
        timing_tool_count=1,
        timing_action="no_reply",
        timing_content="当前不适合继续",
        timing_tool_calls=[SimpleNamespace(call_id="timing-call-2", func_name="no_reply", args={})],
        timing_tool_results=["- no_reply [成功]: 暂停当前对话"],
        timing_prompt_tokens=18,
        timing_completion_tokens=4,
        timing_total_tokens=22,
        timing_duration_ms=6.5,
        planner_request_messages=None,
        planner_selected_history_count=None,
        planner_tool_count=None,
        planner_content=None,
        planner_tool_calls=None,
        planner_prompt_tokens=None,
        planner_completion_tokens=None,
        planner_total_tokens=None,
        planner_duration_ms=None,
        tools=[],
        time_records={"timing_gate": 0.02},
        agent_state="stop",
    )

    assert captured["event"] == "planner.finalized"
    payload = captured["data"]
    assert payload["timing_gate"]["result"]["action"] == "no_reply"
    assert payload["planner"] is None
    assert payload["request"] is None


def test_reasoning_engine_build_tool_monitor_result_keeps_non_reply_tool_without_detail() -> None:
    engine = object.__new__(MaisakaReasoningEngine)
    tool_call = SimpleNamespace(call_id="call-2", func_name="query_memory")
    invocation = ToolInvocation(tool_name="query_memory", arguments={"query": "Alice"})
    result = ToolExecutionResult(tool_name="query_memory", success=True, content="查询成功")

    tool_result = engine._build_tool_monitor_result(tool_call, invocation, result, duration_ms=18.6)

    assert tool_result["tool_call_id"] == "call-2"
    assert tool_result["tool_name"] == "query_memory"
    assert tool_result["tool_args"] == {"query": "Alice"}
    assert tool_result["detail"] is None


def test_runtime_build_tool_detail_panels_renders_reply_monitor_detail() -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "session-1"
    panels = runtime._build_tool_detail_cards(
        [
            {
                "tool_call_id": "call-reply-1",
                "tool_name": "reply",
                "tool_args": {"msg_id": "m1"},
                "success": True,
                "duration_ms": 20.5,
                "summary": "- reply [成功]: 已回复",
                "detail": {
                    "prompt_text": "reply prompt",
                    "reasoning_text": "reply reasoning",
                    "output_text": "reply output",
                    "metrics": {
                        "model_name": "fake-model",
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                        "prompt_ms": 2.1,
                        "llm_ms": 18.4,
                        "overall_ms": 20.5,
                    },
                },
            }
        ],
        stage_title="工具调用",
    )

    assert len(panels) == 1
    assert isinstance(panels[0], Panel)


def test_runtime_filter_redundant_tool_results_keeps_only_non_detailed_summary() -> None:
    filtered_results = MaisakaHeartFlowChatting._filter_redundant_tool_results(
        tool_results=[
            "- reply [成功]: 已回复",
            "- query_memory [成功]: 查询到 2 条记录",
        ],
        tool_detail_results=[
            {
                "summary": "- reply [成功]: 已回复",
                "detail": {"output_text": "测试回复"},
            }
        ],
    )

    assert filtered_results == ["- query_memory [成功]: 查询到 2 条记录"]


def test_runtime_build_tool_detail_panels_uses_prompt_access_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "session-link"
    captured: dict[str, Any] = {}

    def _fake_build_text_access_panel(content: str, **kwargs: Any) -> str:
        captured["content"] = content
        captured["kwargs"] = kwargs
        return "PROMPT_LINK"

    monkeypatch.setattr(
        "src.maisaka.runtime.PromptCLIVisualizer.build_text_access_panel",
        _fake_build_text_access_panel,
    )

    panels = runtime._build_tool_detail_cards(
        [
            {
                "tool_call_id": "call-reply-2",
                "tool_name": "reply",
                "tool_args": {"msg_id": "m2"},
                "success": True,
                "duration_ms": 12.0,
                "summary": "- reply [成功]: 已回复",
                "detail": {
                    "prompt_text": "reply prompt link",
                    "output_text": "reply output",
                },
            }
        ],
        stage_title="工具调用",
    )

    assert len(panels) == 1
    assert captured["content"] == "reply prompt link"
    assert captured["kwargs"]["chat_id"] == "session-link"
    assert captured["kwargs"]["request_kind"] == "replyer"


def test_runtime_build_tool_detail_panels_uses_emotion_prompt_access_panel(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "session-emotion"
    captured: dict[str, Any] = {}

    def _fake_build_text_access_panel(content: str, **kwargs: Any) -> str:
        captured["content"] = content
        captured["kwargs"] = kwargs
        return "EMOTION_PROMPT_LINK"

    monkeypatch.setattr(
        "src.maisaka.runtime.PromptCLIVisualizer.build_text_access_panel",
        _fake_build_text_access_panel,
    )

    panels = runtime._build_tool_detail_cards(
        [
            {
                "tool_call_id": "call-emoji-1",
                "tool_name": "send_emoji",
                "tool_args": {"emotion": "开心"},
                "success": True,
                "duration_ms": 15.0,
                "summary": "- send_emoji [成功]: 已发送表情包",
                "detail": {
                    "prompt_text": "emotion prompt link",
                    "output_text": '{"emoji_index": 1}',
                },
            }
        ],
        stage_title="工具调用",
    )

    assert len(panels) == 1
    assert captured["content"] == "emotion prompt link"
    assert captured["kwargs"]["chat_id"] == "session-emotion"
    assert captured["kwargs"]["request_kind"] == "emotion"


def test_runtime_build_tool_detail_cards_uses_structured_prompt_messages_with_images(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "session-image"
    captured: dict[str, Any] = {}

    def _fake_build_prompt_access_panel(messages: list[Any], **kwargs: Any) -> str:
        captured["messages"] = messages
        captured["kwargs"] = kwargs
        return "IMAGE_PROMPT_LINK"

    def _fake_build_text_access_panel(content: str, **kwargs: Any) -> str:
        captured["text_content"] = content
        captured["text_kwargs"] = kwargs
        return "TEXT_PROMPT_LINK"

    monkeypatch.setattr(
        "src.maisaka.runtime.PromptCLIVisualizer.build_prompt_access_panel",
        _fake_build_prompt_access_panel,
    )
    monkeypatch.setattr(
        "src.maisaka.runtime.PromptCLIVisualizer.build_text_access_panel",
        _fake_build_text_access_panel,
    )

    panels = runtime._build_tool_detail_cards(
        [
            {
                "tool_call_id": "call-reply-image-1",
                "tool_name": "reply",
                "tool_args": {"msg_id": "m3"},
                "success": True,
                "duration_ms": 22.0,
                "summary": "- reply [成功]: 已回复",
                "detail": {
                    "prompt_text": "reply prompt image",
                    "request_messages": [
                        {
                            "role": "user",
                            "content": ["前缀文本", ["png", "ZmFrZQ=="]],
                        }
                    ],
                    "output_text": "reply output",
                },
            }
        ],
        stage_title="工具调用",
    )

    assert len(panels) == 1
    assert "messages" in captured
    assert "text_content" not in captured
    assert captured["kwargs"]["chat_id"] == "session-image"
    assert captured["kwargs"]["request_kind"] == "replyer"


def test_runtime_render_context_usage_panel_merges_timing_and_planner(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime.session_id = "session-merged"
    runtime.session_name = "测试聊天流"
    monkeypatch.setattr(runtime, "_get_effective_reply_frequency", lambda: 0.42)

    printed: list[Any] = []
    monkeypatch.setattr("src.maisaka.runtime.console.print", lambda renderable: printed.append(renderable))

    runtime._render_context_usage_panel(
        cycle_id=12,
        timing_selected_history_count=3,
        timing_prompt_tokens=15,
        timing_action="continue",
        timing_response="继续执行",
        planner_selected_history_count=5,
        planner_prompt_tokens=42,
        planner_response="先查询再回复",
    )

    assert len(printed) == 1
    outer_panel = printed[0]
    assert isinstance(outer_panel, Panel)
    renderables = list(outer_panel.renderable.renderables)
    assert isinstance(renderables[0], Text)
    assert "聊天流名称：测试聊天流" in renderables[0].plain
    assert "聊天流ID：session-merged" in renderables[0].plain
    assert "当前回复频率：0.420（42.0%）" in renderables[0].plain
    assert len(renderables) == 3
