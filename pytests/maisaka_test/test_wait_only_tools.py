from src.config.config import global_config
from src.core.tooling import ToolAvailabilityContext
from src.maisaka.builtin_tool import get_builtin_tools, get_timing_tools
from src.maisaka.mode_policy import is_no_action_equivalent_cycle_reason, is_reply_necessity_trigger_enabled
from src.maisaka.reasoning_engine import MaisakaReasoningEngine


def _tool_names(tool_definitions: list[dict]) -> set[str]:
    return {
        str(tool_definition.get("name") or "").strip()
        for tool_definition in tool_definitions
        if str(tool_definition.get("name") or "").strip()
    }


def _availability_context() -> ToolAvailabilityContext:
    return ToolAvailabilityContext(
        session_id="session-1",
        stream_id="session-1",
        is_group_chat=True,
    )


def test_planner_uses_no_action_when_new_maisaka_disabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", False)

    tool_names = _tool_names(get_builtin_tools(_availability_context()))

    assert "finish" in tool_names
    assert "no_action" in tool_names
    assert "wait" not in tool_names


def test_timing_gate_keeps_no_action_when_new_maisaka_disabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", False)

    tool_names = _tool_names(get_timing_tools(_availability_context()))

    assert "no_action" in tool_names
    assert "wait" in tool_names


def test_planner_wait_replaces_no_action_and_finish_when_new_maisaka_enabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", True)

    tool_names = _tool_names(get_builtin_tools(_availability_context()))

    assert "wait" in tool_names
    assert "finish" not in tool_names
    assert "no_action" not in tool_names


def test_timing_gate_wait_replaces_no_action_tool_when_new_maisaka_enabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", True)

    tool_names = _tool_names(get_timing_tools(_availability_context()))

    assert "wait" in tool_names
    assert "no_action" not in tool_names


def test_new_maisaka_treats_planner_no_tool_as_finish(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", True)

    class DummyRuntime:
        log_prefix = "[test]"

        def __init__(self) -> None:
            self._chat_history = []
            self.finished = False
            self.stopped = False

        def _finish_planner_continuation(self) -> None:
            self.finished = True

        def _enter_stop_state(self) -> None:
            self.stopped = True

    runtime = DummyRuntime()
    engine = MaisakaReasoningEngine.__new__(MaisakaReasoningEngine)
    engine._runtime = runtime
    planner_extra_lines: list[str] = []

    count, cycle_end, should_finish = engine._handle_planner_no_tool_retry(
        0,
        planner_extra_lines,
    )

    assert count == 1
    assert cycle_end.reason == "planner_no_tool_finish"
    assert is_no_action_equivalent_cycle_reason(cycle_end.reason)
    assert "结束" in cycle_end.detail
    assert planner_extra_lines == ["状态：未调用工具，已结束本轮思考"]
    assert should_finish is True
    assert runtime.finished is True
    assert runtime.stopped is True


def test_reply_necessity_trigger_is_optional(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_reply_necessity_trigger", False, raising=False)

    assert is_reply_necessity_trigger_enabled() is False

    monkeypatch.setattr(global_config.chat, "enable_reply_necessity_trigger", True, raising=False)

    assert is_reply_necessity_trigger_enabled() is True
