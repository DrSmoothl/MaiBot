from src.core.tooling import ToolAvailabilityContext
from src.config.config import global_config
from src.maisaka.builtin_tool import get_builtin_tools
from src.maisaka.mode_policy import is_idle_cycle_reason, is_reply_necessity_trigger_enabled
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


def test_planner_exposes_wait_without_no_action_or_finish() -> None:
    tool_names = _tool_names(get_builtin_tools(_availability_context()))

    assert "wait" in tool_names
    assert "finish" not in tool_names
    assert "no_action" not in tool_names


def test_planner_no_tool_ends_cycle() -> None:
    class DummyRuntime:
        log_prefix = "[test]"

        def __init__(self) -> None:
            self._chat_history = []
            self.ended = False
            self.stopped = False

        def _end_planner_continuation(self) -> None:
            self.ended = True

        def _enter_stop_state(self) -> None:
            self.stopped = True

    runtime = DummyRuntime()
    engine = MaisakaReasoningEngine.__new__(MaisakaReasoningEngine)
    engine._runtime = runtime
    planner_extra_lines: list[str] = []

    count, cycle_end, should_end = engine._handle_planner_no_tool_retry(
        0,
        planner_extra_lines,
    )

    assert count == 1
    assert cycle_end.reason == "planner_no_tool_end"
    assert is_idle_cycle_reason(cycle_end.reason)
    assert "结束" in cycle_end.detail
    assert planner_extra_lines == ["状态：未调用工具，已结束本轮思考"]
    assert should_end is True
    assert runtime.ended is True
    assert runtime.stopped is True


def test_reply_necessity_trigger_is_optional(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_reply_necessity_trigger", False, raising=False)

    assert is_reply_necessity_trigger_enabled() is False

    monkeypatch.setattr(global_config.chat, "enable_reply_necessity_trigger", True, raising=False)

    assert is_reply_necessity_trigger_enabled() is True
