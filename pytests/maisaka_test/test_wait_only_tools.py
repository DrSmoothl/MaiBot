from src.config.config import global_config
from src.core.tooling import ToolAvailabilityContext
from src.maisaka.builtin_tool import get_builtin_tools, get_timing_tools


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

    assert "no_action" in tool_names
    assert "wait" not in tool_names


def test_timing_gate_keeps_no_action_when_new_maisaka_disabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", False)

    tool_names = _tool_names(get_timing_tools(_availability_context()))

    assert "no_action" in tool_names
    assert "wait" in tool_names


def test_planner_wait_replaces_no_action_tool_when_new_maisaka_enabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", True)

    tool_names = _tool_names(get_builtin_tools(_availability_context()))

    assert "wait" in tool_names
    assert "no_action" not in tool_names


def test_timing_gate_wait_replaces_no_action_tool_when_new_maisaka_enabled(monkeypatch) -> None:
    monkeypatch.setattr(global_config.chat, "enable_new_maisaka", True)

    tool_names = _tool_names(get_timing_tools(_availability_context()))

    assert "wait" in tool_names
    assert "no_action" not in tool_names
