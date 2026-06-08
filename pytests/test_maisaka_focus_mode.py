from types import SimpleNamespace

import pytest

from src.chat.message_receive.chat_manager import BotChatSession, chat_manager
from src.config.config import global_config
from src.config.official_configs import ChatStreamGroup, TargetItem
from src.core.tooling import ToolAvailabilityContext
from src.maisaka.builtin_tool import get_all_builtin_tool_specs, get_builtin_tools
from src.maisaka.builtin_tool.fetch_histroy import get_tool_spec as get_fetch_histroy_tool_spec
from src.maisaka.focus import runtime_mixin as focus_runtime_mixin
from src.maisaka.focus.manager import FocusModeManager
from src.maisaka.focus.runtime_mixin import FOCUS_NO_ACTION_EXIT_THRESHOLD, MaisakaFocusRuntimeMixin


class _FakeFocusModeManager:
    def __init__(self) -> None:
        self.released_session_ids: list[str] = []

    @staticmethod
    def is_enabled() -> bool:
        return True

    @staticmethod
    def is_enabled_for_chat(*, is_group_chat: bool | None = None) -> bool:
        del is_group_chat
        return True

    def release_focus_and_block_next_entry(self, session_id: str) -> bool:
        self.released_session_ids.append(session_id)
        return True


class _FocusRuntimeStub(MaisakaFocusRuntimeMixin):
    def __init__(self, *, is_group_session: bool = True) -> None:
        self.session_id = "group-a"
        self.chat_stream = SimpleNamespace(is_group_session=is_group_session)
        self.log_prefix = "[group-a]"
        self._consecutive_no_action_count = 0
        self._focus_cooldown_wakeup_scheduled = True
        self.cancel_focus_cooldown_count = 0

    def _cancel_focus_cooldown_timer_task(self) -> None:
        self.cancel_focus_cooldown_count += 1


class _FetchHistoryRuntimeStub(MaisakaFocusRuntimeMixin):
    def __init__(self) -> None:
        self.message_cache = [
            SimpleNamespace(message_id="m1"),
            SimpleNamespace(message_id="m2"),
            SimpleNamespace(message_id="m3"),
            SimpleNamespace(message_id="m4"),
        ]
        self._chat_history = [
            SimpleNamespace(message_id="m2"),
        ]


def _build_group_session(session_id: str, group_id: str, *, platform: str = "test") -> BotChatSession:
    return BotChatSession(session_id=session_id, platform=platform, group_id=group_id)


def _build_focus_group(*group_ids: str, platform: str = "test") -> ChatStreamGroup:
    return ChatStreamGroup(
        targets=[
            TargetItem(platform=platform, item_id=group_id, rule_type="group")
            for group_id in group_ids
        ]
    )


def test_fetch_histroy_tool_spec_only_accepts_num(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)

    tool_spec = get_fetch_histroy_tool_spec()
    properties = tool_spec.parameters_schema["properties"]
    tool_names = {tool["name"] for tool in get_builtin_tools()}

    assert tool_spec.name == "fetch_histroy"
    assert set(properties) == {"num"}
    assert "fetch_histroy" in tool_names
    assert "fetch_new_message" not in tool_names


def test_focus_tools_hidden_for_private_chat_when_focus_on_private_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_on_private", False)

    tool_names = {
        tool_spec.name
        for tool_spec in get_all_builtin_tool_specs(ToolAvailabilityContext(is_group_chat=False))
    }

    assert "fetch_histroy" not in tool_names
    assert "switch_chat" not in tool_names


def test_focus_tools_visible_for_private_chat_when_focus_on_private_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_on_private", True)

    tool_names = {
        tool_spec.name
        for tool_spec in get_all_builtin_tool_specs(ToolAvailabilityContext(is_group_chat=False))
    }

    assert "fetch_histroy" in tool_names
    assert "switch_chat" in tool_names


def test_fetch_histroy_selects_current_stream_messages_newest_first() -> None:
    runtime = _FetchHistoryRuntimeStub()

    fetched_messages = runtime._get_focus_fetch_history_messages(limit=2)

    assert [message.message_id for message in fetched_messages] == ["m4", "m3"]


def test_focus_reentry_block_skips_same_session_until_another_enters(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_groups", [])

    assert manager.try_enter_focus("group-a") is True
    assert manager.release_focus_and_block_next_entry("group-a") is True
    assert manager.try_enter_focus("group-a") is False

    assert manager.try_enter_focus("group-b") is True
    manager.release_focus("group-b")

    assert manager.try_enter_focus("group-a") is True


def test_private_chat_does_not_enter_focus_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_on_private", False)

    assert manager.try_enter_focus("private-a", is_group_chat=False) is True
    assert manager.is_in_focus_set("private-a") is False
    assert manager.can_decide("private-a", is_group_chat=False) is True


def test_private_chat_can_enter_focus_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_on_private", True)

    assert manager.try_enter_focus("private-a", is_group_chat=False) is True
    assert manager.is_in_focus_set("private-a") is True


def test_empty_focus_groups_share_one_global_focus(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_groups", [])

    assert manager.try_enter_focus("group-a", is_group_chat=True) is True
    assert manager.try_enter_focus("group-b", is_group_chat=True) is False


def test_focus_groups_allow_parallel_focus_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    sessions = {
        "group-a": _build_group_session("group-a", "10001"),
        "group-b": _build_group_session("group-b", "10002"),
        "group-c": _build_group_session("group-c", "20001"),
    }
    monkeypatch.setattr(chat_manager, "sessions", sessions)
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(
        global_config.experimental,
        "focus_groups",
        [
            _build_focus_group("10001", "10002"),
            _build_focus_group("20001"),
        ],
    )

    assert manager.is_same_focus_scope("group-a", "group-b") is True
    assert manager.is_same_focus_scope("group-a", "group-c") is False
    assert manager.try_enter_focus("group-a", is_group_chat=True) is True
    assert manager.try_enter_focus("group-b", is_group_chat=True) is False
    assert manager.try_enter_focus("group-c", is_group_chat=True) is True
    assert manager.is_in_focus_set("group-a") is True
    assert manager.is_in_focus_set("group-c") is True


def test_unmatched_focus_group_chats_are_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    sessions = {
        "group-a": _build_group_session("group-a", "10001"),
        "group-x": _build_group_session("group-x", "90001"),
        "group-y": _build_group_session("group-y", "90002"),
    }
    monkeypatch.setattr(chat_manager, "sessions", sessions)
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(global_config.experimental, "focus_groups", [_build_focus_group("10001")])

    assert manager.is_same_focus_scope("group-x", "group-y") is False
    assert manager.try_enter_focus("group-x", is_group_chat=True) is True
    assert manager.try_enter_focus("group-y", is_group_chat=True) is True


def test_switch_focus_rejects_cross_focus_group(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = FocusModeManager()
    sessions = {
        "group-a": _build_group_session("group-a", "10001"),
        "group-c": _build_group_session("group-c", "20001"),
    }
    monkeypatch.setattr(chat_manager, "sessions", sessions)
    monkeypatch.setattr(global_config.experimental, "focus_mode", True)
    monkeypatch.setattr(
        global_config.experimental,
        "focus_groups",
        [
            _build_focus_group("10001"),
            _build_focus_group("20001"),
        ],
    )

    assert manager.try_enter_focus("group-a", is_group_chat=True) is True

    switch_error = manager.switch_focus("group-a", "group-c")

    assert "不在当前 Focus 互通组内" in switch_error
    assert manager.try_enter_focus("group-c", is_group_chat=True) is True


def test_consecutive_no_action_releases_group_focus(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_manager = _FakeFocusModeManager()
    monkeypatch.setattr(focus_runtime_mixin, "focus_mode_manager", fake_manager)
    runtime = _FocusRuntimeStub(is_group_session=True)

    for _ in range(FOCUS_NO_ACTION_EXIT_THRESHOLD - 1):
        runtime.record_no_action_cycle_result("tool_pause:no_action")

    assert fake_manager.released_session_ids == []
    assert runtime._consecutive_no_action_count == FOCUS_NO_ACTION_EXIT_THRESHOLD - 1

    runtime.record_no_action_cycle_result("timing_no_action")

    assert fake_manager.released_session_ids == ["group-a"]
    assert runtime._consecutive_no_action_count == 0
    assert runtime.cancel_focus_cooldown_count == 1
    assert runtime._focus_cooldown_wakeup_scheduled is False


def test_consecutive_no_action_exit_only_applies_to_group_focus(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_manager = _FakeFocusModeManager()
    monkeypatch.setattr(focus_runtime_mixin, "focus_mode_manager", fake_manager)
    runtime = _FocusRuntimeStub(is_group_session=False)

    for _ in range(FOCUS_NO_ACTION_EXIT_THRESHOLD):
        runtime.record_no_action_cycle_result("tool_pause:no_action")

    assert fake_manager.released_session_ids == []
    assert runtime._consecutive_no_action_count == 0
