"""连续等待只提醒，不强制停止普通或 Focus 会话。"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.common.i18n import use_locale
from src.maisaka.chat_loop_service import MaisakaChatLoopService
from src.maisaka.focus.group_runtime import FocusGroupRuntime
from src.maisaka.mode_policy import build_consecutive_wait_reminders
from src.maisaka.runtime import MaisakaHeartFlowChatting


@pytest.mark.parametrize("locale", ["zh-CN", "en-US", "ja-JP"])
def test_reminder_starts_at_three_and_survives_further_waits(locale: str) -> None:
    with use_locale(locale):
        assert build_consecutive_wait_reminders(2) == []
        reminders = build_consecutive_wait_reminders(3)
        assert len(reminders) == 1
        assert "wait" in reminders[0] and "stop" in reminders[0]
        assert build_consecutive_wait_reminders(4) == reminders
        assert build_consecutive_wait_reminders(0) == []


def test_normal_wait_keeps_waiting_beyond_three_and_can_reset() -> None:
    runtime = SimpleNamespace(_consecutive_wait_count=0, _enter_wait_state=Mock(), log_prefix="[test]")
    for count in range(1, 6):
        assert MaisakaHeartFlowChatting._enter_counted_wait_state(runtime, seconds=60) == count
    assert runtime._enter_wait_state.call_count == 5
    MaisakaHeartFlowChatting._reset_consecutive_wait_count(runtime, "tool:reply")
    assert build_consecutive_wait_reminders(runtime._consecutive_wait_count) == []


def test_focus_wait_keeps_waiting_beyond_three_and_stop_resets() -> None:
    group = SimpleNamespace(_consecutive_waits=0, _pending={})
    for count in range(1, 6):
        result = FocusGroupRuntime.execute_control(group, "wait", {"seconds": 60}, "chat")
        assert result.success and result.metadata["pause_execution"]
        assert group._consecutive_waits == count
        assert group._wait_until is not None
        assert group._wait_session_id == "chat"
    FocusGroupRuntime.execute_control(group, "stop", {}, "chat")
    assert build_consecutive_wait_reminders(group._consecutive_waits) == []


def test_wait_reminder_is_in_request_tail_without_persisting_in_history() -> None:
    service = MaisakaChatLoopService(is_group_chat=True)
    with use_locale("zh-CN"):
        reminders = build_consecutive_wait_reminders(3)
        items, history_count = service._build_request_messages(
            [], enable_visual_message=False, tail_user_messages=reminders, system_prompt="test"
        )
    assert history_count == 0
    assert any(reminders[0] in str(item) for item in items[1:])
