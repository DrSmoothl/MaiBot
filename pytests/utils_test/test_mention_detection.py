from datetime import datetime
from types import SimpleNamespace

import pytest

from src.chat.message_receive.message import SessionMessage
from src.chat.utils import utils as chat_utils
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.maisaka import runtime as maisaka_runtime
from src.maisaka.runtime import MaisakaHeartFlowChatting


def _message(text: str, *, additional_config: dict | None = None) -> SessionMessage:
    message = SessionMessage(
        message_id=f"msg-{abs(hash((text, repr(additional_config))))}",
        timestamp=datetime.now(),
        platform="qq",
    )
    message.message_info = MessageInfo(
        user_info=UserInfo(user_id="user-1", user_nickname="用户"),
        additional_config=additional_config or {},
    )
    message.processed_plain_text = text
    return message


@pytest.fixture
def mention_config(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    fake_config = SimpleNamespace(
        bot=SimpleNamespace(
            qq_account="123456",
            nickname="麦麦",
            alias_names=["牢麦"],
            platforms=[],
        ),
        chat=SimpleNamespace(
            reply_timing=SimpleNamespace(
                inevitable_at_reply=False,
                mentioned_bot_reply=False,
            ),
            inevitable_at_reply=True,
            mentioned_bot_reply=True,
        ),
    )
    monkeypatch.setattr(chat_utils, "global_config", fake_config)
    monkeypatch.setattr(maisaka_runtime, "global_config", fake_config)
    return fake_config


def test_mention_detection_uses_reply_timing_config_not_legacy_chat_keys(
    mention_config: SimpleNamespace,
) -> None:
    mentioned, is_at, reply_probability = chat_utils.is_mentioned_bot_in_message(_message("@<麦麦:123456> 你好"))

    assert mentioned is True
    assert is_at is True
    assert reply_probability == 0.0


def test_boolean_mention_marker_does_not_become_probability_boost(mention_config: SimpleNamespace) -> None:
    mentioned, is_at, reply_probability = chat_utils.is_mentioned_bot_in_message(
        _message("你好", additional_config={"is_mentioned": True})
    )

    assert mentioned is True
    assert is_at is False
    assert reply_probability == 0.0


def test_numeric_mention_marker_still_provides_explicit_probability_boost(mention_config: SimpleNamespace) -> None:
    mentioned, is_at, reply_probability = chat_utils.is_mentioned_bot_in_message(
        _message("你好", additional_config={"is_mentioned": "0.6"})
    )

    assert mentioned is True
    assert is_at is False
    assert reply_probability == 0.6


def test_runtime_does_not_force_turn_when_at_reply_switch_is_disabled(mention_config: SimpleNamespace) -> None:
    runtime = MaisakaHeartFlowChatting.__new__(MaisakaHeartFlowChatting)
    runtime._forced_turn_enabled = False
    runtime._forced_turn_message_id = ""
    runtime._forced_turn_reason = ""
    runtime._idle_backoff = SimpleNamespace(reset=lambda: None)
    runtime.log_prefix = "[测试]"

    runtime._update_message_trigger_state(_message("@<麦麦:123456> 你好"))

    assert runtime._has_forced_turn_trigger() is False


def test_runtime_forces_turn_when_at_reply_switch_is_enabled(mention_config: SimpleNamespace) -> None:
    mention_config.chat.reply_timing.inevitable_at_reply = True
    runtime = MaisakaHeartFlowChatting.__new__(MaisakaHeartFlowChatting)
    runtime._forced_turn_enabled = False
    runtime._forced_turn_message_id = ""
    runtime._forced_turn_reason = ""
    runtime._idle_backoff = SimpleNamespace(reset=lambda: None)
    runtime.log_prefix = "[测试]"

    runtime._update_message_trigger_state(_message("@<麦麦:123456> 你好"))

    assert runtime._has_forced_turn_trigger() is True
