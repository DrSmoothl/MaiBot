from datetime import datetime

from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.maisaka.context.messages import (
    AssistantMessage,
    ReferenceMessage,
    ReferenceMessageType,
    SessionBackedMessage,
    ToolResultMessage,
)
from src.maisaka.reasoning_engine import MaisakaReasoningEngine


def _session_message(text: str, source_kind: str) -> SessionBackedMessage:
    return SessionBackedMessage(
        raw_message=MessageSequence([TextComponent(text)]),
        visible_text=text,
        timestamp=datetime.now(),
        source_kind=source_kind,
    )


def test_behavior_scenario_context_filter_keeps_only_chat_messages() -> None:
    user_message = _session_message("用户消息", "user")
    guided_reply = _session_message("麦麦已发送消息", "guided_reply")
    outbound_send = _session_message("麦麦外发消息", "outbound_send")
    perception = _session_message("识图占位", "perception")
    reference = ReferenceMessage(
        content="表达习惯参考",
        timestamp=datetime.now(),
        reference_type=ReferenceMessageType.CUSTOM,
    )
    assistant = AssistantMessage(content="内部 assistant 消息", timestamp=datetime.now())
    tool_result = ToolResultMessage(
        content="工具结果",
        timestamp=datetime.now(),
        tool_call_id="call_1",
        tool_name="fetch_context",
    )

    filtered_messages = MaisakaReasoningEngine._filter_behavior_scenario_context_messages(
        [reference, user_message, assistant, tool_result, guided_reply, perception, outbound_send]
    )

    assert filtered_messages == [user_message, guided_reply, outbound_send]
