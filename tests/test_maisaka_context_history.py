from datetime import datetime
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.llm_models.payload_content.tool_option import ToolCall
from src.maisaka.context.history import normalize_tool_call_result_pairs
from src.maisaka.context.messages import AssistantMessage, ToolResultMessage


def test_normalize_tool_call_result_pairs_removes_no_action_without_tool_result() -> None:
    history = [
        AssistantMessage(
            content="",
            timestamp=datetime.now(),
            tool_calls=[ToolCall(call_id="no_action:0", func_name="no_action", args={})],
        )
    ]

    filtered_history, stats = normalize_tool_call_result_pairs(history)

    assert filtered_history == []
    assert stats["unanswered_tool_calls"] == 1


def test_normalize_tool_call_result_pairs_keeps_answered_tool_call() -> None:
    assistant_message = AssistantMessage(
        content="",
        timestamp=datetime.now(),
        tool_calls=[ToolCall(call_id="reply:0", func_name="reply", args={"msg_id": "m1"})],
    )
    tool_result_message = ToolResultMessage(
        content="工具执行成功。",
        timestamp=datetime.now(),
        tool_call_id="reply:0",
        tool_name="reply",
    )

    filtered_history, stats = normalize_tool_call_result_pairs([assistant_message, tool_result_message])

    assert filtered_history == [assistant_message, tool_result_message]
    assert stats["unanswered_tool_calls"] == 0


def test_normalize_tool_call_result_pairs_keeps_only_answered_tool_calls() -> None:
    assistant_message = AssistantMessage(
        content="",
        timestamp=datetime.now(),
        tool_calls=[
            ToolCall(call_id="reply:0", func_name="reply", args={"msg_id": "m1"}),
            ToolCall(call_id="no_action:0", func_name="no_action", args={}),
        ],
    )
    tool_result_message = ToolResultMessage(
        content="工具执行成功。",
        timestamp=datetime.now(),
        tool_call_id="reply:0",
        tool_name="reply",
    )

    filtered_history, stats = normalize_tool_call_result_pairs([assistant_message, tool_result_message])

    assert len(filtered_history) == 2
    assert isinstance(filtered_history[0], AssistantMessage)
    assert [tool_call.call_id for tool_call in filtered_history[0].tool_calls] == ["reply:0"]
    assert filtered_history[1] == tool_result_message
    assert stats["unanswered_tool_calls"] == 1
