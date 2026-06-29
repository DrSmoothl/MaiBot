from datetime import datetime, timedelta

from src.maisaka.chat_loop_service import MaisakaChatLoopService
from src.maisaka.context.messages import AssistantMessage, ReferenceMessage, ReferenceMessageType
from src.maisaka.runtime import MaisakaHeartFlowChatting


def test_context_restore_reference_marks_short_restart() -> None:
    now = datetime(2026, 6, 29, 10, 0, 0)
    restored_history = [
        AssistantMessage(
            content="刚才聊到一半",
            timestamp=now - timedelta(minutes=2),
        )
    ]

    reference = MaisakaHeartFlowChatting._build_context_restore_reference_message(restored_history, now=now)

    assert reference is not None
    assert reference.reference_type == ReferenceMessageType.CONTEXT_RESTORE
    assert reference.remaining_uses_value is None
    assert reference.display_prefix == "[上下文恢复]"
    assert "距离上次关机前最后一条可恢复聊天记录已经过去 2 分钟" in reference.content
    assert "短暂重启" in reference.content
    assert "前面恢复出来的历史消息" in reference.content
    assert "不代表当前用户刚刚发来新消息" in reference.content


def test_context_restore_reference_marks_long_sleep() -> None:
    now = datetime(2026, 6, 29, 10, 0, 0)
    restored_history = [
        AssistantMessage(
            content="昨天的聊天",
            timestamp=now - timedelta(hours=7, minutes=30),
        )
    ]

    reference = MaisakaHeartFlowChatting._build_context_restore_reference_message(restored_history, now=now)

    assert reference is not None
    assert "7 小时 30 分钟" in reference.content
    assert "沉睡" in reference.content
    assert "仍记得上次关机前的聊天内容" in reference.content


def test_context_restore_reference_stays_selected_after_window_moves_on() -> None:
    now = datetime(2026, 6, 29, 10, 0, 0)
    reference = ReferenceMessage(
        content="启动恢复提醒",
        timestamp=now - timedelta(hours=8),
        reference_type=ReferenceMessageType.CONTEXT_RESTORE,
        remaining_uses_value=None,
        display_prefix="[上下文恢复]",
    )
    chat_history = [
        reference,
        AssistantMessage(content="旧消息 1", timestamp=now - timedelta(minutes=4)),
        AssistantMessage(content="旧消息 2", timestamp=now - timedelta(minutes=3)),
        AssistantMessage(content="新消息 1", timestamp=now - timedelta(minutes=2)),
        AssistantMessage(content="新消息 2", timestamp=now - timedelta(minutes=1)),
    ]

    selected_history, _ = MaisakaChatLoopService.select_llm_context_messages(
        chat_history,
        enable_visual_message=False,
        max_context_size=1,
    )

    assert reference in selected_history
