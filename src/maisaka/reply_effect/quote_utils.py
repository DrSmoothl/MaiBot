"""回复效果记录中的引用消息辅助工具。"""

from src.common.data_models.message_component_data_model import MessageSequence, ReplyComponent


def extract_quote_target_ids(message_sequence: MessageSequence | None) -> list[str]:
    """从消息片段中提取引用回复目标消息 ID。"""

    if message_sequence is None:
        return []

    target_ids: list[str] = []
    for component in message_sequence.components:
        if not isinstance(component, ReplyComponent):
            continue
        target_message_id = str(component.target_message_id or "").strip()
        if target_message_id:
            target_ids.append(target_message_id)
    return target_ids
