from typing import Any, Dict, List

from src.llm_models.payload_content.message import Message, MessageBuilder, RoleType

LOCAL_MAI_REPLYER_SYSTEM_PROMPT = "请根据给你的思考内容生成一条回复：只输出最终要发送的实际发言内容。"


def resolve_local_mai_replyer_input(reply_reason: str, reply_tool_args: Dict[str, Any] | None = None) -> str:
    """解析本地麦麦 replyer 的 user message 输入。"""

    latest_reason = reply_reason.strip()
    if latest_reason:
        return latest_reason

    reply_guide = str((reply_tool_args or {}).get("reply_guide") or "").strip()
    if reply_guide:
        return reply_guide

    raise ValueError("本地麦麦 Replyer 需要来自 Planner 的最新推理或 reply_guide，当前均为空")


def build_local_mai_replyer_messages(
    reply_reason: str,
    reply_tool_args: Dict[str, Any] | None = None,
) -> List[Message]:
    """构建本地麦麦 replyer 的独立请求消息。"""

    local_replyer_input = resolve_local_mai_replyer_input(reply_reason, reply_tool_args)

    return [
        MessageBuilder().set_role(RoleType.System).add_text_content(LOCAL_MAI_REPLYER_SYSTEM_PROMPT).build(),
        MessageBuilder().set_role(RoleType.User).add_text_content(local_replyer_input).build(),
    ]
