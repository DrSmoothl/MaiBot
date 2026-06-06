from typing import Any, Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec
from src.learners.behavior_pattern_store import (
    BEHAVIOR_REFERENCE_SOURCE,
    apply_behavior_feedback,
    behavior_pattern_to_dict,
)
from src.maisaka.context.messages import ReferenceMessage

from .context import BuiltinToolRuntimeContext

ALLOWED_FEEDBACK_STATUSES = {"success", "failed", "blocked", "abandoned", "skipped"}
MIN_FEEDBACK_SCORE = -2.0
MAX_FEEDBACK_SCORE = 2.0


def get_tool_spec() -> ToolSpec:
    """获取 behavior_feedback 工具声明。"""

    return ToolSpec(
        name="behavior_feedback",
        description=(
            "为本轮采纳、尝试、放弃或无法继续的行为表现参考记录反馈。"
            "只有当上下文中出现 behavior_pattern_reference，并且你已经明确知道执行结果时才调用。"
        ),
        parameters_schema={
            "type": "object",
            "properties": {
                "behavior_id": {
                    "type": "integer",
                    "description": "behavior_pattern_reference 中的行为表现 ID。",
                },
                "score": {
                    "type": "number",
                    "description": "反馈分数，范围 -2 到 2。成功、自然、推进对话时给正分；失败、生硬、受阻时给负分。",
                    "minimum": MIN_FEEDBACK_SCORE,
                    "maximum": MAX_FEEDBACK_SCORE,
                },
                "status": {
                    "type": "string",
                    "description": "行为表现当前状态。",
                    "enum": ["success", "failed", "blocked", "abandoned", "skipped"],
                },
                "reason": {
                    "type": "string",
                    "description": "简短说明为什么这样打分。",
                },
                "outcome": {
                    "type": "string",
                    "description": "可选：本次真实结果，若比原预期更准确可填写。",
                },
            },
            "required": ["behavior_id", "score", "status", "reason"],
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


def _coerce_behavior_id(raw_value: Any) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return 0


def _coerce_score(raw_value: Any) -> float:
    try:
        score = float(raw_value)
    except (TypeError, ValueError):
        score = 0.0
    return min(MAX_FEEDBACK_SCORE, max(MIN_FEEDBACK_SCORE, score))


def _clear_behavior_reference_messages(tool_ctx: BuiltinToolRuntimeContext, behavior_id: int) -> int:
    marker = f'id="{behavior_id}"'
    behavior_id_marker = f"behavior_id={behavior_id}"
    removed_count = 0
    retained_history = []
    for message in tool_ctx.runtime._chat_history:
        if (
            isinstance(message, ReferenceMessage)
            and message.source == BEHAVIOR_REFERENCE_SOURCE
            and (marker in message.content or behavior_id_marker in message.content)
        ):
            removed_count += 1
            continue
        retained_history.append(message)

    if removed_count:
        tool_ctx.runtime._chat_history = retained_history
    return removed_count


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 behavior_feedback 内置工具。"""

    del context
    behavior_id = _coerce_behavior_id(invocation.arguments.get("behavior_id"))
    score_delta = _coerce_score(invocation.arguments.get("score"))
    status = str(invocation.arguments.get("status") or "").strip().lower()
    reason = str(invocation.arguments.get("reason") or "").strip()
    outcome = str(invocation.arguments.get("outcome") or "").strip()

    if behavior_id <= 0:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "behavior_feedback 需要提供有效的 behavior_id。",
        )
    if status not in ALLOWED_FEEDBACK_STATUSES:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            f"behavior_feedback 的 status 无效：{status or '空'}。",
        )
    if not reason:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "behavior_feedback 需要提供简短的 reason。",
        )

    pattern = apply_behavior_feedback(
        pattern_id=behavior_id,
        score_delta=score_delta,
        status=status,
        reason=reason,
        outcome=outcome,
        session_id=tool_ctx.runtime.session_id,
    )
    if pattern is None:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            f"未找到行为表现记录，behavior_id={behavior_id}。",
        )

    removed_count = _clear_behavior_reference_messages(tool_ctx, behavior_id)
    structured_content = {
        "behavior": behavior_pattern_to_dict(pattern),
        "score_delta": score_delta,
        "status": status,
        "reason": reason,
        "outcome": outcome,
        "cleared_reference_count": removed_count,
    }
    display_prompt = f"行为表现反馈已记录：id={behavior_id} status={status} score={score_delta:+.1f}"
    return tool_ctx.build_success_result(
        invocation.tool_name,
        display_prompt,
        structured_content=structured_content,
        metadata={"record_display_prompt": display_prompt},
    )
