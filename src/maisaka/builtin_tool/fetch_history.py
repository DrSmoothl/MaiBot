"""fetch_history focus-mode builtin tool."""

from typing import Any, Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """Build the fetch_history tool spec."""

    return ToolSpec(
        name="fetch_history",
        description=(
            "获取当前聊天流中已经存在、但尚未进入 Maisaka 上下文的消息。"
            "按从新到旧最多返回 num 条；不能获取其他聊天的信息。"
        ),
        parameters_schema={
            "type": "object",
            "properties": {
                "num": {
                    "type": "integer",
                    "description": "获取消息数量；不填默认 10，最多 50。",
                    "minimum": 1,
                    "maximum": 50,
                },
            },
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


def _coerce_positive_int(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """Execute fetch_history."""

    del context
    if not tool_ctx.runtime._is_focus_mode_active_for_current_chat():
        return tool_ctx.build_failure_result(invocation.tool_name, "focus_mode 未启用，fetch_history 不可用。")

    num = min(50, _coerce_positive_int(invocation.arguments.get("num"), 10))
    content, structured_content, post_history_messages = await tool_ctx.runtime.build_focus_fetch_history_result(
        num=num,
    )
    return tool_ctx.build_success_result(
        invocation.tool_name,
        content,
        structured_content=structured_content,
        post_history_messages=post_history_messages,
    )
