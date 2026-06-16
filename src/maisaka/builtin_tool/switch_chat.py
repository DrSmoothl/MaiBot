"""switch_chat focus-mode builtin tool."""

from typing import Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """Build the switch_chat tool spec."""

    return ToolSpec(
        name="switch_chat",
        description=(
            "切换到另一个运行中已创建且未关注的聊天。切换会把当前上下文复制并覆盖到目标聊天，"
            "并把目标聊天当前未读新消息按普通 user message 格式接入上下文；未读新消息超过 20 条时只接入最新 20 条，"
        ),
        parameters_schema={
            "type": "object",
            "properties": {
                "chat_id": {
                    "type": "string",
                    "description": "目标聊天的真实 chat_id；与 platform/id/type 组合二选一。",
                },
                "platform": {
                    "type": "string",
                    "description": "目标聊天平台；与 id、type 一起使用。",
                },
                "id": {
                    "type": "string",
                    "description": "目标 ID。type=group 时为群 ID，type=private 时为用户 ID。",
                },
                "type": {
                    "type": "string",
                    "enum": ["group", "private"],
                    "description": "目标聊天类型。",
                },
            },
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """Execute switch_chat."""

    del context
    if not tool_ctx.runtime._is_focus_mode_active_for_current_chat():
        return tool_ctx.build_failure_result(invocation.tool_name, "focus_mode 未启用，switch_chat 不可用。")

    resolution = tool_ctx.runtime.resolve_running_focus_session_from_args(invocation.arguments)
    if resolution.session is None:
        return tool_ctx.build_failure_result(invocation.tool_name, resolution.error)

    success, content, structured_content, metadata = await tool_ctx.runtime.switch_focus_to_session(
        resolution.session,
        tool_call_id=invocation.call_id,
        tool_name=invocation.tool_name,
    )
    if not success:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            content,
            structured_content=structured_content,
            metadata=metadata,
        )
    return tool_ctx.build_success_result(
        invocation.tool_name,
        content,
        structured_content=structured_content,
        metadata=metadata,
    )
