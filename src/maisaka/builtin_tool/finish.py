"""finish 内置工具。"""

from typing import Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """获取 finish 工具声明。"""

    return ToolSpec(
        name="finish",
        description="结束连续思考，不再使用工具，进入等待观望",
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 finish 内置工具。"""

    del context
    tool_ctx.runtime._enter_stop_state()
    return tool_ctx.build_success_result(
        invocation.tool_name,
        "当前对话循环已结束本轮思考，等待新的消息到来。",
        metadata={"pause_execution": True},
    )
