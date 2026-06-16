"""continue 内置工具。"""

from typing import Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """获取 continue 工具声明。"""

    return ToolSpec(
        name="continue",
        description="进入下一轮思考和工具执行。",
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 continue 内置工具。"""

    del tool_ctx, context
    return ToolExecutionResult(
        tool_name=invocation.tool_name,
        success=True,
        content="当前对话继续进入下一轮思考和工具执行。",
        metadata={
            "timing_action": "continue",
        },
    )
