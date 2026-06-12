"""no_action 内置工具。"""

from typing import Optional

from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """获取 no_action 工具声明。"""

    return ToolSpec(
        name="no_action",
        description=(
            "本轮暂时不进行任何动作，等待新的外部消息；在 Planner 中使用时会保留连续 Planner 状态，"
            "下一条消息直接继续交给 Planner，只有 finish 会结束连续 Planner。"
        ),
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 no_action 内置工具。"""

    del context
    tool_ctx.runtime._enter_stop_state()
    return tool_ctx.build_success_result(
        invocation.tool_name,
        "当前暂时停止思考，等待新消息到来。",
        metadata={"pause_execution": True},
    )
