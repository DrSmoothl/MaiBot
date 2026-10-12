"""delegate 内置工具：把一个自包含的任务主动派发给子代理。

子代理在独立上下文里运行，看不到当前聊天，只拿到 Planner 写的任务说明和被授权的工具；
它可以自己多轮调用这些工具，最后只把结论作为工具结果交还给 Planner，中间过程不进入 Planner 上下文。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import json

from src.common.logger import get_logger
from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec
from src.llm_models.payload_content.context_item import FunctionCallItem
from src.llm_models.payload_content.tool_option import ToolCall
from src.maisaka.context.messages import LLMContextMessage, ReferenceMessage, ReferenceMessageType, ToolResultMessage

from .context import BuiltinToolRuntimeContext

logger = get_logger("maisaka_delegate")

DELEGATE_TOOL_NAME = "delegate"
# 子代理最多进行的模型请求轮数；用完后再给一次不带工具的收尾机会
DELEGATE_MAX_ROUNDS = 5
# 子代理上下文里单条工具结果的上限
DELEGATE_TOOL_RESULT_MAX_CHARS = 20000
# 子代理内部上下文按条数计的窗口，足够容纳全部轮次
DELEGATE_CONTEXT_MESSAGE_LIMIT = 60
# 这些工具负责发言、等待和切换聊天，只能由 Planner 自己调用；delegate 自身也不能再派发
DELEGATE_FORBIDDEN_TOOL_NAMES = frozenset(
    {
        DELEGATE_TOOL_NAME,
        "fetch_history",
        "react_message",
        "reply",
        "send_emoji",
        "show_emoji_list",
        "switch_chat",
        "tool_search",
        "wait",
    }
)

DELEGATE_SYSTEM_PROMPT = (
    "你是一个被派发任务的子代理。派发者看不到你的工作过程，只会收到你最后一条不带工具调用的文字。\n"
    "工作方式：\n"
    "1. 任务说明就是你知道的全部背景，不要假设任何没有写出来的信息。\n"
    "2. 用提供的工具查清楚需要的事实；互不依赖的调用可以一次并行发出。\n"
    "3. 工具返回无结果或失败时换一种查法，确实查不到就如实说明，不要编造。\n"
    "4. 查清后直接输出结论：先给答案，再列出支撑它的关键事实；派发者后续可能用到的 id、链接、路径、版本号原样保留。\n"
    "5. 你不能向任何人发言，也不能把任务再派发出去。结论控制在 800 字以内。"
)
DELEGATE_FINAL_ROUND_HINT = "工具调用次数已经用完。请根据已经拿到的结果直接给出结论，没有查清的部分如实说明。"


def get_tool_spec() -> ToolSpec:
    """获取 delegate 工具声明。"""

    return ToolSpec(
        name=DELEGATE_TOOL_NAME,
        description=(
            "把一个自包含的查询或整理任务交给子代理。子代理在独立上下文里工作，可以自己多次调用你授权的工具，"
            "最后只把结论返回给你，中间的原始结果不会进入你的上下文。"
            "适合需要多次查询、交叉比对，或会返回大量原始内容的任务；只需要一次简单查询时直接调用对应工具即可。"
            "子代理看不到当前聊天，也不能发言：发言、等待和群管理操作必须由你自己完成。"
        ),
        parameters_schema={
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "minLength": 1,
                    "description": (
                        "完整、自包含的任务说明。子代理看不到聊天记录，"
                        "要写清楚要查什么、相关的名称或关键词、以及你希望得到什么形式的结论。"
                    ),
                },
                "tools": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": (
                        "授权子代理使用的工具名列表，只填查询、检索、读取类工具；"
                        "可以包含 deferred tools 列表里的工具，不需要先 tool_search。"
                    ),
                },
            },
            "required": ["task", "tools"],
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


def _is_delegatable(tool_spec: ToolSpec) -> bool:
    """判断一个工具能否交给子代理使用。"""

    if not tool_spec.enabled or tool_spec.name in DELEGATE_FORBIDDEN_TOOL_NAMES:
        return False
    # 明确标注为破坏性的工具不交给看不到聊天上下文的子代理
    return not (tool_spec.annotation is not None and tool_spec.annotation.destructive is True)


def _build_tool_result_message(tool_call: ToolCall, result: ToolExecutionResult, logical_turn_id: str) -> ToolResultMessage:
    """把子代理内部的工具执行结果转成它自己上下文里的 tool result。"""

    content = result.get_history_content()[:DELEGATE_TOOL_RESULT_MAX_CHARS]
    if not content:
        content = "工具执行成功。" if result.success else f"工具 {tool_call.func_name} 执行失败。"
    return ToolResultMessage(
        content=content,
        timestamp=datetime.now(),
        tool_call_id=tool_call.call_id,
        logical_turn_id=logical_turn_id,
        tool_name=tool_call.func_name,
        success=result.success,
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 delegate：运行一个带工具的多轮子代理并返回它的结论。"""

    del context
    task = str(invocation.arguments.get("task") or "").strip()
    if not task:
        return tool_ctx.build_failure_result(invocation.tool_name, "delegate 需要提供非空的 task。")
    raw_tool_names = invocation.arguments.get("tools")
    if not isinstance(raw_tool_names, list) or not raw_tool_names:
        return tool_ctx.build_failure_result(invocation.tool_name, "delegate 需要在 tools 里至少授权一个工具。")
    requested_tool_names = [str(tool_name).strip() for tool_name in raw_tool_names if str(tool_name).strip()]

    runtime = tool_ctx.runtime
    engine = tool_ctx.engine
    tool_registry = runtime._tool_registry
    if tool_registry is None:
        return tool_ctx.build_failure_result(invocation.tool_name, "统一工具注册表尚未初始化。")

    available_specs = {
        tool_spec.name: tool_spec
        for tool_spec in await tool_registry.list_tools(engine._build_tool_availability_context())
    }
    granted_specs: List[ToolSpec] = []
    rejected_tool_names: List[str] = []
    for tool_name in dict.fromkeys(requested_tool_names):
        tool_spec = available_specs.get(tool_name)
        if tool_spec is None or not _is_delegatable(tool_spec):
            rejected_tool_names.append(tool_name)
            continue
        granted_specs.append(tool_spec)
    if not granted_specs:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            f"没有可以授权给子代理的工具：{'、'.join(rejected_tool_names)} 不存在，或属于只能由你自己调用的工具。",
        )

    granted_tool_names = {tool_spec.name for tool_spec in granted_specs}
    tool_definitions = [tool_spec.to_llm_definition() for tool_spec in granted_specs]
    sub_agent_history: List[LLMContextMessage] = [
        ReferenceMessage(
            content=task,
            timestamp=datetime.now(),
            reference_type=ReferenceMessageType.TOOL_HINT,
            remaining_uses_value=None,
            display_prefix="[派发给你的任务]",
        )
    ]
    execution_context = engine._build_tool_execution_context(task)
    tool_call_counts: Dict[str, int] = {}
    conclusion = ""

    for round_index in range(DELEGATE_MAX_ROUNDS + 1):
        is_final_round = round_index == DELEGATE_MAX_ROUNDS
        if is_final_round:
            sub_agent_history.append(
                ReferenceMessage(
                    content=DELEGATE_FINAL_ROUND_HINT,
                    timestamp=datetime.now(),
                    reference_type=ReferenceMessageType.TOOL_HINT,
                    remaining_uses_value=None,
                    display_prefix="[系统提示]",
                )
            )
        response = await runtime.run_sub_agent(
            context_message_limit=DELEGATE_CONTEXT_MESSAGE_LIMIT,
            system_prompt=DELEGATE_SYSTEM_PROMPT,
            extra_messages=sub_agent_history,
            tool_definitions=[] if is_final_round else tool_definitions,
            include_parent_context=False,
        )
        if not response.tool_calls or is_final_round:
            conclusion = (response.content or "").strip()
            break

        sub_agent_history.extend(response.raw_messages)
        logical_turn_ids = {
            message.output_item.tool_call.call_id: message.output_item.meta.logical_turn_id
            for message in response.raw_messages
            if isinstance(message.output_item, FunctionCallItem)
        }
        for tool_call in response.tool_calls:
            logical_turn_id = str(logical_turn_ids[tool_call.call_id] or "")
            if tool_call.func_name not in granted_tool_names:
                result = ToolExecutionResult(
                    tool_name=tool_call.func_name,
                    success=False,
                    error_message=f"工具 {tool_call.func_name} 没有授权给你。",
                )
            else:
                tool_call_counts[tool_call.func_name] = tool_call_counts.get(tool_call.func_name, 0) + 1
                result = await tool_registry.invoke(engine._build_tool_invocation(tool_call, task), execution_context)
                logger.info(
                    f"{runtime.log_prefix} 子代理调用工具 {tool_call.func_name} "
                    f"参数={json.dumps(tool_call.args or {}, ensure_ascii=False)[:200]} 成功={result.success}"
                )
            sub_agent_history.append(_build_tool_result_message(tool_call, result, logical_turn_id))

    usage_text = "、".join(f"{tool_name}×{count}" for tool_name, count in tool_call_counts.items()) or "未调用工具"
    logger.info(f"{runtime.log_prefix} 子代理任务结束 工具使用={usage_text} 结论={len(conclusion)}字")
    if not conclusion:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            f"子代理没有给出结论（工具使用：{usage_text}）。",
        )

    rejected_note = (
        f"\n（未授权的工具：{'、'.join(rejected_tool_names)}，它们不存在或只能由你自己调用）" if rejected_tool_names else ""
    )
    structured_content: Dict[str, Any] = {
        "tool_usage": tool_call_counts,
        "granted_tools": sorted(granted_tool_names),
        "rejected_tools": rejected_tool_names,
    }
    return tool_ctx.build_success_result(
        invocation.tool_name,
        f"[子代理结论｜工具使用：{usage_text}]\n{conclusion}{rejected_note}",
        structured_content=structured_content,
    )
