"""query_jargon 内置工具。"""

import json
from typing import Any, Dict, List, Optional

from src.common.utils.utils_config import JargonConfigUtils
from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec
from src.learners.jargon_explainer import search_jargon

from .context import BuiltinToolRuntimeContext


def get_tool_spec() -> ToolSpec:
    """获取 query_jargon 工具声明。"""

    return ToolSpec(
        name="query_jargon",
        description="查询当前聊天上下文中的特殊的词条含义。当你认为某些词可能具有特殊含义，需要进行查询",
        parameters_schema={
            "type": "object",
            "properties": {
                "words": {
                    "type": "array",
                    "description": "要查询的词条列表。",
                    "items": {"type": "string"},
                },
            },
            "required": ["words"],
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 query_jargon 内置工具。"""

    del context
    raw_words = invocation.arguments.get("words")

    if not isinstance(raw_words, list):
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "查询黑话工具需要提供 `words` 数组参数。",
        )

    words = tool_ctx.normalize_words(raw_words)
    if not words:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "查询黑话工具至少需要一个非空词条。",
        )

    use_jargon, _ = JargonConfigUtils.get_jargon_config_for_chat(tool_ctx.runtime.session_id)
    if not use_jargon:
        return tool_ctx.build_failure_result(invocation.tool_name, "当前聊天流未启用黑话使用。")

    limit = 5
    case_sensitive = False
    enable_fuzzy_fallback = True
    before_search_result = await tool_ctx.get_runtime_manager().invoke_hook(
        "jargon.query.before_search",
        words=list(words),
        session_id=tool_ctx.runtime.session_id,
        limit=limit,
        case_sensitive=case_sensitive,
        enable_fuzzy_fallback=enable_fuzzy_fallback,
        abort_message="黑话查询已被 Hook 中止。",
    )
    if before_search_result.aborted:
        abort_message = str(before_search_result.kwargs.get("abort_message") or "黑话查询已被 Hook 中止。").strip()
        return tool_ctx.build_failure_result(invocation.tool_name, abort_message or "黑话查询已被 Hook 中止。")

    before_search_kwargs = before_search_result.kwargs
    if before_search_kwargs.get("words") is not None:
        words = tool_ctx.normalize_words(before_search_kwargs.get("words"))

    if not words:
        return tool_ctx.build_failure_result(invocation.tool_name, "Hook 过滤后没有可查询的黑话词条。")

    try:
        limit = int(before_search_kwargs.get("limit", limit))
    except (TypeError, ValueError):
        limit = 5
    limit = max(limit, 1)
    case_sensitive = bool(before_search_kwargs.get("case_sensitive", case_sensitive))
    enable_fuzzy_fallback = bool(before_search_kwargs.get("enable_fuzzy_fallback", enable_fuzzy_fallback))

    results: List[Dict[str, object]] = []
    for word in words:
        exact_matches = search_jargon(
            keyword=word,
            chat_id=tool_ctx.runtime.session_id,
            limit=limit,
            case_sensitive=case_sensitive,
            fuzzy=False,
        )
        matched_entries = exact_matches
        if not matched_entries and enable_fuzzy_fallback:
            matched_entries = search_jargon(
                keyword=word,
                chat_id=tool_ctx.runtime.session_id,
                limit=limit,
                case_sensitive=case_sensitive,
                fuzzy=True,
            )

        results.append(
            {
                "word": word,
                "found": bool(matched_entries),
                "matches": matched_entries,
            }
        )

    after_search_result = await tool_ctx.get_runtime_manager().invoke_hook(
        "jargon.query.after_search",
        words=list(words),
        session_id=tool_ctx.runtime.session_id,
        limit=limit,
        case_sensitive=case_sensitive,
        enable_fuzzy_fallback=enable_fuzzy_fallback,
        results=list(results),
        abort_message="黑话查询结果已被 Hook 中止。",
    )
    if after_search_result.aborted:
        abort_message = str(after_search_result.kwargs.get("abort_message") or "黑话查询结果已被 Hook 中止。").strip()
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            abort_message or "黑话查询结果已被 Hook 中止。",
        )

    raw_results = after_search_result.kwargs.get("results")
    if raw_results is not None:
        results = tool_ctx.normalize_jargon_query_results(raw_results)

    structured_content: Dict[str, Any] = {"results": results}
    return tool_ctx.build_success_result(
        invocation.tool_name,
        json.dumps(structured_content, ensure_ascii=False),
        structured_content=structured_content,
    )
