"""Maisaka 展示辅助工具。"""

from typing import Any

from src.llm_models.payload_content.tool_option import (
    TOOL_CALL_SOURCE_EXTRA_KEY,
    TOOL_CALL_SOURCE_REASONING,
    TOOL_CALL_SOURCE_RESPONSE,
)


_REQUEST_PANEL_STYLE_MAP: dict[str, tuple[str, str]] = {
    "planner": ("MaiSaka 大模型请求 - 对话单步", "green"),
    "timing_gate": ("MaiSaka 大模型请求 - Timing Gate 子代理", "bright_magenta"),
    "replyer": ("MaiSaka 回复器 Prompt", "bright_yellow"),
    "emotion": ("MaiSaka Emotion Tool Prompt", "bright_cyan"),
    "expression_selector": ("MaiSaka 表达选择子代理 Prompt", "bright_yellow"),
    "expression_learner": ("MaiSaka 表达学习上下文", "bright_green"),
    "jargon_learner": ("MaiSaka 黑话学习上下文", "bright_cyan"),
    "reply_effect_judge": ("MaiSaka 回复效果评分器 Prompt", "bright_red"),
    "sub_agent": ("MaiSaka 大模型请求 - 子代理", "bright_blue"),
}

_DEFAULT_REQUEST_PANEL_STYLE: tuple[str, str] = (
    "MaiSaka 大模型请求 - 对话单步",
    "cyan",
)

def format_token_count(token_count: int) -> str:
    """格式化 token 数量展示文本。"""

    if token_count >= 10_000:
        return f"{token_count / 1000:.1f}k"
    return str(token_count)


def get_request_panel_style(request_kind: str) -> tuple[str, str]:
    """返回不同请求类型对应的标题与边框颜色。"""

    normalized_kind = str(request_kind or "planner").strip().lower()
    return _REQUEST_PANEL_STYLE_MAP.get(normalized_kind, _DEFAULT_REQUEST_PANEL_STYLE)


def format_tool_call_for_display(tool_call: Any) -> dict[str, Any]:
    """将不同来源的工具调用对象规范化为统一展示结构。"""

    if isinstance(tool_call, dict):
        function_info = tool_call.get("function", {})
        extra_content = tool_call.get("extra_content")
        source = _normalize_tool_call_source(
            tool_call.get(TOOL_CALL_SOURCE_EXTRA_KEY)
            or tool_call.get("source")
            or (extra_content.get(TOOL_CALL_SOURCE_EXTRA_KEY) if isinstance(extra_content, dict) else "")
        )
        payload = {
            "id": tool_call.get("id"),
            "name": function_info.get("name", tool_call.get("name")),
            "arguments": function_info.get("arguments", tool_call.get("arguments")),
        }
        if source:
            payload["source"] = source
            payload["source_label"] = format_tool_call_source_label(source)
        if isinstance(extra_content, dict) and extra_content:
            payload["extra_content"] = extra_content
        return payload

    extra_content = getattr(tool_call, "extra_content", None)
    source = _normalize_tool_call_source(
        extra_content.get(TOOL_CALL_SOURCE_EXTRA_KEY) if isinstance(extra_content, dict) else ""
    )
    return {
        "id": getattr(tool_call, "call_id", getattr(tool_call, "id", None)),
        "name": getattr(tool_call, "func_name", getattr(tool_call, "name", None)),
        "arguments": getattr(tool_call, "args", getattr(tool_call, "arguments", None)),
        **({"source": source, "source_label": format_tool_call_source_label(source)} if source else {}),
        **({"extra_content": extra_content} if isinstance(extra_content, dict) and extra_content else {}),
    }


def _normalize_tool_call_source(source: Any) -> str:
    normalized_source = str(source or "").strip().lower()
    if normalized_source in {TOOL_CALL_SOURCE_REASONING, "thinking", "reasoning_content"}:
        return TOOL_CALL_SOURCE_REASONING
    if normalized_source in {TOOL_CALL_SOURCE_RESPONSE, "content", "output", "text"}:
        return TOOL_CALL_SOURCE_RESPONSE
    return ""


def format_tool_call_source_label(source: str) -> str:
    """返回工具调用来源的中文展示标签。"""

    normalized_source = _normalize_tool_call_source(source)
    if normalized_source == TOOL_CALL_SOURCE_REASONING:
        return "推理中调用"
    if normalized_source == TOOL_CALL_SOURCE_RESPONSE:
        return "正文调用"
    return "未知来源"


def build_tool_call_summary_lines(tool_calls: list[Any]) -> list[str]:
    """构建工具调用摘要文本。"""

    summary_lines: list[str] = []
    for tool_call in tool_calls:
        normalized_tool_call = format_tool_call_for_display(tool_call)
        tool_name = str(normalized_tool_call.get("name") or "").strip() or "unknown"
        source_label = str(normalized_tool_call.get("source_label") or "").strip()
        source_suffix = f" [{source_label}]" if source_label else ""
        tool_args = normalized_tool_call.get("arguments")
        if isinstance(tool_args, dict) and tool_args:
            summary_lines.append(f"- {tool_name}{source_suffix}: {tool_args}")
        else:
            summary_lines.append(f"- {tool_name}{source_suffix}")
    return summary_lines
