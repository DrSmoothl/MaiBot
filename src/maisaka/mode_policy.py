"""Maisaka 新旧模式差异策略。"""

from typing import Literal

from src.config.config import global_config

BuiltinStage = Literal["timing", "action", "both"]
BuiltinVisibility = Literal["visible", "deferred", "hidden"]

LEGACY_NO_ACTION_REASONS = {"timing_no_action", "tool_pause:no_action"}
NEW_MAISAKA_NO_ACTION_REASONS = {*LEGACY_NO_ACTION_REASONS, "planner_no_tool_finish", "timing_wait", "tool_pause:wait"}


def is_new_maisaka_enabled() -> bool:
    """判断是否启用新 Maisaka。"""

    return bool(getattr(global_config.chat, "enable_new_maisaka", False))


def is_reply_necessity_trigger_enabled() -> bool:
    """判断是否启用新 Maisaka 的回复必要性触发门。"""

    return bool(getattr(global_config.chat, "enable_reply_necessity_trigger", False))


def should_run_timing_gate(*, planner_continuation_active: bool) -> bool:
    """判断本轮是否需要运行 Timing Gate。"""

    return (not is_new_maisaka_enabled()) and (not planner_continuation_active)


def effective_builtin_stage(name: str, default_stage: BuiltinStage) -> BuiltinStage:
    """返回内置工具在当前模式下的阶段。"""

    if is_new_maisaka_enabled():
        return "timing" if name == "no_action" else default_stage
    return "timing" if name == "wait" else default_stage


def effective_builtin_visibility(name: str, default_visibility: BuiltinVisibility) -> BuiltinVisibility:
    """返回内置工具在当前模式下的可见性。"""

    return "hidden" if name in {"finish", "no_action"} and is_new_maisaka_enabled() else default_visibility


def timing_gate_tool_names() -> set[str]:
    """返回当前模式保留给 Timing Gate 的工具名。"""

    if is_new_maisaka_enabled():
        return {"continue", "wait"}
    return {"continue", "no_action", "wait"}


def planner_filtered_timing_tool_names() -> set[str]:
    """返回 Planner 历史中要过滤的 Timing Gate 工具名。"""

    if is_new_maisaka_enabled():
        return {"continue"}
    return {"continue", "wait"}


def is_no_action_equivalent_cycle_reason(cycle_end_reason: str) -> bool:
    """判断整轮结束原因是否等价于 no_action。"""

    reasons = NEW_MAISAKA_NO_ACTION_REASONS if is_new_maisaka_enabled() else LEGACY_NO_ACTION_REASONS
    return str(cycle_end_reason).strip() in reasons
