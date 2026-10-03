from src.common.logger import get_logger
from src.config.config import config_manager

logger = get_logger("maisaka_visual_mode")


def _normalize_model_names(model_names: list[str]) -> list[str]:
    """过滤空模型名，保持与模型任务实际回退逻辑一致。"""

    return [model_name.strip() for model_name in model_names if model_name.strip()]


def _resolve_enable_visual_task(task_name: str, fallback_task_name: str = "") -> bool:
    """根据指定任务模型的视觉能力，自动决定是否启用视觉消息。"""

    model_config = config_manager.get_model_config()
    model_task_config = model_config.model_task_config
    task_config = getattr(model_task_config, task_name)
    models_by_name = {model.name: model for model in model_config.models}

    task_models = _normalize_model_names(list(task_config.model_list))
    resolved_task_name = task_name
    if not task_models and fallback_task_name:
        fallback_task_config = getattr(model_task_config, fallback_task_name)
        task_models = _normalize_model_names(list(fallback_task_config.model_list))
        resolved_task_name = fallback_task_name

    task_label = f"{task_name} 任务"
    if resolved_task_name != task_name:
        task_label = f"{task_name} 任务继用的 {resolved_task_name} 任务"

    missing_models = [model_name for model_name in task_models if model_name not in models_by_name]
    non_visual_models = [
        model_name for model_name in task_models if model_name in models_by_name and not models_by_name[model_name].visual
    ]

    if missing_models:
        logger.warning(
            f"自动选择视觉模式时发现 {task_label}存在未定义模型："
            f"{', '.join(missing_models)}，将退化为纯文本 planner"
        )
        return False

    return bool(task_models) and not non_visual_models


def resolve_enable_visual_planner() -> bool:
    """根据 planner 配置解析当前是否应启用视觉消息。"""

    return _resolve_enable_visual_task("planner")
