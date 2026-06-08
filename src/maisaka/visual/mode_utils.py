from src.common.logger import get_logger
from src.config.config import config_manager, global_config

logger = get_logger("maisaka_visual_mode")


def resolve_enable_visual_planner() -> bool:
    """根据 planner 配置解析当前是否应启用视觉消息。"""

    planner_mode = global_config.visual.planner_mode
    planner_task_config = config_manager.get_model_config().model_task_config.planner
    models_by_name = {model.name: model for model in config_manager.get_model_config().models}

    if planner_mode == "text":
        return False

    planner_models: list[str] = list(planner_task_config.model_list)
    missing_models = [model_name for model_name in planner_models if model_name not in models_by_name]
    non_visual_models = [
        model_name for model_name in planner_models if model_name in models_by_name and not models_by_name[model_name].visual
    ]

    if planner_mode == "multimodal":
        if missing_models:
            raise ValueError(
                "planner_mode=multimodal，但 planner 任务存在未定义的模型："
                f"{', '.join(missing_models)}"
            )
        if non_visual_models:
            raise ValueError(
                "planner_mode=multimodal，但 planner 任务存在未开启 visual 的模型："
                f"{', '.join(non_visual_models)}"
            )
        return True

    if missing_models:
        logger.warning(
            "planner_mode=auto 时发现 planner 任务存在未定义模型："
            f"{', '.join(missing_models)}，将退化为纯文本 planner"
        )
        return False

    return bool(planner_models) and not non_visual_models
