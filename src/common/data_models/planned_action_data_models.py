"""规划动作相关数据模型。

该模块定义 Planner 阶段产出的标准数据结构，用于描述：

1. 单条已经规划完成的动作。
2. 一轮规划共享的上下文信息。
3. 一轮完整规划的最终结果。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from . import BaseDataModel

if TYPE_CHECKING:
    from src.chat.message_receive.message import SessionMessage
    from src.core.types import ActionInfo


@dataclass
class PlannedAction(BaseDataModel):
    """单条已规划动作。

    该模型表示 Planner 在一次规划中选中的一条具体动作，以及该动作
    在后续执行阶段所需的目标消息、结构化参数和决策理由。

    Attributes:
        action_name: 动作名称。
        target_message: 该动作绑定的目标消息；如果动作不依赖具体消息，则为 ``None``。
        arguments: 动作的结构化参数字典。
        decision_reason: 选择该动作的直接理由，通常用于日志和调试展示。
    """

    action_name: str = field(
        default_factory=str,
        metadata={"description": "Planner 选中的动作名称，例如 reply、wait 或某个插件动作名。"},
    )
    target_message: Optional["SessionMessage"] = field(
        default=None,
        metadata={"description": "该动作绑定的目标消息；若该动作不依赖具体消息，则为空。"},
    )
    arguments: Dict[str, Any] = field(
        default_factory=dict,
        metadata={"description": "该动作的结构化参数字典，供执行阶段直接消费。"},
    )
    decision_reason: str = field(
        default_factory=str,
        metadata={"description": "选择该动作的直接理由，用于解释为什么执行这条动作。"},
    )


@dataclass
class PlanningContext(BaseDataModel):
    """一轮规划共享的上下文信息。

    该模型承载的是整轮规划级别的公共信息，而不是某一条动作私有的参数。
    多条 ``PlannedAction`` 可以共享同一个 ``PlanningContext``。

    Attributes:
        planner_reasoning: Planner 对整轮动作选择的总体推理说明。
        available_action_map: 本轮规划时可供选择的动作快照。
        cycle_started_at: 本轮规划循环的起始时间戳；为空表示未记录。
    """

    planner_reasoning: str = field(
        default_factory=str,
        metadata={"description": "Planner 对整轮规划给出的总体推理说明，而非某一条动作的局部理由。"},
    )
    available_action_map: Dict[str, "ActionInfo"] = field(
        default_factory=dict,
        metadata={"description": "本轮规划时可供选择的动作快照，键为动作名，值为对应的 ActionInfo。"},
    )
    cycle_started_at: Optional[float] = field(
        default=None,
        metadata={"description": "本轮规划循环开始时的时间戳；如果当前场景未记录，则为空。"},
    )


@dataclass
class PlanningResult(BaseDataModel):
    """一次完整规划的最终结果。

    该模型用于聚合 Planner 的最终输出，包含动作列表以及这轮规划共享的上下文。

    Attributes:
        actions: 本轮规划产出的动作列表，按 Planner 最终决定的顺序保存。
        context: 本轮规划共享的上下文信息。
    """

    actions: List["PlannedAction"] = field(
        default_factory=list,
        metadata={"description": "本轮规划产出的动作列表，列表中的每一项都是一条 PlannedAction。"},
    )
    context: PlanningContext = field(
        default_factory=PlanningContext,
        metadata={"description": "本轮规划共享的上下文信息。"},
    )


__all__ = ["PlannedAction", "PlanningContext", "PlanningResult"]
