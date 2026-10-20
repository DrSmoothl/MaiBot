"""电脑能力的配置模板，不设置探索配额或应用白名单。"""

from typing import Dict, List, Literal

from src.config.config_base import ConfigBase, Field


class ComputerConfig(ConfigBase):
    """电脑使用"""

    __ui_label__ = "电脑使用"

    enabled: bool = Field(default=False, json_schema_extra={"label": {"zh_CN": "启用电脑使用"}})
    """启用独立电脑 Agent 与自主探索；内置后端使用电脑模型（留空用 Planner），ACP 使用外部 Agent 配置的模型。"""

    backend: Literal["builtin", "acp"] = Field(default="builtin", json_schema_extra={
        "label": {"zh_CN": "Agent 后端"}, "advanced": True, "x-widget": "select",
        "options": ["builtin", "acp"],
    })
    """builtin 使用内置 Agent；acp 通过标准输入输出驱动 dsh 等外部 Agent。"""

    acp_command: str = Field(default="dsh", json_schema_extra={"label": {"zh_CN": "ACP 启动程序"}, "advanced": True})
    """外部 Agent 可执行程序或绝对路径，不是整段 shell 命令；Windows 可填写 dsh.cmd。"""

    acp_args: List[str] = Field(default_factory=lambda: ["--profile", "acp"], json_schema_extra={
        "label": {"zh_CN": "ACP 启动参数"}, "advanced": True,
    })
    """逐项填写启动参数；dsh 使用 --profile acp。"""

    acp_cwd: str = Field(default="", json_schema_extra={"label": {"zh_CN": "ACP 主机工作目录"}, "advanced": True})
    """外部进程的主机工作目录，留空使用 data/computer/acp/workspace；不是沙盒 C:\\MaiWorkspace。同一会话恢复时保持不变。"""

    acp_env: Dict[str, str] = Field(default_factory=dict, json_schema_extra={"label": {"zh_CN": "ACP 环境变量"}, "advanced": True})
    """追加或覆盖外部进程环境变量，例如 DSH_HOME；模型凭据建议保存在 dsh 自身配置中。"""

    acp_model: str = Field(default="", json_schema_extra={"label": {"zh_CN": "ACP 模型选项"}, "advanced": True})
    """留空使用外部 Agent 自身模型配置；填写时必须是其 model 配置选项公布的值，不使用本体 planner 回退。"""

    acp_timeout: float = Field(default=60, gt=0, json_schema_extra={"label": {"zh_CN": "ACP 连接与关停超时（秒）"}, "advanced": True})
    """握手、会话恢复和取消确认的等待时间，不限制任务执行时长。"""

    context_multiplier: float = Field(default=3, ge=1, json_schema_extra={"label": {"zh_CN": "上下文容量倍数"}})
    """内置 Agent 上下文相对普通群聊的倍数，超过窗口时整理连续性笔记；ACP 的上下文由外部 Agent 管理。"""

    networking: bool = Field(default=True, json_schema_extra={"label": {"zh_CN": "允许沙盒联网"}})
    """允许沙盒联网；修改后在下一次启动沙盒时生效。"""

    memory_mb: int = Field(default=4096, ge=2048, json_schema_extra={"label": {"zh_CN": "沙盒内存（MB）"}})
    """沙盒内存 MB，下次启动沙盒时生效。"""

    startup_timeout: float = Field(default=120, gt=0, json_schema_extra={"label": {"zh_CN": "启动超时（秒）"}})
    """等待沙盒启动的超时秒数。"""

    request_timeout: float = Field(default=60, gt=0, json_schema_extra={"label": {"zh_CN": "操作等待超时（秒）"}})
    """等待代理回复的超时秒数；超时不重放操作，可继续领取原请求结果。"""

    screenshot_max_edge: int = Field(default=1600, ge=0, json_schema_extra={"label": {"zh_CN": "截图最长边（像素）"}})
    """截图最长边，0 表示原始分辨率；鼠标坐标始终按返回截图换算。"""

    idle_seconds: float = Field(default=30, gt=0, json_schema_extra={"label": {"zh_CN": "自主探索间隔（秒）"}})
    """电脑 Agent空闲后再次自主思考的间隔；麦麦也可通过 computer_rest 自行决定休息多久。"""

    interests: str = Field(
        default="自由探索电脑、上网、画画、学习软件、玩游戏，和其他聊天交流见闻。",
        json_schema_extra={"label": {"zh_CN": "探索兴趣与活动指导"}},
    )
    """电脑 Agent的兴趣与活动指导，不限制其操作范围。"""
