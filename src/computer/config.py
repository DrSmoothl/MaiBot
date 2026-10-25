"""共享沙盒电脑配置，与外接 Agent 连接独立。"""

from src.config.config_base import ConfigBase, Field


class ComputerConfig(ConfigBase):
    __ui_parent__ = "experimental"
    __ui_label__ = "沙盒电脑"

    enabled: bool = Field(default=False, json_schema_extra={"label": {"zh_CN": "启用沙盒电脑"}})
    """向内部 Planner、子 Agent 和外接 Agent 提供共享沙盒电脑及 WebUI 接管界面；无需启用外接 Agent，人工接管期间拒绝所有自主输入。"""

    networking: bool = Field(default=True, json_schema_extra={"label": {"zh_CN": "允许沙盒联网"}})
    """允许沙盒联网；修改后在下一次启动沙盒时生效。"""

    memory_mb: int = Field(default=4096, ge=2048, json_schema_extra={"label": {"zh_CN": "沙盒内存（MB）"}, "advanced": True})
    """沙盒内存 MB，下次启动沙盒时生效。"""

    startup_timeout: float = Field(default=120, gt=0, json_schema_extra={"label": {"zh_CN": "启动超时（秒）"}, "advanced": True})
    """等待沙盒启动的超时秒数。"""

    request_timeout: float = Field(default=60, gt=0, json_schema_extra={"label": {"zh_CN": "操作等待超时（秒）"}, "advanced": True})
    """等待代理回复的超时秒数；超时不重放操作，可继续领取原请求结果。"""

    screenshot_max_edge: int = Field(default=1600, ge=0, json_schema_extra={"label": {"zh_CN": "截图最长边（像素）"}, "advanced": True})
    """截图最长边，0 表示原始分辨率；鼠标坐标始终按返回截图换算。"""

