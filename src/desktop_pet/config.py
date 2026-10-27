"""内置桌宠配置。"""

from src.config.config_base import ConfigBase, Field


class DesktopPetConfig(ConfigBase):
    __ui_parent__ = "experimental"
    __ui_label__ = "桌宠"

    enabled: bool = Field(default=False, json_schema_extra={"label": {"zh_CN": "启用桌宠"}})
    """在本机 Windows 桌面显示麦麦，和网页聊天室共用聊天与昵称；支持热重载。"""
