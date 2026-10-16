"""插件信息源和单个下载源设置；网络错误直接暴露，不自动换源。"""

from os import getenv
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Literal, Optional
from urllib.parse import quote
import asyncio
import json
import os
import tempfile

from pydantic import BaseModel, model_validator
import httpx

from src.webui.utils.http_client import get_shared_ssl_context

SETTINGS_FILE = Path("data/plugin_market.json")
MARKET_BASE_URL = getenv("MAIBOT_PLUGIN_MARKET_BASE_URL", "http://hyybuth.xyz:10059").rstrip("/")
_settings_lock = RLock()


class PluginSourceSettings(BaseModel):
    information_source: Literal["official", "cnb", "github"] = "official"
    download_source: Literal["cnb", "github", "custom"] = "cnb"
    custom_download_prefix: Optional[str] = None

    @model_validator(mode="after")
    def validate_custom_source(self) -> "PluginSourceSettings":
        if self.download_source == "custom" and not self.custom_download_prefix:
            raise ValueError("自定义下载源必须填写 Git 克隆前缀")
        return self


def load_source_settings() -> PluginSourceSettings:
    with _settings_lock:
        if not SETTINGS_FILE.exists():
            return PluginSourceSettings()
        data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        # 旧开关只表达是否使用 Git 索引，开启时迁移为固定的 CNB 信息源。
        if "use_github" in data:
            if not isinstance(data["use_github"], bool):
                raise ValueError("插件市场 use_github 设置必须为布尔值")
            data = {"information_source": "cnb" if data["use_github"] else "official"}
        return PluginSourceSettings.model_validate(data)


def use_github_market_data() -> bool:
    return load_source_settings().information_source != "official"


def save_source_settings(settings: PluginSourceSettings) -> None:
    from src.webui.utils.network_security import validate_public_url

    if settings.custom_download_prefix:
        settings = settings.model_copy(update={
            "custom_download_prefix": validate_public_url(settings.custom_download_prefix, allow_fake_ip=True).rstrip("/")
        })
    with _settings_lock:
        SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(dir=SETTINGS_FILE.parent, prefix=".plugin-market-")
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(settings.model_dump(), stream)
            os.replace(name, SETTINGS_FILE)
        finally:
            if os.path.exists(name):
                os.unlink(name)


def download_mirror() -> Dict[str, Any]:
    """下载只选择一个源，与市场信息源及主程序更新镜像配置无关。"""
    from .git_mirror_service import GitMirrorConfig

    settings = load_source_settings()
    if settings.download_source == "custom":
        return {"id": "custom", "name": "自定义下载源", "clone_prefix": settings.custom_download_prefix}
    return dict(next(item for item in GitMirrorConfig.DEFAULT_MIRRORS if item["id"] == settings.download_source))


async def fetch_information_file(file_path: str, source: Optional[str] = None) -> Dict[str, Any]:
    """静态市场索引固定从所选 CNB 或 GitHub 读取，失败不换源。"""
    from .git_mirror_service import GitMirrorConfig, mirror_raw_url

    selected = source if source is not None else (await asyncio.to_thread(load_source_settings)).information_source
    if selected not in {"cnb", "github"}:
        raise ValueError("当前插件信息源不是 Git 仓库")
    mirror = next(item for item in GitMirrorConfig.DEFAULT_MIRRORS if item["id"] == selected)
    url = await asyncio.to_thread(mirror_raw_url, "Mai-with-u", "plugin-repo", "main", file_path, mirror)
    async with httpx.AsyncClient(verify=get_shared_ssl_context(), timeout=20) as client:
        response = await client.get(url)
        response.raise_for_status()
    return {"success": True, "data": response.text, "mirror_used": selected, "attempts": 1, "url": url}


async def request_market(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(verify=get_shared_ssl_context(), timeout=20) as client:
        response = await client.get(f"{MARKET_BASE_URL}/api/v2/market/{path}", params=params)
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, dict):
        raise ValueError("插件中心返回了无效的数据结构")
    return data


async def request_market_detail(plugin_id: str, maibot_version: str) -> Dict[str, Any]:
    return await request_market(f"plugins/{quote(plugin_id, safe='')}", {"maibot_version": maibot_version})
