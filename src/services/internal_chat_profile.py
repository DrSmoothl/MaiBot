"""内部聊天的共享主人昵称，不依赖某个浏览器或插件配置。"""

from pathlib import Path
from threading import RLock
from typing import Optional

import json
import os
import tempfile

from src.common.paths import get_instance_root

PROFILE_PATH = get_instance_root() / "data" / "internal_chat_profile.json"
_profile_lock = RLock()
DEFAULT_USER_NAME = "人类"


def _validate_name(name: str) -> str:
    normalized = name.strip()
    if not 1 <= len(normalized) <= 64:
        raise ValueError("内部聊天昵称长度必须为 1～64 个字符")
    return normalized


def get_internal_chat_user_name(initial_name: Optional[str] = None) -> str:
    """首次网页连接可提供旧浏览器昵称；已有共享昵称不被其他浏览器的旧值覆盖。"""
    with _profile_lock:
        if PROFILE_PATH.exists():
            name = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))["user_name"]
            if not isinstance(name, str):
                raise ValueError("内部聊天昵称必须是字符串")
            return _validate_name(name)
        if initial_name is not None:
            return set_internal_chat_user_name(initial_name)
        return DEFAULT_USER_NAME


def set_internal_chat_user_name(name: str) -> str:
    """原子保存昵称。同步文件访问由异步调用方交给线程池。"""
    normalized = _validate_name(name)
    with _profile_lock:
        PROFILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=PROFILE_PATH.parent,
                                             prefix=".internal-chat-", suffix=".tmp", delete=False) as handle:
                temporary = Path(handle.name)
                json.dump({"user_name": normalized}, handle, ensure_ascii=False)
            os.replace(temporary, PROFILE_PATH)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    return normalized
