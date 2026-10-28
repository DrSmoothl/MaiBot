"""桌宠窗口线程与 Agent 指令共用的待办存储，完成即删除。"""

from pathlib import Path
from typing import Any, Dict, List
import json
import threading
import time
import uuid


class TodoStore:
    """待办列表，完成即删除。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._items: List[Dict[str, Any]] = json.loads(path.read_text("utf-8")) if path.exists() else []

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._items]

    def add(self, text: str) -> Dict[str, Any]:
        text = self._validate_text(text)
        item = {"id": uuid.uuid4().hex, "text": text, "created_at": time.time()}
        with self._lock:
            self._save([*self._items, item])
        return dict(item)

    @staticmethod
    def _validate_text(text: str) -> str:
        if not isinstance(text, str) or not 1 <= len(text.strip()) <= 200:
            raise ValueError("待办内容必须为 1 至 200 字")
        return text.strip()

    def update(self, todo_id: str, text: str) -> Dict[str, Any]:
        text = self._validate_text(text)
        with self._lock:
            item = next((item for item in self._items if item["id"] == todo_id), None)
            if item is None:
                raise ValueError("待办不存在，请重新查询列表")
            updated = {**item, "text": text}
            self._save([updated if entry["id"] == todo_id else entry for entry in self._items])
            return dict(updated)

    def done(self, todo_id: str) -> None:
        with self._lock:
            if not any(item["id"] == todo_id for item in self._items):
                raise ValueError("待办不存在，请重新查询列表")
            self._save([item for item in self._items if item["id"] != todo_id])

    def _save(self, items: List[Dict[str, Any]]) -> None:
        # 先持久化再替换内存状态，写入失败不能显示为操作成功。
        temp_path = self._path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(items, ensure_ascii=False), "utf-8")
        temp_path.replace(self._path)
        self._items = items
