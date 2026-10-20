"""两种 Agent 共用的输入队列、会话状态和系统提示词。"""

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Sequence
from uuid import uuid4

import asyncio
import base64

from src.common.async_work import run_sync_work
from src.common.data_models.message_component_data_model import ImageComponent
from src.common.i18n import get_locale
from src.config.config import global_config
from src.llm_models.payload_content.context_item import ContextItem, ContextItemBuilder, RoleType

from .history import ComputerHistory
from .persistence import read_json, write_json

if TYPE_CHECKING:
    from .service import ComputerService


class AgentSession:
    backend = "builtin"

    def __init__(self, service: "ComputerService") -> None:
        self.service = service
        self.history = ComputerHistory(service.bridge.workspace.parent / ("agent" if self.backend == "builtin" else "acp"))
        self.state = "idle"
        self.inbox: List[Dict[str, Any]] = []
        self.pending: Optional[Dict[str, Any]] = None
        self.summary = ""
        self.summary_until = 0
        self._lock = asyncio.Lock()
        self.backend_state: Dict[str, Any] = {}

    async def load(self) -> None:
        await asyncio.to_thread(self.history.load)
        path = self.history.root / "state.json"
        if await asyncio.to_thread(path.exists):
            state = await asyncio.to_thread(read_json, path)
            self.inbox, self.pending = state["inbox"], state["pending"]
            self.summary, self.summary_until = state["summary"], state["summary_until"]
            self.backend_state = state.get("backend_state", {})

    async def save(self) -> None:
        async with self._lock:
            state = {"inbox": deepcopy(self.inbox), "pending": deepcopy(self.pending),
                     "summary": self.summary, "summary_until": self.summary_until, "backend_state": deepcopy(self.backend_state)}
            await run_sync_work(write_json, self.history.root / "state.json", state)

    async def enqueue(self, text: str, images: Sequence[ImageComponent] = (), *, kind: Literal["user", "system"] = "user") -> str:
        builder = ContextItemBuilder().add_text_content(text)
        for image in images:
            # 接口中的图片已校验、存入工作区；上下文直接带实际图像。
            from io import BytesIO
            from PIL import Image

            def image_format(data: bytes) -> str:
                with Image.open(BytesIO(data)) as decoded:
                    return decoded.format.lower()
            fmt = await asyncio.to_thread(image_format, image.binary_data)
            builder.add_image_content(fmt, base64.b64encode(image.binary_data).decode())
        record = await self.record(kind, [builder.build()])
        self.inbox.append(record)
        await self.save()
        self.service._wake.set()
        return record["id"]

    async def record(self, kind: str, items: Sequence[ContextItem]) -> Dict[str, Any]:
        return {"id": uuid4().hex, "time": datetime.now().astimezone().isoformat(), "kind": kind,
                "items": await asyncio.to_thread(self.history.pack, items)}

    async def drain(self) -> None:
        # 输入只在完整模型/工具回合之间入列，避免破坏调用与结果的相邻关系。
        while self.inbox:
            record = self.inbox[0]
            await self.history.append(record)
            self.inbox.pop(0)
            await self.save()

    def system_prompt(self) -> ContextItem:
        policy = (Path(__file__).parent / "prompts" / f"{get_locale()}.txt").read_text(encoding="utf-8")
        text = "\n\n".join([global_config.bot.nickname, global_config.personality.personality,
                                policy, global_config.computer.interests])
        return ContextItemBuilder().set_role(RoleType.System).add_text_content(text).build()

    async def close(self) -> None:
        await self.save()

    async def run_turn(self) -> None:
        raise NotImplementedError


