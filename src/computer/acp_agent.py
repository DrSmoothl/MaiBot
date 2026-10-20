"""外部 ACP Agent 后端：会话由外部 harness 持久化，图文、工具和 WebUI 仍由电脑工作区提供。"""

from contextlib import AsyncExitStack
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

import asyncio
import json

from src.llm_models.payload_content.context_item import (
    ContextImagePart, ContextItemBuilder, ContextItemMeta, ContextTextPart, ProviderActivityItem, RoleType,
    build_portable_output_items, get_item_text,
)

from .acp import ACPConnection
from .session import AgentSession
from .mcp_server import ComputerMCPServer

if TYPE_CHECKING:
    from .service import ComputerService


class AcpAgent(AgentSession):
    backend = "acp"

    def __init__(self, service: "ComputerService") -> None:
        super().__init__(service)
        self.config = service.bridge.config.model_copy(deep=True)
        self.connection: Optional[ACPConnection] = None
        self.mcp: Optional[ComputerMCPServer] = None
        self.capabilities: Dict[str, Any] = {}
        self._stop = asyncio.Event()

    async def connect(self) -> None:
        if self.connection is not None:
            if self.connection.failure is None:
                return
            await self.close()
        cwd = Path(self.config.acp_cwd).expanduser().resolve() if self.config.acp_cwd else self.history.root / "workspace"
        await asyncio.to_thread(cwd.mkdir, parents=True, exist_ok=True)
        if self.backend_state.get("cwd", str(cwd)) != str(cwd):
            raise ValueError("ACP 会话的主机工作目录已改变，请恢复原目录，或在外部会话停止后移走 acp/state.json 以新建会话")
        self.mcp = ComputerMCPServer(self)
        self.connection = ACPConnection(self._update)
        try:
            mount = await self.mcp.start()
            await self.connection.start(self.config.acp_command, self.config.acp_args, str(cwd), self.config.acp_env)
            initialized = await self.connection.request("initialize", {
                "protocolVersion": 1, "clientInfo": {"name": "maibot-computer", "version": "1"},
                "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
            }, self.config.acp_timeout)
            if initialized["protocolVersion"] != 1:
                raise ValueError("外部 Agent 未协商 ACP v1")
            self.capabilities = initialized["agentCapabilities"]
            if not self.capabilities.get("mcpCapabilities", {}).get("http", False):
                raise ValueError("外部 Agent 未声明 HTTP MCP 能力，无法挂载电脑工具")
            params = {"cwd": str(cwd), "mcpServers": [mount]}
            if self.backend_state.get("session_id"):
                if "resume" not in self.capabilities.get("sessionCapabilities", {}):
                    raise ValueError("外部 Agent 未声明 session/resume 能力，不能恢复已记录的会话")
                params["sessionId"] = self.backend_state["session_id"]
                session = await self.connection.request("session/resume", params, self.config.acp_timeout)
            else:
                session = await self.connection.request("session/new", params, self.config.acp_timeout)
                self.backend_state.update(session_id=session["sessionId"], cwd=str(cwd))
                await self.save()
            if self.config.acp_model:
                if not any(option["id"] == "model" for option in session.get("configOptions", [])):
                    raise ValueError("外部 Agent 未提供 model 配置选项")
                await self.connection.request("session/set_config_option", {
                    "sessionId": self.backend_state["session_id"], "configId": "model", "value": self.config.acp_model,
                }, self.config.acp_timeout)
        except BaseException:
            await self.close()
            raise

    async def _update(self, params: Dict[str, Any]) -> None:
        if params["sessionId"] != self.backend_state.get("session_id"):
            return
        update = params["update"]
        kind = update["sessionUpdate"]
        if kind in {"agent_message_chunk", "agent_thought_chunk"}:
            content = update["content"]
            if content["type"] == "text" and content["text"]:
                items = build_portable_output_items(
                    content=content["text"] if kind == "agent_message_chunk" else None,
                    reasoning=content["text"] if kind == "agent_thought_chunk" else None,
                    logical_turn_id=str(update.get("messageId", params["sessionId"])),
                )
            elif content["type"] == "image":
                items = [ContextItemBuilder().set_role(RoleType.Assistant).add_image_content(
                    content["mimeType"].split("/")[-1], content["data"],
                ).build()]
            else:
                return
            await self.history.append(await self.record("step", items))
        elif kind in {"tool_call", "tool_call_update"}:
            status = update.get("status", "pending")
            self.state = "tool" if status in {"pending", "in_progress"} else "thinking" if self.pending else "idle"
            title = update.get("title", update["toolCallId"])
            item = ProviderActivityItem(meta=ContextItemMeta.create(), action_type="tool", call_id=update["toolCallId"],
                                        display_summary=title, provider_type="acp", status=status)
            record = await self.record("step", [item])
            raw = record["items"][0]
            if kind == "tool_call":
                raw["tool_call"] = {"func_name": title, "args": update.get("rawInput", {})}
            else:
                raw.update(tool_name=title, success=status != "failed",
                           output=json.dumps(update.get("rawOutput", update.get("content", update)), ensure_ascii=False))
                # 使用现有 WebUI 的结果卡片，但不把外部协议事件误当成本体函数调用。
                raw["display_kind"] = "tool_result"
            await self.history.append(record)

    def request_stop(self) -> None:
        self._stop.set()

    async def _cancel_prompt(self, prompt: asyncio.Task[Any]) -> None:
        await self.connection.notify("session/cancel", {"sessionId": self.backend_state["session_id"]})
        grace = min(self.config.acp_timeout, 2) if self.service._closing else self.config.acp_timeout
        try:
            await asyncio.wait_for(asyncio.shield(prompt), grace)
        except asyncio.TimeoutError:
            await self.connection.close(1)
            raise RuntimeError("ACP 未确认取消，连接已关闭；不会重发原任务") from None

    async def run_turn(self) -> None:
        try:
            await self.connect()
            if self.pending is not None:
                uncertain = set(self.pending["input_ids"])
                self.inbox = [record for record in self.inbox if record["id"] not in uncertain]
                warning = ContextItemBuilder().add_text_content(
                    "上次 ACP 回合在进程中断前未确认结束，执行结果未知，未重发原任务。请检查外部会话及原桌面操作 ID。",
                ).build()
                await self.history.append(await self.record("step", [warning]))
                self.pending = None
                await self.save()
            batch = list(self.inbox)
            if not batch:
                return
            items = await asyncio.to_thread(self.history.unpack, [item for record in batch for item in record["items"]])
            prompt = []
            if not self.backend_state.get("policy_sent", False):
                prompt.append({"type": "text", "text": get_item_text(await asyncio.to_thread(self.system_prompt))})
            prompt.append({"type": "text", "text": await self.service.context()})
            for item in items:
                for part in item.parts:
                    if isinstance(part, ContextImagePart):
                        if not self.capabilities.get("promptCapabilities", {}).get("image", False):
                            raise ValueError("ACP 当前模型路由未声明图片输入能力，任务保留在队列中")
                        prompt.append({"type": "image", "data": part.image_base64, "mimeType": "image/" + part.image_format})
                    elif isinstance(part, ContextTextPart):
                        prompt.append({"type": "text", "text": part.text})
            self.pending = {"input_ids": [record["id"] for record in batch]}
            await self.save()
            for record in batch:
                await self.history.append(record)
                self.inbox.remove(record)
            await self.save()
            self.state = "thinking"
            self._stop.clear()
            task = asyncio.create_task(self.connection.request("session/prompt", {
                "sessionId": self.backend_state["session_id"], "prompt": prompt,
            }), name="computer.acp.prompt")
            stopping = asyncio.create_task(self._stop.wait())
            try:
                await asyncio.wait([task, stopping], return_when=asyncio.FIRST_COMPLETED)
                if not task.done():
                    await self._cancel_prompt(task)
                result = await asyncio.shield(task)
            except asyncio.CancelledError:
                await self._cancel_prompt(task)
                self.backend_state.update(policy_sent=True, stop_reason="cancelled")
                self.pending = None
                await self.save()
                raise
            finally:
                stopping.cancel()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, stopping, return_exceptions=True)
            self.backend_state["policy_sent"] = True
            self.backend_state["stop_reason"] = result["stopReason"]
            self.pending = None
            await self.save()
        finally:
            self.state = "idle"

    async def close(self) -> None:
        connection, mcp = self.connection, self.mcp
        self.connection, self.mcp = None, None
        async with AsyncExitStack() as cleanup:
            cleanup.push_async_callback(self.save)
            if mcp is not None:
                cleanup.push_async_callback(mcp.close)
            if connection is not None:
                cleanup.push_async_callback(connection.close, 1)
                if connection.failure is None and self.backend_state.get("session_id") and "close" in self.capabilities.get("sessionCapabilities", {}):
                    await connection.request("session/close", {"sessionId": self.backend_state["session_id"]}, min(self.config.acp_timeout, 2))
