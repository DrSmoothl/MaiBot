"""独立电脑 Agent 的生命周期、沙盒状态与自主调度。"""

from contextlib import AsyncExitStack, suppress
from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio
import json
import time

from src.common.async_work import run_sync_work
from src.common.logger import get_logger
from src.common.paths import get_instance_root
from src.common.runtime_loop import run_on_main_loop
from src.config.config import config_manager, global_config

from . import COMPUTER_AGENT_ID, COMPUTER_NAME
from .bridge import SandboxBridge
from .persistence import read_json, write_json
from .agent import ComputerAgent
from .session import AgentSession

logger = get_logger("computer")


async def _cancel_task(task: Optional[asyncio.Task[None]]) -> None:
    if task is not None and task is not asyncio.current_task():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


class ComputerService:
    def __init__(self) -> None:
        root = get_instance_root()
        self.bridge = SandboxBridge(root / "temp" / "computer", root / "data" / "computer" / "workspace", global_config.computer)
        self.agent: Optional[AgentSession] = None
        self._backend_settings: Optional[Dict[str, Any]] = None
        self._closing = False
        self.agent_id = COMPUTER_AGENT_ID
        self.paused = False
        self.error = ""
        self.next_wake = 0.0
        self._task: Optional[asyncio.Task[None]] = None
        self._reload_registered = False
        self._lifecycle_lock = asyncio.Lock()
        self.activity: Dict[str, Any] = {}
        self._wake = asyncio.Event()
        self._turn_task: Optional[asyncio.Task[None]] = None
        self.takeover = False
        self._state_lock = asyncio.Lock()

    async def persist(self) -> None:
        async with self._state_lock:
            state = {"paused": self.paused, "takeover": self.takeover, "error": self.error,
                     "activity": dict(self.activity),
                     "wake_at": time.time() + max(0, self.next_wake - time.monotonic())}
            def save() -> None:
                self.bridge.runtime_dir.mkdir(parents=True, exist_ok=True)
                write_json(self.bridge.runtime_dir / "service.json", state)
            await run_sync_work(save)

    async def start(self) -> None:
        if not self._reload_registered:
            config_manager.register_reload_callback(self.reload, sections=("computer",))
            self._reload_registered = True
        await self.reload()

    async def reload(self) -> None:
        await run_on_main_loop(self._reload())

    async def _reload(self) -> None:
        async with self._lifecycle_lock:
            self.bridge.config = global_config.computer
            if not global_config.computer.enabled:
                await self.stop()
                return
            fields = {"backend"}
            if global_config.computer.backend == "acp":
                fields.update({"acp_command", "acp_args", "acp_cwd", "acp_env", "acp_model", "acp_timeout"})
            settings = global_config.computer.model_dump(include=fields)
            if self.agent is not None:
                if self._backend_settings == settings:
                    return
                await self.stop()
            state_path = self.bridge.runtime_dir / "service.json"
            if await asyncio.to_thread(state_path.exists):
                state = await asyncio.to_thread(read_json, state_path)
                self.activity = state.get("activity", {})
                self.paused, self.takeover = state["paused"], state["takeover"]
                self.error = state.get("error", "")
                self.next_wake = time.monotonic() + max(0, state["wake_at"] - time.time())
            try:
                await self.bridge.restore()
            except ConnectionError as exc:
                self.error, self.paused = str(exc), True
            if global_config.computer.backend == "acp":
                from .acp_agent import AcpAgent
                agent = AcpAgent(self)
            else:
                agent = ComputerAgent(self)
            await agent.load()
            self.agent = agent
            self._backend_settings = settings
            self._task = asyncio.create_task(self._run(), name="computer.agent")

    async def stop(self) -> None:
        self._closing = True
        try:
            # 按依赖逆序清理；前一步失败时仍执行后续清理，异常继续向上传播。
            async with AsyncExitStack() as cleanup:
                cleanup.push_async_callback(self.bridge.detach)
                cleanup.push_async_callback(self.persist)
                if self.agent is not None:
                    cleanup.push_async_callback(self.agent.close)
                cleanup.push_async_callback(_cancel_task, self._task)
                await _cancel_task(self._turn_task)
        finally:
            self.agent = None
            self._task = None
            self._backend_settings = None
            self._closing = False

    def require_agent(self) -> AgentSession:
        if self.agent is None:
            raise RuntimeError("电脑 Agent 未启动，请在本体配置中启用电脑使用。")
        return self.agent

    def require_computer(self, agent_id: str) -> None:
        self.require_agent()
        if agent_id != self.agent_id:
            raise PermissionError("桌面和跨聊天读取由独立电脑 Agent 执行；普通聊天请使用 computer_talk。")

    async def context(self) -> str:
        """电脑专用环境上下文，每轮替换注入，不重复追加固定说明到聊天历史。"""
        frame = self.bridge.last_frame
        observation = {key: value for key, value in frame.items() if key != "image"} if frame else None
        state, activity = self.status(), dict(self.activity)
        def read() -> str:
            diary = self.bridge.workspace / "diary.jsonl"
            notes = diary.read_text(encoding="utf-8").splitlines()[-10:] if diary.exists() else []
            return json.dumps({
                "state": state, "last_activity": activity, "last_observation": observation,
                "recent_notes": notes,
            }, ensure_ascii=False)
        return await asyncio.to_thread(read)

    async def wake(self) -> None:
        await self.require_agent().enqueue("探索机会已到：参考当前桌面、工作记录和日记，继续工作或选择活动、观察和休息。", kind="system")

    async def _run(self) -> None:
        while True:
            try:
                agent = self.require_agent()
                if self.paused or self.takeover:
                    await self._wake.wait()
                    self._wake.clear()
                    continue
                delay = max(0, self.next_wake - time.monotonic())
                if not agent.inbox:
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        await self.wake()
                self._wake.clear()
                if self.paused or self.takeover:
                    continue
                self._turn_task = asyncio.create_task(agent.run_turn(), name="computer.turn")
                try:
                    await self._turn_task
                except asyncio.CancelledError:
                    if asyncio.current_task().cancelling():
                        raise
                finally:
                    self._turn_task = None
                if not agent.inbox:
                    self._wake.clear()
                self.next_wake = max(self.next_wake, time.monotonic() + global_config.computer.idle_seconds)
                await self.persist()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.error, self.paused = str(exc), True
                logger.exception("电脑 Agent 执行失败，已暂停")
                await self.persist()

    async def desktop(self, actions: List[Dict[str, Any]], timeout: Optional[float]) -> Dict[str, Any]:
        if self.paused:
            raise RuntimeError("电脑探索已暂停，可用 computer_control resume 恢复。")
        try:
            result = await self.bridge.perform(actions, timeout)
            self.error = ""
            return result
        except ConnectionError as exc:
            # 连接丢失才暂停自主探索；软件报错由代理原样返回，不影响后续操作。
            self.error = str(exc)
            self.paused = True
            raise

    async def note(self, text: str) -> None:
        path = self.bridge.workspace / "diary.jsonl"

        def append() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"time": datetime.now().astimezone().isoformat(), "note": text}, ensure_ascii=False) + "\n")

        await asyncio.to_thread(append)

    async def rest(self, seconds: float) -> None:
        self.next_wake = time.monotonic() + seconds
        await self.persist()

    async def control(self, operation: str, *, from_agent: bool = False) -> Dict[str, Any]:
        self.require_agent()
        if operation not in {"pause", "resume", "takeover", "stop", "restart"}:
            raise ValueError("operation 只接受 pause/resume/takeover/stop/restart。")
        if operation in {"pause", "takeover", "stop", "restart"}:
            self.paused = True
            if not from_agent:
                await _cancel_task(self._turn_task)
        if operation == "takeover":
            self.takeover = True
            if self.bridge.pending is not None:
                cancelled = await self.bridge.cancel(self.bridge.pending)
                if cancelled["status"] == "running":
                    await self.bridge.poll(cancelled["operation_id"], 0)
            # 即使 key_down 已经完成，也可能仍有持续按键；交接前统一释放。
            released = await self.bridge.perform([{"action": "release"}])
            if released["status"] == "running":
                await self.bridge.poll(released["operation_id"], 0)
        elif operation == "resume":
            self.paused = False
            self.takeover = False
            self.error = ""
            self.next_wake = time.monotonic()
        elif operation in {"stop", "restart"}:
            await self.bridge.close()
            if operation == "restart":
                self.paused = False
                self.takeover = False
                self.error = ""
                self.next_wake = time.monotonic()
        self._wake.set()
        await self.persist()
        return self.status()

    def status(self) -> Dict[str, Any]:
        return {"enabled": global_config.computer.enabled, "name": COMPUTER_NAME, "agent_id": self.agent_id,
                "backend": self.agent.backend if self.agent is not None else global_config.computer.backend,
                "external_session_id": self.agent.backend_state.get("session_id") if self.agent is not None else None,
                "paused": self.paused, "error": self.error, "pending_request": self.bridge.pending,
                "takeover": self.takeover, "connected": self.bridge.connected, "sandbox_id": self.bridge.sandbox_id,
                "agent_state": self.agent.state if self.agent is not None else "stopped",
                "pause_scope": "decision_only; in-flight guest operation may still run",
                "next_wake_seconds": max(0, self.next_wake - time.monotonic()),
                "sandbox_running": self.bridge.running}


computer_service = ComputerService()
