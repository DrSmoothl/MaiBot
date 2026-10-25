"""共享沙盒电脑：独立于 Agent 的生命周期、输入互斥及人工接管。"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import asyncio

from src.common.async_work import run_sync_work
from src.common.json_storage import read_json, write_json
from src.common.paths import get_instance_root
from src.common.runtime_loop import run_on_main_loop
from src.config.config import config_manager, global_config

from .bridge import SandboxBridge
from .config import ComputerConfig


class ComputerService:
    def __init__(self, root: Optional[Path] = None, bridge: Optional[SandboxBridge] = None) -> None:
        instance = get_instance_root()
        self.root = root if root is not None else instance / "data" / "computer"
        self.bridge = bridge if bridge is not None else SandboxBridge(
            instance / "temp" / "computer", self.root / "workspace", global_config.computer,
        )
        self.paused = False
        self.takeover = False
        self.error = ""
        self.activity: Dict[str, Any] = {}
        self._control_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._reload_registered = False
        # 完成状态恢复前不接收输入，WebUI 可能先于业务服务启动。
        self._closing = True
        self._input_revision = 0
        self._state_loaded = False

    @property
    def config(self) -> ComputerConfig:
        return global_config.computer

    def require_enabled(self) -> None:
        if not self.config.enabled or self._closing:
            raise RuntimeError("电脑服务未启用或正在关闭，请检查 computer.enabled。")

    def _require_input(self, manual: bool) -> None:
        self.require_enabled()
        if manual:
            if not self.takeover:
                raise RuntimeError("先接管电脑，再发送人工输入。")
        elif self.paused or self.takeover:
            raise RuntimeError("电脑已暂停或正在人工接管，请在 WebUI 恢复。")

    async def persist(self) -> None:
        async with self._state_lock:
            state = {"paused": self.paused, "takeover": self.takeover, "error": self.error,
                     "activity": dict(self.activity)}

            def save() -> None:
                self.root.mkdir(parents=True, exist_ok=True)
                write_json(self.root / "runtime.json", state)

            await run_sync_work(save)
            self._state_loaded = True

    async def _load_state(self) -> None:
        if self._state_loaded:
            return
        path = self.root / "runtime.json"
        legacy_path = self.root.parent / "agent" / "runtime.json"
        if await asyncio.to_thread(path.exists):
            state = await asyncio.to_thread(read_json, path)
        elif await asyncio.to_thread(legacy_path.exists):
            # 首次拆分时保留旧电脑的接管/暂停状态，避免重启后未经人工恢复就发送输入。
            state = await asyncio.to_thread(read_json, legacy_path)
            if "takeover" not in state:
                self._state_loaded = True
                return
        else:
            self._state_loaded = True
            return
        self.paused, self.takeover = state["paused"], state["takeover"]
        self.error, self.activity = state["error"], state["activity"]
        await self.persist()

    async def start(self) -> None:
        if not self._reload_registered:
            config_manager.register_reload_callback(self.reload, sections=("computer",))
            self._reload_registered = True
        await self.reload()

    async def reload(self) -> None:
        await run_on_main_loop(self._reload())

    async def _reload(self) -> None:
        async with self._control_lock:
            await self._load_state()
            self.bridge.config = self.config
            if not self.config.enabled:
                await self.stop()
                return
            self._closing = False
            try:
                # 修改下一次启动参数时不重载正在执行的请求状态。
                if not self.bridge.connected:
                    await self.bridge.restore()
            except ConnectionError as exc:
                self.error, self.paused = str(exc), True

    async def stop(self) -> None:
        # 先阻止新输入；退出只保存并断开连接，保留来宾操作供下次恢复。
        self._closing = True
        self._input_revision += 1
        try:
            await self._load_state()
            await self.persist()
        finally:
            await self.bridge.detach()

    async def desktop(
        self, actions: List[Dict[str, Any]], timeout: Optional[float] = None,
        *, manual: bool = False, reason: str = "", caller: str = "",
    ) -> Dict[str, Any]:
        self._require_input(manual)
        revision = self._input_revision

        def guard() -> None:
            # 排队或启动沙盒期间可能发生接管，必须在桥接锁内、发布输入前再次检查。
            self._require_input(manual)
            if revision != self._input_revision:
                raise RuntimeError("电脑控制状态已改变，原排队输入已取消，请重新观察桌面。")

        self.activity = {"reason": reason, "caller": caller, "actions": actions}
        await self.persist()
        try:
            result = await self.bridge.perform(actions, timeout, before_submit=guard)
            self.error = ""
            return result
        except ConnectionError as exc:
            # 连接丢失才暂停电脑；应用错误仍交由调用者处理，不重放原操作。
            self.error, self.paused = str(exc), True
            await self.persist()
            raise

    async def observe(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        self.require_enabled()
        if self.paused and not self.bridge.running:
            raise RuntimeError("沙盒已停止，请先恢复电脑。")
        return await self.bridge.observe(timeout)

    async def poll(self, operation_id: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        self.require_enabled()
        if not self.bridge.running:
            raise RuntimeError("没有运行中的沙盒，不能查询旧操作；请先恢复电脑。")
        return await self.bridge.poll(operation_id, timeout)

    async def cancel(self, operation_id: str) -> Dict[str, Any]:
        self._require_input(False)
        if not self.bridge.running:
            raise RuntimeError("没有运行中的沙盒，不能取消旧操作。")
        return await self.bridge.cancel(operation_id)

    async def control(self, operation: str) -> Dict[str, Any]:
        self.require_enabled()
        if operation not in {"pause", "resume", "takeover", "stop", "restart"}:
            raise ValueError("operation 只接受 pause/resume/takeover/stop/restart。")
        async with self._control_lock:
            self.require_enabled()
            self._input_revision += 1
            if operation != "resume":
                self.paused = True
            try:
                if operation == "takeover":
                    # 先封锁所有自主输入，等待在途请求取消确认后才允许人工输入。
                    self.takeover = False
                    if self.bridge.pending is not None:
                        cancelled = await self.bridge.cancel(self.bridge.pending)
                        if cancelled["status"] == "running":
                            await self.bridge.poll(cancelled["operation_id"], 0)
                    # 已完成的 key_down 也可能仍按住，交接前统一释放。
                    released = await self.bridge.perform([{"action": "release"}])
                    if released["status"] == "running":
                        released = await self.bridge.poll(released["operation_id"], 0)
                    if not released["success"]:
                        raise RuntimeError(f"释放桌面输入失败：{released}")
                    self.takeover = True
                elif operation == "resume":
                    self.takeover = False
                    # 主动启动/恢复，内部 Agent 关闭时 WebUI 也能独立使用电脑。
                    await self.bridge.observe(0)
                    self.paused, self.error = False, ""
                elif operation in {"stop", "restart"}:
                    self.takeover = False
                    await self.bridge.close()
                    if operation == "restart":
                        await self.bridge.observe(0)
                        self.paused, self.error = False, ""
            except Exception as exc:
                self.error, self.paused = str(exc), True
                raise
            finally:
                await self.persist()
            return self.status()

    def status(self) -> Dict[str, Any]:
        return {"enabled": self.config.enabled, "paused": self.paused, "takeover": self.takeover,
                "error": self.error, "activity": dict(self.activity), "pending_request": self.bridge.pending,
                "connected": self.bridge.connected, "sandbox_id": self.bridge.sandbox_id,
                "sandbox_running": self.bridge.running}


computer_service = ComputerService()
