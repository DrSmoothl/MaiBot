"""ACP v1 的双向 NDJSON/stdio 连接，不解释终端文本或重试请求。"""

from collections import deque
from typing import Any, Awaitable, Callable, Dict, List, Optional

import asyncio
import json
import os
import shutil
import subprocess


class ACPConnection:
    def __init__(self, update: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self.update = update
        self.process: Optional[asyncio.subprocess.Process] = None
        self._reader: Optional[asyncio.Task[None]] = None
        self._stderr: Optional[asyncio.Task[None]] = None
        self._errors = deque(maxlen=20)
        self._pending: Dict[int, asyncio.Future[Any]] = {}
        self._sequence = 0
        self._write_lock = asyncio.Lock()
        self.failure: Optional[Exception] = None

    async def start(self, command: str, args: List[str], cwd: str, env: Dict[str, str]) -> None:
        executable = shutil.which(command)
        if executable is None:
            raise FileNotFoundError(f"找不到 ACP 启动程序：{command}")
        options = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {"start_new_session": True}
        self.process = await asyncio.create_subprocess_exec(
            executable, *args, cwd=cwd, env={**os.environ, **env}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=64 * 1024 * 1024, **options,
        )
        self._reader = asyncio.create_task(self._read(), name="computer.acp.read")
        self._stderr = asyncio.create_task(self._read_errors(), name="computer.acp.stderr")

    async def send(self, message: Dict[str, Any]) -> None:
        if self.failure is not None:
            raise self.failure
        if self.process is None or self.process.returncode is not None:
            raise ConnectionError("ACP 进程未运行")
        async with self._write_lock:
            self.process.stdin.write((json.dumps(message, ensure_ascii=False) + "\n").encode())
            await self.process.stdin.drain()

    async def notify(self, method: str, params: Dict[str, Any]) -> None:
        await self.send({"jsonrpc": "2.0", "method": method, "params": params})

    async def request(self, method: str, params: Dict[str, Any], timeout: Optional[float] = None) -> Any:
        self._sequence += 1
        request_id = self._sequence
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            try:
                return await asyncio.wait_for(asyncio.shield(future), timeout)
            except asyncio.TimeoutError:
                raise TimeoutError(f"ACP {method} 超时（{timeout} 秒），请求未重试") from None
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def _read_errors(self) -> None:
        while line := await self.process.stderr.readline():
            self._errors.append(line.decode("utf-8", errors="replace").rstrip())

    async def _read(self) -> None:
        try:
            while line := await self.process.stdout.readline():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError("ACP stdout 包含非协议输出，请使用 ACP/stdio 模式并将日志写入 stderr") from exc
                if message.get("jsonrpc") != "2.0":
                    raise ValueError("ACP stdout 必须仅包含 JSON-RPC 2.0 消息")
                if "method" in message:
                    if message["method"] == "session/update":
                        await self.update(message["params"])
                    elif "id" in message:
                        await self._server_request(message)
                elif message.get("id") in self._pending:
                    future = self._pending[message["id"]]
                    if not future.done():
                        if "error" in message:
                            future.set_exception(RuntimeError("ACP: " + json.dumps(message["error"], ensure_ascii=False)))
                        else:
                            future.set_result(message["result"])
            raise ConnectionError("ACP 连接已关闭\n" + "\n".join(self._errors))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = exc
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(self.failure or ConnectionError("ACP 连接已关闭"))

    async def _server_request(self, message: Dict[str, Any]) -> None:
        response = {"jsonrpc": "2.0", "id": message["id"]}
        if message["method"] == "session/request_permission":
            # 电脑工作区已由用户启用自主执行；只授权这一次，不创建外部持久授权。
            option = next((option for option in message["params"]["options"] if option["kind"] == "allow_once"), None)
            response["result"] = {"outcome": {"outcome": "selected", "optionId": option["optionId"]}
                                  if option is not None else {"outcome": "cancelled"}}
        else:
            response["error"] = {"code": -32601, "message": "客户端未提供此能力：" + message["method"]}
        await self.send(response)

    async def close(self, timeout: float) -> None:
        if self.process is not None and self.process.returncode is None:
            self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), timeout)
            except asyncio.TimeoutError:
                if os.name == "nt":
                    killer = await asyncio.create_subprocess_exec(
                        "taskkill", "/PID", str(self.process.pid), "/T", "/F",
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NO_WINDOW,
                    )
                    await killer.wait()
                else:
                    import signal
                    os.killpg(self.process.pid, signal.SIGKILL)
                await self.process.wait()
        for task in (self._reader, self._stderr):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
