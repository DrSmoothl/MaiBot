"""WebUI 子应用的连接生命周期，不影响公共文件传输。"""

from typing import Dict

from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect

import asyncio


class WebUIMount:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self._retired = False
        self._connections: Dict[asyncio.Task, Send] = {}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if self._retired:
            if scope["type"] == "websocket":
                await send({"type": "websocket.close", "code": 1001})
            else:
                await Response(status_code=404)(scope, receive, send)
            return
        if scope["type"] != "websocket":
            await self.app(scope, receive, send)
            return

        closed = False

        async def tracked_send(message: Message) -> None:
            nonlocal closed
            if closed:
                raise WebSocketDisconnect(code=1001)
            if message["type"] == "websocket.close":
                closed = True
            await send(message)

        # 只追踪我们拥有的处理任务，不能取消或等待 ASGI 宿主的连接任务。
        task = asyncio.create_task(self.app(scope, receive, tracked_send))
        self._connections[task] = tracked_send
        try:
            await task
        except asyncio.CancelledError:
            # 主动退役属于正常断开；其他取消仍向服务器传播。
            if not self._retired:
                raise
        finally:
            self._connections.pop(task, None)

    async def retire(self) -> None:
        """阻止新连接，通知旧连接关闭，并等待处理器清理订阅和会话。"""
        self._retired = True
        connections = list(self._connections.items())
        try:
            for task, send in connections:
                if not task.done():
                    try:
                        await send({"type": "websocket.close", "code": 1001, "reason": "WebUI configuration changed"})
                    except (OSError, WebSocketDisconnect):
                        # 对端已断开，仍需取消处理器以执行其 finally 清理。
                        pass
        finally:
            for task, _ in connections:
                if not task.done():
                    task.cancel()
            if connections:
                results = await asyncio.gather(*(task for task, _ in connections), return_exceptions=True)
                for result in results:
                    if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                        raise result
