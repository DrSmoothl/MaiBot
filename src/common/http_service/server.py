"""独立的 HTTP 服务器。"""

from typing import Any, List, Optional, Sequence

import asyncio
import sys
import threading

from src.common.logger import get_logger
from src.common.utils.port_checker import (
    assert_port_available,
    is_address_unavailable_error,
    is_port_conflict_error,
    log_port_conflict,
)

logger = get_logger("http_server")


def _get_loaded_config_manager() -> Optional[Any]:
    config_module = sys.modules.get("src.config.config")
    if config_module is None:
        return None
    return getattr(config_module, "config_manager", None)


class _ASGIProxy:
    def __init__(self, app):
        self._app = app

    def set_app(self, app) -> None:
        self._app = app

    async def __call__(self, scope, receive, send):
        await self._app(scope, receive, send)


class HTTPServer:
    """独立的 HTTP 服务器"""

    def __init__(self, hosts: Optional[List[str]] = None, port: int = 8001, register_config_reload: bool = True):
        self.hosts = hosts or ["127.0.0.1", "::1"]
        self.port = port
        from .app import create_app

        self._app = create_app(host=self.hosts[0], port=port, enable_static=True, show_token=True)
        self.app = _ASGIProxy(self._app)
        self._server: Optional[Any] = None
        self._watchdog_task: Optional["asyncio.Task[None]"] = None
        self._serve_task: Optional[asyncio.Task] = None
        self._reload_callback_registered = False
        self._register_config_reload = register_config_reload

        if self._register_config_reload:
            self._maybe_register_reload_callback()

    def _maybe_register_reload_callback(self) -> None:
        if not self._register_config_reload:
            return
        if self._reload_callback_registered:
            return

        config_manager = _get_loaded_config_manager()
        if config_manager is None:
            return

        # WebUI 的开关和防爬虫配置在组装时确定；更改时重建子应用，不重启 HTTP 监听
        config_manager.register_reload_callback(self.reload_app, sections=("webui",))
        self._reload_callback_registered = True

    def _maybe_unregister_reload_callback(self) -> None:
        if not self._register_config_reload:
            return
        if not self._reload_callback_registered:
            return

        config_manager = _get_loaded_config_manager()
        if config_manager is None:
            return

        config_manager.unregister_reload_callback(self.reload_app)
        self._reload_callback_registered = False

    async def reload_app(self) -> None:
        from .app import create_app

        previous_webui = self._app.state.webui
        self._app = await asyncio.to_thread(create_app, host=self.hosts[0], port=self.port, enable_static=True)
        self.app.set_app(self._app)
        # 替换应用只影响新请求；旧 WebSocket 必须显式结束，否则关闭 WebUI 后仍可操作。
        if previous_webui is not None:
            await previous_webui.retire()
        logger.info("HTTP 应用已热重载")

    def _create_bound_sockets(self, hosts: List[str]) -> list:
        import socket as _socket

        sockets = []
        for host in hosts:
            addr_info_list = _socket.getaddrinfo(host, self.port, _socket.AF_UNSPEC, _socket.SOCK_STREAM)
            for af, socktype, proto, _, sa in addr_info_list:
                sock = None
                try:
                    sock = _socket.socket(af, socktype, proto)
                    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
                    if af == _socket.AF_INET6:
                        try:
                            sock.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 1)
                        except OSError:
                            pass
                    sock.bind(sa)
                    sock.listen()
                    sockets.append(sock)
                    break
                except OSError as bind_err:
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                    logger.warning(f"⚠️ HTTP 无法绑定到 {host}:{self.port} ({sa}): {bind_err}")
                    continue

        return sockets

    def _filter_bindable_hosts(self) -> List[str]:
        """预检各监听地址的端口，返回可用于绑定的地址。

        端口被占用时报错中止；本机不存在的地址（如系统禁用 IPv6 后的 ::1）只记录警告并跳过该地址，
        是否还有可绑定的地址由调用方在实际绑定后判断。
        """
        bindable_hosts: List[str] = []
        for host in self.hosts:
            try:
                assert_port_available(
                    host=host,
                    port=self.port,
                    service_name="HTTP 服务器",
                    logger=logger,
                    config_hint="http.port (config/bot_config.toml)",
                    allow_reuse_addr=True,
                )
            except OSError as e:
                if not is_address_unavailable_error(e):
                    raise
                logger.warning(f"⚠️ HTTP 监听地址 {host}:{self.port} 在本机不可用，已跳过: {e}")
                continue
            bindable_hosts.append(host)
        return bindable_hosts

    def _log_bind_addresses(self, hosts: List[str]) -> None:
        has_v4_localhost = False
        has_v6_localhost = False
        has_v4_wildcard = False
        has_v6_wildcard = False

        for host in hosts:
            if ":" in host:
                logger.info(f"🌐 访问地址: http://[{host}]:{self.port}")
                if host == "::":
                    has_v6_wildcard = True
                elif host == "::1":
                    has_v6_localhost = True
            else:
                logger.info(f"🌐 访问地址: http://{host}:{self.port}")
                if host == "0.0.0.0":
                    has_v4_wildcard = True
                elif host == "127.0.0.1":
                    has_v4_localhost = True

        if has_v4_wildcard or has_v6_wildcard:
            local = []
            if has_v4_wildcard or has_v4_localhost:
                local.append(f"http://localhost:{self.port}")
            if has_v6_wildcard or has_v6_localhost:
                local.append(f"http://[::1]:{self.port}")
            if local:
                logger.info(f"💡 本机访问: {'， '.join(local)}")

    async def start(self):
        """启动服务器；serve 是唯一负责执行 Uvicorn shutdown 的任务。"""
        from uvicorn import Config
        from uvicorn import Server as UvicornServer

        from .app import limit_sync_endpoint_concurrency
        from src.common.event_loop_watchdog import start_watchdog

        self._serve_task = asyncio.current_task()
        sockets = []
        try:
            self._maybe_register_reload_callback()
            # 必须在本循环内设置：anyio 的线程池上限按事件循环保存。
            logger.debug(f"同步路由端点并发上限: {limit_sync_endpoint_concurrency()}")
            # 看门狗同样必须挂在本循环上，才能检测 HTTP 服务卡顿。
            self._watchdog_task = start_watchdog("http")
            bindable_hosts = self._filter_bindable_hosts()
            sockets = self._create_bound_sockets(bindable_hosts)
            if not sockets:
                raise OSError("HTTP 无法绑定到任何指定地址")

            config = Config(
                app=self.app,
                host=self.hosts[0],
                port=self.port,
                log_config=None,
                access_log=False,
                timeout_graceful_shutdown=3.0,
            )
            self._server = UvicornServer(config=config)
            logger.info("🌐 HTTP 服务器启动中...")
            self._log_bind_addresses(bindable_hosts)
            if len(bindable_hosts) > 1:
                logger.info("🔗 HTTP 已绑定到多个地址")
            await self._server.serve(sockets=sockets)
        except OSError as exc:
            if is_port_conflict_error(exc):
                for host in self.hosts:
                    log_port_conflict(
                        logger, service_name="HTTP 服务器", host=host, port=self.port,
                        config_hint="http.port (config/bot_config.toml)",
                    )
            else:
                logger.error(f"❌ HTTP 服务器网络错误: {exc}")
            raise
        except Exception as exc:
            logger.error(f"❌ HTTP 服务器运行错误: {exc}", exc_info=True)
            raise
        finally:
            for sock in sockets:
                sock.close()
            if self._watchdog_task is not None:
                self._watchdog_task.cancel()
                self._watchdog_task = None
            self._maybe_unregister_reload_callback()
            self._server = None
            self._serve_task = None

    def request_shutdown(self) -> None:
        """由监听循环执行退出请求，清理由 serve 唯一负责。"""
        if self._server is not None:
            self._server.should_exit = True

    async def shutdown(self) -> None:
        """请求 serve 退出并等待它完成清理，不重复调用 Uvicorn shutdown。"""
        if self._server is None:
            return
        logger.info("正在关闭 HTTP 服务器...")
        self.request_shutdown()
        task = self._serve_task
        if task is not None and task is not asyncio.current_task():
            await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
        logger.info("✅ HTTP 服务器已关闭")


class ThreadedHTTPServer:
    """在专用线程中运行 HTTP，避免阻塞主事件循环。"""

    def __init__(self, hosts: Optional[List[str]] = None, port: int = 8001) -> None:
        self.hosts = hosts or ["127.0.0.1", "::1"]
        self.port = port
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: HTTPServer | None = None
        self._startup_event = threading.Event()
        self._startup_error: BaseException | None = None
        self._reload_callback_registered = False

    @property
    def is_running(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def start(self) -> None:
        """启动 HTTP 专用线程。"""
        if self.is_running:
            return

        self._startup_error = None
        self._startup_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="maibot-http",
            daemon=True,
        )
        self._thread.start()
        self._maybe_register_reload_callback()
        logger.info("HTTP 服务器已在独立线程中启动")

    async def wait_started(self, timeout: float = 30.0) -> None:
        """等待实际监听就绪，向主启动链路暴露线程中的绑定/初始化错误。"""
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self._startup_error is not None:
                raise RuntimeError("HTTP 服务启动失败") from self._startup_error
            server = self._server
            if server is not None and server._server is not None and server._server.started:
                return
            if not self.is_running:
                raise RuntimeError("HTTP 服务线程已退出")
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("等待 HTTP 服务监听超时")
            await asyncio.sleep(0.05)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        try:
            self._server = HTTPServer(
                hosts=self.hosts,
                port=self.port,
                register_config_reload=False,
            )
            self._startup_event.set()
            loop.run_until_complete(self._server.start())
        except Exception as exc:
            self._startup_error = exc
            if not self._startup_event.is_set():
                self._startup_event.set()
            logger.error(f"HTTP 独立线程运行失败: {exc}", exc_info=True)
        finally:
            if not self._startup_event.is_set():
                self._startup_event.set()
            try:
                pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                for task in pending_tasks:
                    task.cancel()
                if pending_tasks:
                    loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception as exc:
                logger.debug(f"清理 HTTP 线程事件循环时出现异常: {exc}")
            finally:
                loop.close()
                self._server = None
                self._loop = None

    def _maybe_register_reload_callback(self) -> None:
        if self._reload_callback_registered:
            return

        config_manager = _get_loaded_config_manager()
        if config_manager is None:
            return

        # WebUI 的开关和防爬虫配置在组装时确定；更改时重建子应用，不重启 HTTP 监听
        config_manager.register_reload_callback(self.reload_app, sections=("webui",))
        self._reload_callback_registered = True

    def _maybe_unregister_reload_callback(self) -> None:
        if not self._reload_callback_registered:
            return

        config_manager = _get_loaded_config_manager()
        if config_manager is None:
            return

        config_manager.unregister_reload_callback(self.reload_app)
        self._reload_callback_registered = False

    async def reload_app(self, changed_scopes: Optional[Sequence[str]] = None) -> None:
        """在线程事件循环中重建 HTTP app。"""
        if changed_scopes and "bot" not in changed_scopes:
            return

        if not self._startup_event.is_set():
            await asyncio.to_thread(self._startup_event.wait, 5.0)
        if self._startup_error is not None:
            return

        loop = self._loop
        server = self._server
        if loop is None or server is None or not loop.is_running():
            return

        future = asyncio.run_coroutine_threadsafe(server.reload_app(), loop)
        await asyncio.wrap_future(future)

    async def shutdown(self, timeout: float = 5.0) -> None:
        """关闭 HTTP 专用线程。"""
        self._maybe_unregister_reload_callback()

        thread = self._thread
        if thread is None:
            return

        if not self._startup_event.is_set():
            await asyncio.to_thread(self._startup_event.wait, timeout)

        loop = self._loop
        server = self._server
        if loop is not None and server is not None and loop.is_running():
            # 在线程内发退出信号；在线程外 join，避免监听任务结束时取消正在等待它的 shutdown 协程。
            loop.call_soon_threadsafe(server.request_shutdown)

        if thread.is_alive():
            await asyncio.to_thread(thread.join, timeout)
        if thread.is_alive():
            logger.warning("HTTP 线程未能在超时时间内退出")
        else:
            logger.info("HTTP 线程已关闭")
            self._thread = None


# 全局 HTTP 服务器实例
_http_server = None
_threaded_http_server = None


def get_http_server() -> HTTPServer:
    """获取全局 HTTP 服务器实例"""
    global _http_server
    if _http_server is None:
        from src.config.startup_bindings import resolve_http_bind_address

        bind_address = resolve_http_bind_address()
        _http_server = HTTPServer(hosts=bind_address.hosts, port=bind_address.port)
    return _http_server


def get_threaded_http_server() -> ThreadedHTTPServer:
    """获取运行在专用线程中的 HTTP 服务器实例。"""
    global _threaded_http_server
    if _threaded_http_server is None:
        from src.config.startup_bindings import resolve_http_bind_address

        bind_address = resolve_http_bind_address()
        _threaded_http_server = ThreadedHTTPServer(hosts=bind_address.hosts, port=bind_address.port)
    return _threaded_http_server
