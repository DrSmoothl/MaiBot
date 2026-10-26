"""组装共享 HTTP 应用；WebUI 是可选子应用，不能控制文件通道的生命周期。"""

from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .webui_mount import WebUIMount

# 同步路由端点由 Starlette 交给 anyio 线程池执行，anyio 默认上限是 40；
# 底层 SQLite 连接池只有 5 条常驻 + 10 条溢出连接，并发过高会让线程排队等连接，
# 因此把同步端点的并发数限制在连接池容量以内。
MAX_CONCURRENT_SYNC_ENDPOINTS = 8


def limit_sync_endpoint_concurrency() -> int:
    """把同步路由端点的并发数限制在 SQLite 连接池容量以内，并返回生效值。

    必须在目标事件循环内调用：anyio 的线程池上限是按事件循环保存的，在循环外设置
    不会作用到 HTTP 服务的循环上。
    """

    import anyio.to_thread

    limiter = anyio.to_thread.current_default_thread_limiter()
    if limiter.total_tokens > MAX_CONCURRENT_SYNC_ENDPOINTS:
        limiter.total_tokens = MAX_CONCURRENT_SYNC_ENDPOINTS
    return int(limiter.total_tokens)


def create_app(
    host: str = "127.0.0.1",
    port: int = 8001,
    enable_static: bool = True,
    webui_enabled: Optional[bool] = None,
    show_token: bool = False,
) -> FastAPI:
    if webui_enabled is None:
        from src.config.config import global_config

        webui_enabled = global_config.webui.enabled

    from src.files.http import router as file_transfers

    # 公共服务不公开管理文档；WebUI 自己保留原有文档入口。
    app = FastAPI(title="MaiBot HTTP", docs_url=None, redoc_url=None, openapi_url=None)
    _setup_cors(app, port)
    app.include_router(file_transfers)
    app.state.webui = None
    if webui_enabled:
        from src.webui.app import create_app as create_webui_app

        # 最后挂载，既保留原有管理/前端 URL，又隔离 WebUI 防爬虫、压缩等中间件。
        webui = WebUIMount(create_webui_app(host=host, port=port, enable_static=enable_static))
        app.state.webui = webui
        app.mount("/", webui)
        if show_token:
            from src.webui.app import show_access_token

            show_access_token()
    return app


def _setup_cors(app: FastAPI, port: int):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:7999",
            "http://127.0.0.1:7999",
            f"http://localhost:{port}",
            f"http://127.0.0.1:{port}",
        ],
        allow_credentials=True,
        allow_methods=["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        allow_headers=[
            "Content-Type",
            "Accept",
            "Origin",
            "X-Requested-With",
            "Range",
        ],
        expose_headers=["Content-Length", "Content-Type", "Content-Range", "Content-Disposition", "ETag"],
    )
