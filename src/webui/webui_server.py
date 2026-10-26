"""旧 WebUI 服务导入入口；监听和生命周期已由底层 HTTP 服务统一管理。"""

from src.common.http_service.server import (
    HTTPServer as WebUIServer,
    ThreadedHTTPServer as ThreadedWebUIServer,
    get_http_server as get_webui_server,
    get_threaded_http_server as get_threaded_webui_server,
)

__all__ = ["WebUIServer", "ThreadedWebUIServer", "get_webui_server", "get_threaded_webui_server"]
