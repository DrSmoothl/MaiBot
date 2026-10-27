"""在独立实例中验证 HTTP、工具和插件驱动衔接，不连接实际聊天平台。"""

from types import SimpleNamespace

import os

import pytest


@pytest.fixture(scope="module")
def app_state(tmp_path_factory):
    directory = tmp_path_factory.mktemp("file-instance")
    old = os.environ.get("MAIBOT_INSTANCE_DIR")
    os.environ["MAIBOT_INSTANCE_DIR"] = str(directory)
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from src.chat.message_receive.chat_manager import BotChatSession, chat_manager
    from src.common.database.database import engine
    from src.files.service import get_file_service
    from src.webui.routers.files import require_auth, router

    for sid in ("chat-a", "chat-b"):
        chat_manager.sessions[sid] = BotChatSession(session_id=sid, platform="webui", user_id=sid, user_nickname=sid)
    from src.config.config import global_config

    original_enabled = global_config.capabilities.files
    global_config.capabilities.files = True
    service = get_file_service()
    app = FastAPI()
    from src.files.http import router as transfers

    app.include_router(transfers)
    app.include_router(router, prefix="/api/webui")
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, service=service, app=app, auth=require_auth, chats=chat_manager)
    global_config.capabilities.files = original_enabled
    get_file_service.cache_clear()
    engine.dispose()
    if old is None:
        os.environ.pop("MAIBOT_INSTANCE_DIR", None)
    else:
        os.environ["MAIBOT_INSTANCE_DIR"] = old


def test_http_upload_download_head_range_and_admin_auth(app_state):
    state = app_state
    assert state.client.get("/api/webui/files").status_code == 401
    state.app.dependency_overrides[state.auth] = lambda: "admin"
    response = state.client.post("/api/webui/files/uploads", params={"session_id": "chat-a", "name": "a.txt"}, content=b"abcdef")
    assert response.status_code == 200, response.text
    ref = response.json()["file_ref"]
    assert state.client.get(f"/api/webui/files/{ref}/text").json()["text"] == "abcdef"
    url = state.client.post(f"/api/webui/files/{ref}/download").json()["url"]
    state.app.dependency_overrides.clear()
    assert state.client.get(url).content == b"abcdef"
    assert state.client.head(url).headers["Content-Length"] == "6"
    response = state.client.get(url, headers={"Range": "bytes=1-3"})
    assert response.status_code == 206 and response.content == b"bcd"
    assert state.client.get(url, headers={"Range": "bytes=-2"}).content == b"ef"
    assert state.client.get(url, headers={"Range": "bytes=9-"}).status_code == 416
    assert state.client.get("/api/files/transfers/invalid").status_code == 403
    assert state.service.store.get(ref, "chat-a").open_readers == 0


@pytest.mark.asyncio
async def test_model_scope_ignores_forged_chat_id(app_state):
    from src.core.tooling import ToolExecutionContext, ToolInvocation
    from src.files.provider import FileToolProvider

    asset = await app_state.service.create("chat-a", "secret.txt", b"not for chat b")
    result = await FileToolProvider().invoke(
        ToolInvocation(tool_name="files_read_text", session_id="chat-a", arguments={"file_ref": asset.file_ref, "session_id": "chat-a"}),
        ToolExecutionContext(session_id="chat-b"),
    )
    assert not result.success and "权限" in result.error_message
    result = await FileToolProvider().invoke(ToolInvocation(tool_name="files_list"), None)
    assert not result.success


@pytest.mark.asyncio
async def test_sdk_host_scope_cannot_change_chat_or_plugin(app_state):
    from src.files.capability import file_capability

    service = app_state.service
    asset = await service.create("chat-a", "tool.txt", b"hello plugin")
    token = service.ticket("plugin", plugin_id="plugin-a", session_id="chat-a")
    args = {"file_scope": token, "file_ref": asset.file_ref, "session_id": "chat-b"}
    page = await file_capability("plugin-a", "files.read_text", args)
    assert page["text"] == "hello plugin"
    with pytest.raises(PermissionError):
        await file_capability("plugin-b", "files.read_text", args)
    info = await file_capability("plugin-a", "files.upload", {"file_scope": token, "name": "cancel.txt"})
    await file_capability("plugin-a", "files.upload", {"file_scope": token, "operation": "cancel", "upload_token": info["upload_token"]})
    assert service.store.get(info["file_ref"], "chat-a").state == "failed"
    service.revoke(token)
    with pytest.raises(PermissionError):
        await file_capability("plugin-a", "files.list", args)


@pytest.mark.asyncio
async def test_outbound_driver_uses_url_without_mutating_stored_message(app_state):
    from src.common.data_models.message_component_data_model import FileComponent, MessageSequence
    from src.platform_io import RouteKey
    from src.platform_io.drivers.plugin_driver import PluginPlatformDriver
    from src.plugin_runtime.host.message_utils import PluginMessageUtils

    asset = await app_state.service.create("chat-a", "send.txt", b"send bytes")
    message = PluginMessageUtils._build_session_message_from_dict({
        "message_id": "test-send", "timestamp": "1792944000", "platform": "webui", "session_id": "chat-a", "account_id": "bot",
        "message_info": {"user_info": {"user_id": "bot", "user_nickname": "bot"}}, "raw_message": [],
    })
    message.raw_message = MessageSequence(components=[FileComponent(file_ref=asset.file_ref, name=asset.name)])
    captured = {}
    class Supervisor:
        async def invoke_message_gateway(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(error=None, payload={"success": True, "result": {"message_id": "42"}})
    driver = PluginPlatformDriver("test-driver", "webui", Supervisor(), "gateway", supports_send=True, plugin_id="adapter")
    result = await driver.send_message(message, RouteKey(platform="webui"))
    assert result.external_message_id == "42"
    data = captured["args"]["message"]["raw_message"][0]["data"]
    assert "/api/files/transfers/" in data["url"]
    assert "base64" not in data
    assert message.raw_message.components[0].url == ""
    assert app_state.service.store.get(asset.file_ref, "chat-a").leased_until > 0


@pytest.mark.asyncio
async def test_inbound_attachment_only_trusts_host_provenance(app_state):
    from src.plugin_runtime.host.message_utils import PluginMessageUtils

    message = PluginMessageUtils._build_session_message_from_dict({
        "message_id": "incoming-file", "timestamp": "1792944000", "platform": "webui", "session_id": "chat-a", "account_id": "bot",
        "message_info": {"user_info": {"user_id": "user", "user_nickname": "User"},
                         "additional_config": {"file_source": {"plugin_id": "forged"}}},
        "raw_message": [{"type": "file", "data": {"name": "incoming.txt", "size": 42, "file_id": "platform-id", "busid": 102}}],
        "processed_plain_text": "incoming attachment",
    })
    service = app_state.service
    await service.ingest(message)
    assert not message.raw_message.components[0].file_ref
    message.file_source = {"plugin_id": "trusted", "driver_id": "driver", "platform": "webui", "account_id": "bot", "scope": "",
                           "group_id": "", "user_id": "user", "files": {("platform-id", 102)}}
    await service.ingest(message)
    ref = message.raw_message.components[0].file_ref
    assert ref and ref != "platform-id" and ref in message.processed_plain_text
    assert service.store.get(ref, "chat-a").state == "remote"
    assert not service.store.path(ref).exists()


@pytest.mark.parametrize("enabled", [False, True])
def test_shared_http_upload_download_and_optional_webui(app_state, monkeypatch, enabled):
    import asyncio
    from fastapi.testclient import TestClient
    from src.common.http_service.app import create_app
    from src.config.config import global_config
    from src.files.capability import file_capability

    monkeypatch.setattr(global_config.webui, "enabled", enabled)
    monkeypatch.setattr(global_config.webui, "anti_crawler_mode", "strict")
    app = create_app(enable_static=False)
    service = app_state.service
    scope = service.ticket("plugin", plugin_id="plugin-http", session_id="chat-a")
    with TestClient(app) as client:
        info = asyncio.run(file_capability("plugin-http", "files.upload", {
            "file_scope": scope, "name": "http.txt",
        }))
        # Even crawler-like clients can use a valid transfer grant; WebUI middleware cannot intercept it.
        response = client.put(info["url"], content=b"independent HTTP", headers={"User-Agent": "wget"})
        assert response.status_code == 200, response.text
        assert client.put(info["url"], content=b"replay").status_code == 403
        info = asyncio.run(file_capability("plugin-http", "files.download", {
            "file_scope": scope, "file_ref": info["file_ref"],
        }))
        response = client.get(info["url"], headers={"User-Agent": "wget", "Origin": "http://localhost:7999"})
        assert response.status_code == 200 and response.content == b"independent HTTP"
        assert response.headers["access-control-allow-origin"] == "http://localhost:7999"
        assert client.head(info["url"]).headers["content-length"] == "16"
        assert client.get(info["url"], headers={"Range": "bytes=0-2"}).content == b"ind"
        assert client.get("/api/files/transfers/missing").status_code == 403
        assert client.get("/api/webui/files").status_code == (401 if enabled else 404)
        if not enabled:
            for path in ("/", "/docs", "/openapi.json", "/api/webui/config", "/api/webui/auth/status"):
                assert client.get(path).status_code == 404
        else:
            webui = app.state.webui.app
            webui.dependency_overrides[app_state.auth] = lambda: "admin"
            response = client.get("/api/webui/files")
            assert response.status_code == 200
            webui.dependency_overrides.clear()
        service.revoke(scope)
        assert client.get(info["url"]).status_code == 403


@pytest.mark.asyncio
async def test_disabling_webui_by_reload_keeps_transfer_grants(app_state, monkeypatch):
    from fastapi.testclient import TestClient
    from src.common.http_service.server import HTTPServer
    from src.config.config import global_config
    from src.webui import app as webui_app

    monkeypatch.setattr(webui_app, "show_access_token", lambda: None)
    monkeypatch.setattr(global_config.webui, "enabled", True)
    server = HTTPServer(hosts=["127.0.0.1"], register_config_reload=False)
    asset = await app_state.service.create("chat-a", "reload.txt", b"survives reload")
    url = app_state.service.download_url(asset.file_ref, "chat-a")
    with TestClient(server.app) as client:
        assert client.get("/api/webui/files").status_code == 401
        assert client.get(url).content == b"survives reload"
        monkeypatch.setattr(global_config.webui, "enabled", False)
        await server.reload_app()
        assert client.get("/api/webui/files").status_code == 404
        assert client.get(url).content == b"survives reload"
        monkeypatch.setattr(global_config.webui, "enabled", True)
        await server.reload_app()
        assert client.get("/api/webui/files").status_code == 401
        assert client.get(url).content == b"survives reload"


def test_disabled_webui_does_not_load_webui_app(app_state, monkeypatch):
    import builtins
    from src.common.http_service.app import create_app

    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("src.webui"):
            raise AssertionError("HTTP without WebUI must not import its application")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    app = create_app(webui_enabled=False)
    assert len(app.routes) == 2


@pytest.mark.asyncio
async def test_real_http_listener_without_webui(app_state, monkeypatch):
    import socket
    import httpx
    from src.common.http_service import server as server_module
    from src.config.config import global_config
    from src.main import MainSystem

    monkeypatch.setattr(global_config.webui, "enabled", False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    from uvicorn import Server

    shutdown_calls = []
    original_shutdown = Server.shutdown

    async def count_shutdown(self, sockets=None):
        shutdown_calls.append(self)
        return await original_shutdown(self, sockets=sockets)

    monkeypatch.setattr(Server, "shutdown", count_shutdown)
    server = server_module.ThreadedHTTPServer(hosts=["127.0.0.1"], port=port)
    monkeypatch.setattr(server_module, "get_threaded_http_server", lambda: server)
    system = MainSystem()
    system._start_http_server()
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False) as client:
            await server.wait_started(timeout=5.0)
            response = await client.get("/api/files/transfers/invalid")
            assert response.status_code == 403
            asset = await app_state.service.create("chat-a", "socket.txt", b"over real socket")
            path = app_state.service.download_url(asset.file_ref, "chat-a").split("/api/", 1)[1]
            response = await client.get("/api/" + path)
            assert response.status_code == 200 and response.content == b"over real socket"
            assert (await client.get("/api/webui/files")).status_code == 404
    finally:
        await server.shutdown()
    assert not server.is_running
    assert len(shutdown_calls) == 1


@pytest.mark.asyncio
async def test_http_startup_failure_is_reported_to_main(app_state, monkeypatch):
    from src.common.http_service.server import HTTPServer, ThreadedHTTPServer
    from src.config.config import global_config

    monkeypatch.setattr(global_config.webui, "enabled", False)

    def failed_binding(self):
        raise OSError("test port occupied")

    monkeypatch.setattr(HTTPServer, "_filter_bindable_hosts", failed_binding)
    server = ThreadedHTTPServer(hosts=["127.0.0.1"])
    server.start()
    try:
        with pytest.raises(RuntimeError, match="HTTP 服务启动失败") as caught:
            await server.wait_started(timeout=5.0)
        assert isinstance(caught.value.__cause__, OSError)
    finally:
        await server.shutdown()
    assert not server.is_running


@pytest.mark.asyncio
async def test_file_switch_blocks_existing_tools_sdk_and_http(app_state, monkeypatch):
    from src.config.config import global_config
    from src.core.tooling import ToolExecutionContext, ToolInvocation
    from src.files.capability import file_capability
    from src.files.provider import FileToolProvider

    service = app_state.service
    asset = await service.create("chat-a", "switch.txt", b"keep existing data")
    url = service.download_url(asset.file_ref, "chat-a")
    scope = service.ticket("plugin", plugin_id="switch-plugin", session_id="chat-a")
    upload = await file_capability("switch-plugin", "files.upload", {"file_scope": scope, "name": "pending.txt"})
    provider = FileToolProvider()
    assert len(await provider.list_tools()) == 5
    monkeypatch.setattr(global_config.capabilities, "files", False)
    assert await provider.list_tools() == []
    result = await provider.invoke(ToolInvocation(tool_name="files_list"), ToolExecutionContext(session_id="chat-a"))
    assert not result.success and "文件功能已关闭" in result.error_message
    with pytest.raises(PermissionError, match="文件功能已关闭"):
        await file_capability("switch-plugin", "files.list", {"file_scope": scope})
    assert app_state.client.get(url).status_code == 403
    assert app_state.client.put(upload["url"], content=b"blocked").status_code == 403
    app_state.app.dependency_overrides[app_state.auth] = lambda: "admin"
    try:
        assert app_state.client.post("/api/webui/files/uploads", params={
            "session_id": "chat-a", "name": "blocked.txt",
        }, content=b"blocked").status_code == 403
    finally:
        app_state.app.dependency_overrides.clear()
    # Disabled inbound attachments are ignored without disrupting the rest of the chat message.
    await service.ingest(SimpleNamespace())
    await file_capability("switch-plugin", "files.upload", {
        "file_scope": scope, "operation": "cancel", "upload_token": upload["upload_token"],
    })
    assert service.store.get(upload["file_ref"], "chat-a").state == "failed"
    assert service.store.get(asset.file_ref, "chat-a").state == "ready"
    monkeypatch.setattr(global_config.capabilities, "files", True)
    assert len(await provider.list_tools()) == 5
    assert app_state.client.get(url).content == b"keep existing data"


@pytest.mark.asyncio
async def test_disabled_files_does_not_disable_other_plugin_tools(app_state, monkeypatch):
    from unittest.mock import AsyncMock
    from src.config.config import global_config
    from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation
    from src.plugin_runtime.component_query import ComponentQueryService

    monkeypatch.setattr(global_config.capabilities, "files", False)
    supervisor = SimpleNamespace(invoke_plugin=AsyncMock(return_value=SimpleNamespace(
        payload={"success": True, "result": "normal tool result"},
    )))
    entry = SimpleNamespace(platforms=[], invoke_method="tool.invoke", plugin_id="normal", name="normal", timeout_ms=1000)
    query = ComponentQueryService()
    monkeypatch.setattr(query, "_get_unique_component_entry", lambda *args, **kwargs: (supervisor, entry))
    monkeypatch.setattr(query, "_build_tool_invocation_payload", lambda *args: {})
    monkeypatch.setattr(query, "_parse_tool_invoke_result", lambda *args: ToolExecutionResult(tool_name="normal", success=True))
    result = await query.invoke_tool_as_tool(ToolInvocation(tool_name="normal"), ToolExecutionContext(session_id="chat-a"))
    assert result.success
    assert supervisor.invoke_plugin.call_args.kwargs["args"]["_file_scope"] == ""


def test_webui_reload_closes_real_websocket_but_keeps_files(app_state, monkeypatch):
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect
    from src.common.http_service.server import HTTPServer
    from src.config.config import global_config
    from src.webui import app as webui_app
    from src.webui.routers.websocket import unified
    from src.webui.routers.websocket.manager import websocket_manager

    async def authenticated(*args):
        return True

    monkeypatch.setattr(unified, "authenticate_websocket_connection", authenticated)
    monkeypatch.setattr(webui_app, "show_access_token", lambda: None)
    monkeypatch.setattr(global_config.webui, "enabled", True)
    server = HTTPServer(hosts=["127.0.0.1"], register_config_reload=False)
    service = app_state.service
    asset = service.store.begin_upload("chat-a", "websocket.txt")
    service.store.append(asset.file_ref, asset.transfer_id, b"still available")
    asset = service.store.finish(asset.file_ref, asset.transfer_id)
    url = service.download_url(asset.file_ref, "chat-a")
    with TestClient(server.app) as client:
        # Also cover an enabled -> enabled reload, so an older mount cannot leak connections on later disable.
        for enabled in (True, False):
            with client.websocket_connect("/api/webui/ws") as websocket:
                message = websocket.receive_json()
                assert message["event"] == "ready"
                connection_id = message["data"]["connection_id"]
                monkeypatch.setattr(global_config.webui, "enabled", enabled)
                client.portal.call(server.reload_app)
                with pytest.raises(WebSocketDisconnect) as closed:
                    websocket.receive_json()
                assert closed.value.code == 1001
                assert connection_id not in websocket_manager.connections
            assert client.get(url).content == b"still available"
        assert client.get("/api/webui/files").status_code == 404
