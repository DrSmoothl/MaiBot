from pathlib import Path
from unittest.mock import Mock

import asyncio
import sys

import pytest

from src.agent import acp
from src.agent.acp import ACPConnection, session_cwd
from src.agent.config import AgentConfig
from src.agent.mcp_server import ToolMCPServer


# 使用真实 stdio 子进程核验启动 cwd、远端路径、端口替换及带认证的 MCP 回连。
AGENT_SCRIPT = r'''
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json
import os
import sys

assert os.getcwd() == sys.argv[1]
assert sys.argv[4] == '{"keep": "braces"}'
for line in sys.stdin:
    message = json.loads(line)
    method = message["method"]
    if method == "initialize":
        result = {"protocolVersion": 1, "agentCapabilities": {
            "mcpCapabilities": {"http": True}, "sessionCapabilities": {"resume": {}}
        }}
    else:
        assert method == sys.argv[5]
        assert message["params"]["cwd"] == sys.argv[2]
        mount = message["params"]["mcpServers"][0]
        assert mount["url"] == "http://127.0.0.1:" + sys.argv[3] + "/mcp"
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}).encode()
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        try:
            urlopen(Request(mount["url"], data=body, headers=headers), timeout=5)
        except HTTPError as exc:
            assert exc.code == 401
        else:
            raise AssertionError("MCP must require authentication")
        headers.update({entry["name"]: entry["value"] for entry in mount["headers"]})
        with urlopen(Request(mount["url"], data=body, headers=headers), timeout=5) as response:
            assert json.load(response)["result"]["tools"] == []
        result = {"sessionId": message["params"].get("sessionId", "remote-session")}
    print(json.dumps({"jsonrpc": "2.0", "id": message["id"], "result": result}), flush=True)
'''


@pytest.mark.parametrize("remote_cwd", ["/srv/dsh/workspace", r"D:\Agent Workspace"])
@pytest.mark.parametrize("session_id", [None, "saved-session"])
def test_custom_connection_preserves_remote_path_and_mounts_tools(tmp_path: Path, remote_cwd, session_id) -> None:
    async def scenario():
        config = AgentConfig(
            connection="custom", acp_command=sys.executable, acp_remote_cwd=remote_cwd,
            acp_args=["-u", "-c", AGENT_SCRIPT, str(tmp_path), remote_cwd, "{mcp_port}",
                      '{"keep": "braces"}', "session/resume" if session_id else "session/new"],
        )
        original_args = list(config.acp_args)
        connection = ACPConnection(lambda update: asyncio.sleep(0))
        mcp = ToolMCPServer([], "test-session", 5)
        try:
            mount = await mcp.start()
            await connection.initialize(config, tmp_path, "test-client", mcp_port=mcp.port)
            result = await connection.open_session(session_cwd(config, tmp_path), mount, session_id, 5)
            assert result["sessionId"] == (session_id or "remote-session")
            assert config.acp_args == original_args
        finally:
            await connection.close(1)
            await mcp.close()

    asyncio.run(scenario())


def test_stderr_is_logged_before_timeout_and_included_in_error(tmp_path: Path, monkeypatch) -> None:
    log = Mock()
    monkeypatch.setattr(acp, "logger", log)

    async def scenario():
        connection = ACPConnection(lambda update: asyncio.sleep(0))
        try:
            await connection.start(sys.executable, ["-u", "-c", "import sys; print('ssh: forwarding failed', file=sys.stderr, flush=True); sys.stdin.read()"], str(tmp_path), {})

            async def wait_for_log():
                while not log.info.called:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(wait_for_log(), 5)
            log.info.assert_called_with("ACP stderr: %s", "ssh: forwarding failed")
            with pytest.raises(TimeoutError, match="ssh: forwarding failed"):
                await connection.request("initialize", {}, 0.05)
            assert not connection._pending
        finally:
            await connection.close(1)

    asyncio.run(scenario())


def test_command_resolution_uses_configured_path(tmp_path: Path, monkeypatch) -> None:
    resolve = Mock(return_value=sys.executable)
    monkeypatch.setattr(acp.shutil, "which", resolve)

    async def scenario():
        connection = ACPConnection(lambda update: asyncio.sleep(0))
        try:
            await connection.start("remote-agent", ["-c", "import sys; sys.stdin.read()"], str(tmp_path), {"PATH": str(tmp_path)})
            resolve.assert_called_once_with("remote-agent", path=str(tmp_path))
        finally:
            await connection.close(1)

    asyncio.run(scenario())


def test_mcp_fixed_port_can_be_reused_after_close() -> None:
    async def scenario():
        first = ToolMCPServer([], "first", 5)
        try:
            mount = await first.start()
            port = first.port
        finally:
            await first.close()
        second = ToolMCPServer([], "second", 5, port=port)
        try:
            assert (await second.start())["url"] == mount["url"]
        finally:
            await second.close()

    asyncio.run(scenario())
