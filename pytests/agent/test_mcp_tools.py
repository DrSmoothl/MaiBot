from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio

import httpx

from src.agent.mcp_server import ToolMCPServer
from src.core.tooling import ToolExecutionResult, ToolSpec


def test_mcp_rejects_invalid_arguments_and_unknown_tools_before_execution():
    async def scenario():
        provider = SimpleNamespace(
            provider_name="test", provider_type="builtin",
            list_tools=AsyncMock(return_value=[ToolSpec(
                name="echo", description="echo",
                parameters_schema={"type": "object", "properties": {"text": {"type": "string"}},
                                   "required": ["text"], "additionalProperties": False},
            )]),
            invoke=AsyncMock(return_value=ToolExecutionResult(tool_name="echo", success=True, content="ok")),
        )
        server = ToolMCPServer([provider], "test-session", 5)
        try:
            mount = await server.start()
            headers = {entry["name"]: entry["value"] for entry in mount["headers"]}
            headers["Accept"] = "application/json, text/event-stream"
            async with httpx.AsyncClient(headers=headers, trust_env=False) as client:
                for name, arguments in [("echo", {}), ("echo", {"text": 42}),
                                        ("echo", {"text": "ok", "unknown": True}), ("missing", {})]:
                    response = await client.post(mount["url"], json={
                        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                        "params": {"name": name, "arguments": arguments},
                    })
                    response.raise_for_status()
                    result = response.json()["result"]
                    assert result["isError"] is True
                    assert result["content"][0]["text"]
                provider.invoke.assert_not_awaited()

                response = await client.post(mount["url"], json={
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "echo", "arguments": {"text": "ok"}},
                })
                response.raise_for_status()
                assert response.json()["result"]["isError"] is False
                provider.invoke.assert_awaited_once()
        finally:
            await server.close()

    asyncio.run(scenario())
