from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncio

import pytest

from src.common import startup_diagnostics
from src.plugin_runtime.host.supervisor import PluginRunnerSupervisor
from src.plugin_runtime.runner import rpc_client, runner_main


def test_handshake_timeout_keeps_failure_stage_and_exception(monkeypatch):
    error = TimeoutError("host did not respond")
    connection = SimpleNamespace(
        is_closed=False, send_frame=AsyncMock(), recv_frame=AsyncMock(side_effect=error),
    )
    transport = SimpleNamespace(connect=AsyncMock(return_value=connection))
    monkeypatch.setattr(rpc_client, "create_transport_client", lambda address: transport)
    warning = Mock()
    monkeypatch.setattr(startup_diagnostics.logger, "warning", warning)
    client = rpc_client.RPCClient("unused", "secret-session-token")
    with pytest.raises(TimeoutError) as caught:
        asyncio.run(client.connect_and_handshake())
    assert caught.value is error
    assert "runner.wait_hello_response:timeout=10s" in warning.call_args.args[0]
    assert "secret-session-token" not in str(warning.call_args_list)


def test_runner_fatal_error_is_logged_and_still_raised(monkeypatch):
    error = RuntimeError("startup failure")
    monkeypatch.setattr(runner_main, "initialize_logging", lambda **kwargs: None)
    monkeypatch.setattr(runner_main, "_disable_runner_console_logging", lambda: None)
    monkeypatch.setattr(runner_main, "_async_main", AsyncMock(side_effect=error))
    exception_log = Mock()
    monkeypatch.setattr(runner_main.logger, "exception", exception_log)
    with pytest.raises(RuntimeError) as caught:
        runner_main.main()
    assert caught.value is error
    exception_log.assert_called_once_with("Runner 进程异常退出")


def test_exited_runner_stderr_is_drained_before_cleanup(monkeypatch):
    async def scenario():
        supervisor = PluginRunnerSupervisor(plugin_dirs=[])
        supervisor._runner_process = SimpleNamespace(pid=1234, returncode=1)
        warning = Mock()
        monkeypatch.setattr(supervisor._logger, "warning", warning)
        stream = asyncio.StreamReader()
        supervisor._stderr_drain_task = asyncio.create_task(supervisor._drain_runner_stderr(stream))

        async def delayed_stderr():
            await asyncio.sleep(0)
            stream.feed_data(b"Traceback: runner handshake timed out\n")
            stream.feed_eof()

        writer = asyncio.create_task(delayed_stderr())
        await supervisor._shutdown_runner(reason="startup_failed")
        await writer
        assert supervisor._stderr_drain_task is None
        assert supervisor._runner_process is None
        warning.assert_any_call("[runner-stderr] Traceback: runner handshake timed out")

    asyncio.run(scenario())
