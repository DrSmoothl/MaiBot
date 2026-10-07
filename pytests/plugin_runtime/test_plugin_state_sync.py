from types import SimpleNamespace

import asyncio

import pytest

from src.config.file_watcher import FileChange
from src.plugin_runtime.integration import PluginRuntimeManager
from src.webui.routers.websocket import plugin_runtime as runtime_ws


@pytest.mark.asyncio
async def test_snapshot_only_marks_plugins_in_their_loading_supervisor(tmp_path, monkeypatch):
    manager = PluginRuntimeManager()
    manager._started = True
    extension = SimpleNamespace(_plugin_dirs=[tmp_path], is_loading=False)
    core = SimpleNamespace(_plugin_dirs=[tmp_path], is_loading=True)
    manager._builtin_supervisor = core
    manager._third_party_supervisor = extension
    monkeypatch.setattr(manager, "_iter_discovered_plugin_paths", lambda dirs: iter([
        ("demo.extension", tmp_path / "extension"), ("demo.adapter", tmp_path / "adapter"),
    ]))
    monkeypatch.setattr(manager, "_supervisor_accepts_plugin_path", lambda sv, path:
                        (sv is core) == (path.name == "adapter"))
    monkeypatch.setattr(manager, "get_plugin_load_statuses", lambda: {})
    monkeypatch.setattr(manager, "get_plugin_load_failure_reasons", lambda: {})
    monkeypatch.setattr(manager, "get_plugin_circuit_statuses", lambda: {})
    snapshot = await manager.get_plugin_state_snapshot()
    assert snapshot["statuses"] == {"demo.extension": "not_loaded", "demo.adapter": "loading"}


@pytest.mark.asyncio
async def test_explicit_toggle_does_not_depend_on_a_file_event(monkeypatch):
    manager = PluginRuntimeManager()
    manager._started = True
    loaded = []
    calls = []
    notifications = []
    manager.subscribe_status_changes(lambda: notifications.append(dict(manager._plugin_transitions)))

    async def inspect(plugin_id):
        return SimpleNamespace(enabled=True, normalized_config={"plugin": {"enabled": True}})

    async def load(plugin_id, reason):
        assert manager._plugin_file_update_lock.locked()
        calls.append("load")
        loaded.append(plugin_id)
        return True

    async def save():
        assert manager._plugin_file_update_lock.locked()
        calls.append("save")

    supervisor = SimpleNamespace(inspect_plugin_config=inspect, get_loaded_plugin_ids=lambda: loaded)
    monkeypatch.setattr(manager, "_get_supervisor_for_plugin", lambda plugin_id: supervisor)
    monkeypatch.setattr(manager, "load_plugin_globally", load)
    async def refresh():
        pass

    monkeypatch.setattr(manager, "_refresh_plugin_config_watch_subscriptions", refresh)
    assert await manager.apply_plugin_config("demo", save) == "success"
    # 文件监听随后收到同一份配置，不再次加载或通知插件。
    assert await manager.apply_plugin_config("demo") == "success"
    assert calls == ["save", "load"]
    assert notifications == [{"demo": "loading"}, {}]


@pytest.mark.asyncio
async def test_file_callback_returns_before_slow_plugin_work(tmp_path, monkeypatch):
    manager = PluginRuntimeManager()
    manager._started = True
    completed = asyncio.Event()

    async def apply(plugin_id):
        await asyncio.sleep(0.03)
        completed.set()

    monkeypatch.setattr(manager, "apply_plugin_config", apply)
    await asyncio.wait_for(manager._handle_plugin_config_changes(
        "demo", [FileChange(change_type=2, path=tmp_path / "config.toml")],
    ), timeout=0.01)
    assert not completed.is_set()
    await manager._plugin_change_task
    assert completed.is_set()


@pytest.mark.asyncio
async def test_status_subscription_replays_and_handles_cross_thread_notifications(monkeypatch):
    manager = PluginRuntimeManager()
    monkeypatch.setattr(runtime_ws, "get_plugin_runtime_manager", lambda: manager)
    received = asyncio.Queue()

    async def send_event(*args, **kwargs):
        received.put_nowait(kwargs["event"])

    async def send_response(*args, **kwargs):
        pass

    monkeypatch.setattr(runtime_ws.websocket_manager, "send_event", send_event)
    monkeypatch.setattr(runtime_ws.websocket_manager, "send_response", send_response)
    monkeypatch.setattr(runtime_ws.websocket_manager, "subscribe", lambda *args, **kwargs: None)
    await runtime_ws.subscribe_plugin_runtime("test", None)
    assert await asyncio.wait_for(received.get(), 1) == "changed"
    await asyncio.to_thread(manager._notify_status_changed)
    assert await asyncio.wait_for(received.get(), 1) == "changed"
    runtime_ws.stop_plugin_runtime_subscription("test")
    await asyncio.sleep(0)
    assert not manager._status_listeners
    # 连接立即退订时，后台协程可能还没开始，也必须释放监听。
    await runtime_ws.subscribe_plugin_runtime("test", None)
    runtime_ws.stop_plugin_runtime_subscription("test")
    assert not manager._status_listeners


@pytest.mark.asyncio
async def test_failed_config_notification_does_not_mark_running_plugin_as_failed(monkeypatch):
    manager = PluginRuntimeManager()
    manager._started = True

    async def inspect(plugin_id):
        return SimpleNamespace(enabled=True, normalized_config={"plugin": {"enabled": True}})

    async def notify(**kwargs):
        return False

    supervisor = SimpleNamespace(
        inspect_plugin_config=inspect, get_loaded_plugin_ids=lambda: ["demo"],
        notify_plugin_config_updated=notify,
    )
    monkeypatch.setattr(manager, "_get_supervisor_for_plugin", lambda plugin_id: supervisor)
    monkeypatch.setattr(manager, "get_plugin_load_statuses", lambda: {"demo": "success"})
    with pytest.raises(RuntimeError, match="插件配置更新通知失败"):
        await manager.apply_plugin_config("demo")
    snapshot = await manager.get_plugin_state_snapshot()
    assert snapshot["statuses"]["demo"] == "success"
    assert not manager._plugin_transitions
    assert "demo" not in manager._applied_plugin_configs
