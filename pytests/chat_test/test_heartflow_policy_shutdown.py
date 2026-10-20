from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from watchfiles import Change

import pytest

from src.chat.heart_flow.heartflow_manager import HeartflowManager
from src.config.config import ConfigManager
from src.config.file_watcher import FileChange
from src.platform_io.adapter_policy import AdapterPolicyManager
from src.plugin_runtime.integration import PluginRuntimeManager


@pytest.mark.asyncio
async def test_policy_change_closes_blocked_runtimes_and_keeps_allowed(tmp_path: Path, monkeypatch):
    manager = HeartflowManager()
    chats = {}
    for name, platform, is_group in (
        ("blocked_group", "qq", True),
        ("blocked_private", "qq", False),
        ("allowed", "qq", True),
        ("local", "maisaka_cli", False),
        ("webui", "webui", True),
    ):
        chat = SimpleNamespace(
            chat_stream=SimpleNamespace(
                platform=platform,
                account_id="bot",
                scope="main",
                is_group_session=is_group,
                group_id=name,
                user_id=name,
            ),
            stop=AsyncMock(),
        )
        chats[name] = chat
        manager.heartflow_chat_list[name] = chat
        manager._touch_chat(name)
    platform_manager = SimpleNamespace(
        ensure_send_pipeline_ready=AsyncMock(),
        is_target_allowed=AsyncMock(side_effect=lambda route, *, chat_type, target_id: target_id == "allowed"),
    )
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.get_platform_io_manager", lambda: platform_manager)
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.heartflow_manager", manager)
    policy = AdapterPolicyManager(tmp_path / "adapter_policy.toml")
    monkeypatch.setattr("src.platform_io.adapter_policy.get_adapter_policy_manager", lambda: policy)
    config = ConfigManager()
    config.bot_config_path = tmp_path / "bot_config.toml"
    config.model_config_path = tmp_path / "model_config.toml"

    await config._handle_file_changes([FileChange(Change.modified, policy.policy_path.resolve())])

    assert set(manager.heartflow_chat_list) == {"allowed", "local", "webui"}
    assert set(manager._chat_last_active_at) == {"allowed", "local", "webui"}
    chats["blocked_group"].stop.assert_awaited_once()
    chats["blocked_private"].stop.assert_awaited_once()
    for name in ("allowed", "local", "webui"):
        chats[name].stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_blocked_session_cannot_recreate_runtime(monkeypatch):
    manager = HeartflowManager()
    session = SimpleNamespace(
        platform="qq", account_id="bot", scope="main", is_group_session=True, group_id="blocked", user_id=None
    )
    platform_manager = SimpleNamespace(
        ensure_send_pipeline_ready=AsyncMock(), is_target_allowed=AsyncMock(return_value=False)
    )
    constructor = Mock()
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.get_platform_io_manager", lambda: platform_manager)
    monkeypatch.setattr(
        "src.chat.heart_flow.heartflow_manager.chat_manager.get_session_by_session_id", lambda session_id: session
    )
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.MaisakaHeartFlowChatting", constructor)

    with pytest.raises(PermissionError):
        await manager.get_or_create_heartflow_chat("session")

    constructor.assert_not_called()
    assert not manager.heartflow_chat_list

    platform_manager.is_target_allowed.return_value = True
    constructor.return_value.start = AsyncMock()
    runtime = await manager.get_or_create_heartflow_chat("session")
    assert manager.heartflow_chat_list["session"] is runtime
    runtime.start.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_failure", [False, True])
async def test_offline_closes_chats_after_adapter_unload(monkeypatch, partial_failure):
    manager = HeartflowManager()
    chats = {}
    for name, platform, is_group in (
        ("group", "qq", True),
        ("private", "qq", False),
        ("other_adapter", "other", True),
        ("local", "maisaka_cli", False),
        ("webui", "webui", True),
    ):
        chat = SimpleNamespace(
            chat_stream=SimpleNamespace(
                platform=platform, account_id="bot", scope="main", is_group_session=is_group,
                group_id=name if is_group else None, user_id=name,
            ),
            stop=AsyncMock(),
        )
        chats[name] = chat
        manager.heartflow_chat_list[name] = chat
        manager._touch_chat(name)

    loaded = {"qq_adapter", "other_adapter"}

    async def unload(plugin_ids, *, reason):
        assert reason == "local_operator_offline"
        # 模拟卸载驱动后才移除路由，保证清理使用卸载后的链路状态。
        unloaded = {"qq_adapter"} if partial_failure else set(plugin_ids)
        loaded.difference_update(unloaded)
        return SimpleNamespace(
            unloaded_plugins=unloaded,
            failed_plugins={"other_adapter": "unload failed"} if partial_failure else {},
        )

    runtime_manager = PluginRuntimeManager()
    runtime_manager._started = True
    runtime_manager._builtin_supervisor = SimpleNamespace(
        get_loaded_plugin_ids_by_type=lambda plugin_type: sorted(loaded), unload_plugins=unload,
    )
    platform_manager = SimpleNamespace(
        ensure_send_pipeline_ready=AsyncMock(),
        is_target_allowed=AsyncMock(
            side_effect=lambda route, **kwargs: f"{route.platform}_adapter" in loaded
        ),
    )
    clear_context = AsyncMock()
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.get_platform_io_manager", lambda: platform_manager)
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.heartflow_manager", manager)
    monkeypatch.setattr("src.chat.heart_flow.heartflow_manager.focus_mode_manager.clear_context", clear_context)

    result = await runtime_manager.take_adapters_offline()

    remaining = {"local", "webui"} | ({"other_adapter"} if partial_failure else set())
    assert result.success is not partial_failure
    assert set(result.changed_plugin_ids) == {"qq_adapter", "other_adapter"} - loaded
    assert set(result.pending_plugin_ids) == set(result.changed_plugin_ids)
    assert set(manager.heartflow_chat_list) == remaining
    assert set(manager._chat_last_active_at) == remaining
    for name, chat in chats.items():
        if name in remaining:
            chat.stop.assert_not_awaited()
        else:
            chat.stop.assert_awaited_once()
            assert any(call.args == (name,) for call in clear_context.await_args_list)
