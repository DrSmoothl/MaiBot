"""权限拒绝的插件命令经过统一结果处理后只写入一次。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncio
import pytest


@pytest.fixture(scope="module")
def command_modules(tmp_path_factory):
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("MAIBOT_INSTANCE_DIR", str(tmp_path_factory.mktemp("command-instance")))
        from src.chat.message_receive import bot
        from src.services import send_service

        yield bot, send_service


@pytest.mark.parametrize("silent", [False, True])
def test_denied_command_is_stored_once(command_modules, monkeypatch, silent):
    bot, send_service = command_modules
    message = SimpleNamespace(
        processed_plain_text="/restricted",
        platform="webui",
        session_id="permission-test",
        is_command=False,
        message_info=SimpleNamespace(
            user_info=SimpleNamespace(user_id="user"), additional_config={}
        ),
    )
    executor = AsyncMock()
    command = SimpleNamespace(plugin_name="test-plugin", name="restricted", permission="operator")
    monkeypatch.setattr(bot.component_query_service, "find_command_by_text", lambda text: (executor, {}, command))
    monkeypatch.setattr(bot.global_announcement_manager, "get_disabled_chat_commands", lambda session_id: [])
    monkeypatch.setattr(bot, "is_command_disabled", lambda command_id: False)
    monkeypatch.setattr(bot, "has_command_permission", lambda *args, **kwargs: False)
    monkeypatch.setattr(bot.global_config.plugin, "silent_permission_denied", silent)
    store = AsyncMock()
    reply = AsyncMock()
    monkeypatch.setattr(bot.MessageUtils, "store_message_to_db_async", store)
    monkeypatch.setattr(send_service, "text_to_stream", reply)
    handler = bot.ChatBot.__new__(bot.ChatBot)

    async def process():
        is_command, result, continue_process = await handler._process_commands(message)
        assert is_command and not continue_process
        assert await handler._handle_command_processing_result(message, result, continue_process)

    asyncio.run(process())
    store.assert_awaited_once_with(message)
    executor.assert_not_awaited()
    assert message.is_command
    assert message.message_info.additional_config["intercept_message_level"] == 1
    if silent:
        reply.assert_not_awaited()
    else:
        reply.assert_awaited_once_with("你没有权限使用此命令。", message.session_id, storage_message=False)
