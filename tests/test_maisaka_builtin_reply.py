from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.data_models.reply_generation_data_models import ReplyGenerationResult
from src.core.tooling import ToolInvocation
from src.maisaka.builtin_tool.context import BuiltinToolRuntimeContext
from src.maisaka.builtin_tool.reply import handle_tool


@pytest.mark.asyncio
async def test_reply_keeps_anchor_message_when_quote_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    target_message = SimpleNamespace(
        message_id="source-1",
        message_info=SimpleNamespace(
            user_info=SimpleNamespace(
                user_cardname="",
                user_nickname="用户",
                user_id="user-1",
            )
        ),
    )
    reply_result = ReplyGenerationResult()
    reply_result.completion.response_text = "收到"

    class FakeReplyer:
        async def generate_reply_with_context(self, **kwargs: Any) -> tuple[bool, ReplyGenerationResult]:
            captured["replyer_reply_message"] = kwargs["reply_message"]
            return True, reply_result

    async def fake_send_to_target_with_message(**kwargs: Any) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(message_id="sent-1")

    async def fake_post_process_reply_message_sequences_async(_reply_text: str) -> list[MessageSequence]:
        return [MessageSequence([TextComponent("收到")])]

    monkeypatch.setattr("src.maisaka.builtin_tool.reply.replyer_manager.get_replyer", lambda **_kwargs: FakeReplyer())
    monkeypatch.setattr(
        "src.maisaka.builtin_tool.reply.config_module.global_config.chat.enable_reply_quote",
        False,
    )
    monkeypatch.setattr(
        "src.maisaka.builtin_tool.reply.config_module.global_config.bot.nickname",
        "MaiSaka",
    )
    monkeypatch.setattr(
        "src.maisaka.builtin_tool.reply.send_service._send_to_target_with_message",
        fake_send_to_target_with_message,
    )

    runtime = SimpleNamespace(
        session_id="session-1",
        log_prefix="[test]",
        chat_stream=SimpleNamespace(platform="qq"),
        _chat_history=[],
        find_source_message_by_id=lambda message_id: target_message if message_id == "source-1" else None,
    )
    tool_ctx = BuiltinToolRuntimeContext(engine=SimpleNamespace(), runtime=runtime)
    monkeypatch.setattr(
        tool_ctx,
        "post_process_reply_message_sequences_async",
        fake_post_process_reply_message_sequences_async,
    )

    result = await handle_tool(
        tool_ctx,
        ToolInvocation(
            tool_name="reply",
            arguments={"msg_id": "source-1", "set_quote": True},
        ),
    )

    assert result.success is True
    assert captured["set_reply"] is False
    assert captured["reply_message"] is target_message
    assert captured["replyer_reply_message"] is target_message
