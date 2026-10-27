from datetime import datetime
from typing import List
from unittest.mock import AsyncMock, Mock

import pytest

from src.chat.message_receive.message import SessionMessage
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.common.data_models.message_component_data_model import (
    ForwardComponent,
    ForwardNodeComponent,
    MessageSequence,
    ReplyComponent,
    StandardMessageComponents,
    TextComponent,
)
from src.learners import expression_learner
from src.learners.expression_learner import ExpressionLearner
from src.llm_models.payload_content.context_item import get_item_text


def make_message(components: List[StandardMessageComponents]) -> SessionMessage:
    message = SessionMessage("source", datetime(2026, 10, 24), "test")
    message.message_info = MessageInfo(UserInfo("human", "测试用户"))
    message.raw_message = MessageSequence(components)
    message.processed_plain_text = "保留给普通聊天的完整引用上下文"
    return message


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted_sender", ["bot", "other-user"])
async def test_learning_drops_quote_and_preserves_own_text(
    monkeypatch: pytest.MonkeyPatch, quoted_sender: str
) -> None:
    monkeypatch.setattr(expression_learner, "is_bot_self", lambda platform, user_id: user_id == "bot")
    reply = ReplyComponent("original", "你的词汇量就剩这一个动词了", quoted_sender, "引用作者")
    original = make_message([reply, TextComponent("给我看看吧")])
    learner = ExpressionLearner("session")
    prepared = learner._prepare_learning_messages([original])
    request = await learner._build_multi_learning_messages(prepared, "学习正文")

    assert get_item_text(request[1]).endswith("[content]\n给我看看吧")
    assert "词汇量" not in "\n".join(map(get_item_text, request))
    assert learner._filter_expressions([("提出请求", "给我看看吧", "1")], prepared) == [
        ("提出请求", "给我看看吧")
    ]
    assert original.raw_message.components[0] is reply
    assert len(original.raw_message.components) == 2
    assert original.processed_plain_text == "保留给普通聊天的完整引用上下文"


@pytest.mark.asyncio
async def test_empty_quote_source_is_rejected_without_shifting_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(expression_learner, "is_bot_self", lambda platform, user_id: False)
    learner = ExpressionLearner("session")
    prepared = learner._prepare_learning_messages([
        make_message([ReplyComponent("missing-original", "妙得很")]),
        make_message([TextComponent("真的不错")]),
    ])
    request = await learner._build_multi_learning_messages(prepared, "学习正文")

    assert len(request) == 3
    assert "[id:2]" in get_item_text(request[1])
    assert learner._filter_expressions([
        ("表示惊叹", "妙得很", "1"),
        ("表示赞同", "真的不错", "2"),
    ], prepared) == [("表示赞同", "真的不错")]


@pytest.mark.asyncio
async def test_forwarded_nodes_are_dropped_but_literal_quote_text_is_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(expression_learner, "is_bot_self", lambda platform, user_id: False)
    forwarded = ForwardNodeComponent([
        ForwardComponent("麦麦", "forwarded", [TextComponent("转发内容不能学习")], user_id="bot")
    ])
    own_text = "我自己写的[回复了某人的消息: 示例]"
    original = make_message([forwarded, TextComponent(own_text)])
    learner = ExpressionLearner("session")
    prepared = learner._prepare_learning_messages([original])
    request = await learner._build_multi_learning_messages(prepared, "学习正文")

    assert get_item_text(request[1]).endswith(own_text)
    assert "转发内容不能学习" not in "\n".join(map(get_item_text, request))
    assert original.raw_message.components[0] is forwarded


@pytest.mark.asyncio
async def test_quote_only_batch_does_not_call_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(expression_learner, "is_bot_self", lambda platform, user_id: False)
    monkeypatch.setattr(expression_learner.prompt_manager, "get_prompt", Mock(return_value=Mock()))
    monkeypatch.setattr(expression_learner.prompt_manager, "render_prompt", AsyncMock(return_value="学习正文"))
    generate = AsyncMock()
    monkeypatch.setattr(expression_learner.express_learn_model, "generate_response_with_context", generate)
    learner = ExpressionLearner("session")

    result = await learner._run_learning_batch(
        [make_message([ReplyComponent("missing-original", "妙得很")])],
        learning_session_id="session",
    )

    assert result is False
    assert learner.learning_status == "no_results"
    generate.assert_not_awaited()
