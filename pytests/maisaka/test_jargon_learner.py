from datetime import datetime
from json import dumps
from types import SimpleNamespace

import pytest

from scripts.jargon_learn_from_planner_logs import build_planner_candidate
from src.common.data_models.message_component_data_model import EmojiComponent, MessageSequence, TextComponent
from src.learners.jargon_learner import JargonLearner, JargonLearningSourceItem
from src.llm_models.payload_content.context_item import (
    AssistantMessageItem,
    ContextItemMeta,
    ContextTextPart,
    FunctionCallOutputItem,
    ReasoningItem,
    ReasoningRepresentation,
    UserMessageItem,
    get_item_text,
)
from src.llm_models.request_snapshot import serialize_context_items_snapshot
from src.maisaka.context.messages import ModelOutputContextMessage, SessionBackedMessage, ToolResultMessage


def test_extract_learning_sources_excludes_all_model_output_items() -> None:
    """模型正文和内部推理都不能进入黑话学习素材。"""

    reasoning_message = ModelOutputContextMessage(
        output_item=ReasoningItem(
            meta=ContextItemMeta.create(),
            text_parts=("这是内部推理",),
            representation=ReasoningRepresentation.RAW_TEXT,
        )
    )
    assistant_message = ModelOutputContextMessage(
        output_item=AssistantMessageItem(
            meta=ContextItemMeta.create(),
            parts=(ContextTextPart("这是最终正文"),),
        )
    )

    sources = JargonLearner._extract_learning_sources_from_context(
        [reasoning_message, assistant_message]
    )

    assert sources == []


def test_planner_offline_learning_reads_v5_request_items(tmp_path) -> None:
    """离线黑话学习脚本应读取 v5 Items，并忽略所有模型输出。"""

    timestamp = datetime(2026, 8, 5, 2, 14, 8)
    request_items = [
        UserMessageItem(
            meta=ContextItemMeta.create(timestamp=timestamp),
            parts=(
                ContextTextPart(
                    '<message msg_id="message-1" time="02:14:00" user="测试用户">这是什么梗'
                ),
            ),
        ),
        ReasoningItem(
            meta=ContextItemMeta.create(timestamp=timestamp),
            text_parts=("不能进入学习素材的内部推理",),
            representation=ReasoningRepresentation.RAW_TEXT,
        ),
        AssistantMessageItem(
            meta=ContextItemMeta.create(timestamp=timestamp),
            parts=(ContextTextPart("这是可见回复正文"),),
        ),
        FunctionCallOutputItem(
            meta=ContextItemMeta.create(logical_turn_id="legacy-turn-1", timestamp=timestamp),
            call_id="call-1",
            tool_name="fetch_new_message",
            output="新消息内容",
        ),
    ]
    planner_path = tmp_path / "1785876848000.json"
    planner_path.write_text(
        dumps(
            {
                "schema_version": 5,
                "request_items": serialize_context_items_snapshot(request_items),
                "output_items": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    candidate = build_planner_candidate("test-session", planner_path, 0)

    assert candidate is not None
    assert candidate.message_ids == {"message-1"}
    assert [source.content for source in candidate.source_items] == [
        '<message msg_id="message-1" time="02:14:00" user="测试用户">\n这是什么梗',
    ]


@pytest.mark.asyncio
async def test_learning_request_filters_assistant_sources_and_reindexes() -> None:
    """过滤后的编号必须与缓存匹配和证据来源一致。"""

    learner = JargonLearner("test-session")
    sources = [
        JargonLearningSourceItem(kind, speaker, "测试", content, datetime.now())
        for kind, speaker, content in [
            ("assistant", "ASSISTANT", "内部分析"),
            ("user", "ASSISTANT", "机器人正文"),
            ("guided_reply", "USER", "引导回复"),
            ("outbound_send", "USER", "主动发送"),
            ("planner_assistant_visible", "ASSISTANT", "历史机器人发言"),
            ("tool", "TOOL_RESULT", "工具结果"),
            ("planner_tool_result", "USER", "旧工具结果"),
            ("tool_result_media", "USER", "工具图片"),
            ("user", "USER", "[表情包: 得意]"),
            ("user", "USER", '<message group_card="群名片" user="用户">[表情包: 得意]yyds'),
        ]
    ]
    prepared = await learner._prepare_learning_source_items(sources)
    assert len(prepared) == 1
    assert "yyds" in prepared[0].content
    assert "group_card" not in prepared[0].content
    assert "表情包" not in prepared[0].content

    request = await learner._build_multi_learning_messages(sources, "提取黑话")
    assert len(request) == 3
    assert [type(item).__name__ for item in request] == ["SystemMessageItem", "UserMessageItem", "UserMessageItem"]
    content = get_item_text(request[1])
    assert 'source_id="1"' in content
    assert "yyds" in content
    for source in sources[:-2]:
        assert source.content not in content



@pytest.mark.asyncio
async def test_learning_filters_bot_session_messages_before_processing(monkeypatch) -> None:
    """机器人消息在转换和处理前就应被过滤。"""

    monkeypatch.setattr("src.learners.jargon_learner.is_bot_self", lambda platform, user_id: user_id == "bot")
    bot_message = SimpleNamespace(
        platform="qq",
        message_info=SimpleNamespace(user_info=SimpleNamespace(user_id="bot")),
    )
    learner = JargonLearner("test-session")
    assert await learner._prepare_learning_source_items([bot_message]) == []

    wrapped = JargonLearningSourceItem(
        "user", "USER", "机器人", "机器人发言", datetime.now(), bot_message
    )
    assert await learner._prepare_learning_source_items([wrapped]) == []


@pytest.mark.parametrize("has_text", [True, False])
def test_context_learning_removes_emoji_components_and_group_card(monkeypatch, has_text) -> None:
    monkeypatch.setattr("src.learners.jargon_learner.is_bot_self", lambda platform, user_id: False)
    components = [EmojiComponent(binary_hash="emoji", content="[表情包: 标签]")]
    if has_text:
        components.append(TextComponent("保留用户正文"))
    original = SimpleNamespace(
        platform="qq", message_id="user-1", is_notify=False,
        timestamp=datetime.now(), raw_message=MessageSequence(components),
        message_info=SimpleNamespace(user_info=SimpleNamespace(
            user_id="user", user_nickname="用户昵称", user_cardname="不该显示的群名片",
        )),
    )
    message = SessionBackedMessage(
        raw_message=original.raw_message, visible_text="[表情包: 标签]保留用户正文" if has_text else "[表情包: 标签]",
        timestamp=original.timestamp, original_message=original,
    )
    sources = JargonLearner._extract_learning_sources_from_context([message])
    if not has_text:
        assert sources == []
        return
    assert len(sources) == 1
    assert "保留用户正文" in sources[0].content
    assert "用户昵称" in sources[0].content
    assert "表情包" not in sources[0].content
    assert "群名片" not in sources[0].content
    assert len(original.raw_message.components) == 2


def test_context_learning_excludes_all_tool_results() -> None:
    tool = ToolResultMessage(
        content="工具结果", timestamp=datetime.now(), tool_call_id="call-1",
        logical_turn_id="turn-1", tool_name="search",
    )
    media = SessionBackedMessage(
        raw_message=MessageSequence([]), visible_text="工具图片",
        timestamp=datetime.now(), source_kind="tool_result_media",
    )
    assert JargonLearner._extract_learning_sources_from_context([tool, media]) == []


@pytest.mark.parametrize("body,expected", [
    ("[表情包: 得意]", ""),
    ("[表情包: 得意]  [表情包: 吃瓜]", ""),
    ("[表情包: 得意]继续聊天[表情包: 吃瓜]", "继续聊天"),
    ("[消息类型]表情包", ""),
])
def test_clean_rendered_user_message(body, expected) -> None:
    text = f'<message user="用户" group_card="不应学习的名片">\n{body}'
    result = JargonLearner._clean_learning_content(text)
    if expected:
        assert result == f'<message user="用户">\n{expected}'
    else:
        assert result == ""
