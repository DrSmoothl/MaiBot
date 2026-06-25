"""测试黑话学习器的独立抽取行为。"""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import json

import pytest

from src.chat.message_receive.message import SessionMessage
from src.common.data_models.llm_service_data_models import LLMResponseResult
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.common.data_models.message_component_data_model import EmojiComponent, MessageSequence, ReplyComponent, TextComponent
from src.learners.jargon_learner import JargonLearner, JargonLearningSourceItem
from src.llm_models.payload_content.message import MessageBuilder, RoleType
from src.llm_models.payload_content.tool_option import ToolCall
from src.maisaka.context.messages import (
    AssistantMessage,
    ReferenceMessage,
    ReferenceMessageType,
    SessionBackedMessage,
    ToolResultMessage,
)


def _make_session_message(message_id: str, text: str, *, is_emoji: bool = False) -> SessionMessage:
    message = SessionMessage(message_id=message_id, timestamp=datetime(2026, 1, 1, 12, 0, 0), platform="qq")
    message.message_info = MessageInfo(user_info=UserInfo(user_id=f"user-{message_id}", user_nickname="用户"))
    message.session_id = "session-a"
    message.is_emoji = is_emoji
    message.processed_plain_text = text
    message.raw_message = MessageSequence([TextComponent(text)])
    return message


@pytest.mark.asyncio
async def test_jargon_learner_processes_all_extracted_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    """黑话学习器应独立处理 LLM 提取出的全部候选。"""

    import src.learners.jargon_learner as jargon_learner_module

    class FakePromptTemplate:
        def add_context(self, key: str, value: object) -> None:
            del key, value

    class FakePromptManager:
        def get_prompt(self, name: str) -> FakePromptTemplate:
            assert name == "learn_jargon"
            return FakePromptTemplate()

        async def render_prompt(self, prompt_template: FakePromptTemplate) -> str:
            del prompt_template
            return "prompt"

    class FakeLearnModel:
        async def generate_response_with_messages(self, builder, options, **kwargs):
            del builder, options, kwargs
            return SimpleNamespace(response="response")

    class FakeJargonMiner:
        session_id = "session-a"
        session_name = "session-a"

        def get_cached_jargons(self):
            return []

    captured_jargon_entries = []

    async def fake_build_multi_learning_messages(self, pending_messages, prompt):
        del self, pending_messages, prompt
        return []

    async def fake_process_jargon_entries(self, jargon_entries, messages, jargon_miner):
        del self, messages, jargon_miner
        captured_jargon_entries.extend(jargon_entries)
        return True

    jargon_entries = [(f"黑话{i}", "1") for i in range(31)]
    monkeypatch.setattr(jargon_learner_module, "prompt_manager", FakePromptManager())
    monkeypatch.setattr(jargon_learner_module, "jargon_learn_model", FakeLearnModel())
    monkeypatch.setattr(
        jargon_learner_module,
        "global_config",
        SimpleNamespace(bot=SimpleNamespace(nickname="麦麦")),
    )
    monkeypatch.setattr(jargon_learner_module, "parse_jargon_response", lambda response: jargon_entries)
    monkeypatch.setattr(JargonLearner, "_build_multi_learning_messages", fake_build_multi_learning_messages)
    monkeypatch.setattr(JargonLearner, "_process_jargon_entries", fake_process_jargon_entries)
    monkeypatch.setattr(JargonLearner, "_log_learning_context_preview", lambda *args, **kwargs: None)

    learner = JargonLearner(session_id="session-a")
    wrote_result = await learner._run_learning_batch(
        [
            JargonLearningSourceItem(
                source_kind="user",
                speaker_kind="USER",
                speaker_name="用户",
                content="测试上下文",
                timestamp=datetime(2026, 1, 1, 12, 0, 0),
            )
        ],
        learning_session_id="session-a",
        jargon_miner=FakeJargonMiner(),
    )

    assert wrote_result is True
    assert captured_jargon_entries == jargon_entries


def test_jargon_learning_sources_keep_assistant_tool_and_filter_emoji() -> None:
    """上线黑话学习应保留 assistant/tool 上下文，并过滤表情包真实消息。"""

    normal_message = _make_session_message("normal", "用户黑话")
    emoji_message = _make_session_message("emoji", "表情包黑话", is_emoji=True)
    emoji_component_message = _make_session_message("emoji-component", "[表情包]组件黑话")
    emoji_component_message.raw_message = MessageSequence(
        [EmojiComponent(binary_hash="emoji-hash", content="[表情包]组件黑话")]
    )
    notice_message = _make_session_message("notice", "[事件-戳一戳]notice黑话")
    notice_message.is_notify = True
    jargon_reference_message = _make_session_message(
        "jargon-reference",
        "[黑话参考]\n以下黑话来自当前上下文中其他用户消息的机械匹配，仅作理解聊天语境的参考：\n1. token：模型调用额度",
    )
    context_messages = [
        ReferenceMessage(
            content="以下黑话来自当前上下文中其他用户消息的机械匹配，仅作理解聊天语境的参考：\n1. token：模型调用额度",
            timestamp=datetime(2026, 1, 1, 11, 59, 0),
            reference_type=ReferenceMessageType.JARGON,
            display_prefix="[黑话参考]",
        ),
        SessionBackedMessage.from_session_message(
            normal_message,
            raw_message=normal_message.raw_message,
            visible_text=normal_message.processed_plain_text or "",
            source_kind="user",
        ),
        SessionBackedMessage.from_session_message(
            emoji_message,
            raw_message=emoji_message.raw_message,
            visible_text=emoji_message.processed_plain_text or "",
            source_kind="user",
        ),
        SessionBackedMessage.from_session_message(
            emoji_component_message,
            raw_message=emoji_component_message.raw_message,
            visible_text=emoji_component_message.processed_plain_text or "",
            source_kind="user",
        ),
        SessionBackedMessage.from_session_message(
            notice_message,
            raw_message=notice_message.raw_message,
            visible_text=notice_message.processed_plain_text or "",
            source_kind="user",
        ),
        SessionBackedMessage.from_session_message(
            jargon_reference_message,
            raw_message=jargon_reference_message.raw_message,
            visible_text=jargon_reference_message.processed_plain_text or "",
            source_kind="user",
        ),
        AssistantMessage(
            content="assistant黑话",
            timestamp=datetime(2026, 1, 1, 12, 1, 0),
            tool_calls=[
                ToolCall(
                    call_id="call-1",
                    func_name="search_tool",
                    args={"query": "toolcall黑话"},
                )
            ],
        ),
        ToolResultMessage(
            content="toolresult黑话",
            timestamp=datetime(2026, 1, 1, 12, 2, 0),
            tool_call_id="call-1",
            tool_name="search_tool",
        ),
        AssistantMessage(
            content="",
            timestamp=datetime(2026, 1, 1, 12, 2, 30),
            tool_calls=[
                ToolCall(
                    call_id="call-wait",
                    func_name="wait",
                    args={"reason": "wait调用黑话"},
                )
            ],
        ),
        ToolResultMessage(
            content="wait结果黑话",
            timestamp=datetime(2026, 1, 1, 12, 2, 40),
            tool_call_id="call-wait",
            tool_name="wait",
        ),
        AssistantMessage(
            content="",
            timestamp=datetime(2026, 1, 1, 12, 3, 0),
            tool_calls=[
                ToolCall(
                    call_id="call-profile",
                    func_name="query_person_profile",
                    args={"person_name": "用户"},
                )
            ],
        ),
        ToolResultMessage(
            content="【人物画像-内部参考】\n画像黑话",
            timestamp=datetime(2026, 1, 1, 12, 4, 0),
            tool_call_id="call-profile",
            tool_name="query_person_profile",
        ),
    ]

    source_items = JargonLearner._extract_learning_sources_from_context(context_messages)
    source_text = "\n".join(item.content for item in source_items)
    speaker_kinds = [item.speaker_kind for item in source_items]

    assert "用户黑话" in source_text
    assert "表情包黑话" not in source_text
    assert "组件黑话" not in source_text
    assert "notice黑话" not in source_text
    assert "模型调用额度" not in source_text
    assert "assistant黑话" in source_text
    assert "[assistant_content]" not in source_text
    assert "toolcall黑话" not in source_text
    assert "toolresult黑话" in source_text
    assert "wait调用黑话" not in source_text
    assert "wait结果黑话" not in source_text
    assert "画像黑话" not in source_text
    assert "query_person_profile" not in source_text
    assert speaker_kinds == ["USER", "ASSISTANT", "TOOL_RESULT"]


def test_jargon_learning_real_message_uses_shared_message_metadata_format() -> None:
    """真实聊天消息复用 planner `<message>` 元信息格式，引用不混入正文。"""

    message = _make_session_message("m-reply", "是不是你调用错误了")
    message.raw_message = MessageSequence(
        [
            ReplyComponent("-95288214"),
            TextComponent("是不是你调用错误了"),
        ]
    )
    visible_text = "21:57:27[msg_id:m-reply][用户][引用消息]-95288214\n[发言内容]是不是你调用错误了"
    context_message = SessionBackedMessage.from_session_message(
        message,
        raw_message=message.raw_message,
        visible_text=visible_text,
        source_kind="user",
    )

    source_items = JargonLearner._extract_learning_sources_from_context([context_message])
    assert len(source_items) == 1

    source_text = JargonLearner._build_learning_source_content(1, source_items[0])
    assert source_text.startswith('<message source_id="1" quote="-95288214" time="12:00:00" user="用户">')
    assert "是不是你调用错误了" in source_text
    assert "[发言内容]" not in source_text
    assert "[引用消息]" not in source_text
    assert "msg_id" not in source_text


@pytest.mark.asyncio
async def test_jargon_learning_session_message_filters_notice_and_standalone_emoji() -> None:
    """直接学习 SessionMessage 时也过滤 notice 和单独表情包。"""

    normal_message = _make_session_message("normal-direct", "直接消息黑话")
    notice_message = _make_session_message("notice-direct", "通知黑话")
    notice_message.is_notify = True
    emoji_component_message = _make_session_message("emoji-direct", "[表情包]直接组件黑话")
    emoji_component_message.raw_message = MessageSequence(
        [ReplyComponent("quoted-id"), EmojiComponent(binary_hash="emoji-direct-hash", content="[表情包]直接组件黑话")]
    )

    source_items = await JargonLearner("session-a")._prepare_learning_source_items(
        [normal_message, notice_message, emoji_component_message]
    )
    source_text = "\n".join(item.content for item in source_items)

    assert len(source_items) == 1
    assert "直接消息黑话" in source_text
    assert "通知黑话" not in source_text
    assert "直接组件黑话" not in source_text


@pytest.mark.asyncio
async def test_jargon_entries_can_use_assistant_or_tool_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """assistant/tool 来源的 source_id 不应因为没有真实消息 ID 被过滤。"""

    captured_entries = []

    class FakeJargonMiner:
        session_id = "session-a"
        session_name = "session-a"

        async def process_extracted_entries(self, entries):
            captured_entries.extend(entries)
            return 1, 0

    monkeypatch.setattr(
        "src.learners.jargon_learner.global_config",
        SimpleNamespace(bot=SimpleNamespace(nickname="麦麦")),
    )

    source_items = [
        JargonLearningSourceItem(
            source_kind="user",
            speaker_kind="USER",
            speaker_name="用户",
            content="用户上下文",
            timestamp=datetime(2026, 1, 1, 12, 0, 0),
            original_message=_make_session_message("m1", "用户上下文"),
        ),
        JargonLearningSourceItem(
            source_kind="assistant",
            speaker_kind="ASSISTANT",
            speaker_name="assistant",
            content="助手黑话",
            timestamp=datetime(2026, 1, 1, 12, 1, 0),
        ),
        JargonLearningSourceItem(
            source_kind="tool",
            speaker_kind="TOOL_RESULT",
            speaker_name="tool",
            content="工具结果黑话",
            timestamp=datetime(2026, 1, 1, 12, 2, 0),
        ),
    ]

    result = await JargonLearner("session-a")._process_jargon_entries(
        [("助手黑话", "2"), ("工具结果黑话", "3")],
        source_items,
        FakeJargonMiner(),
    )

    assert result is True
    assert [entry["content"] for entry in captured_entries] == ["助手黑话", "工具结果黑话"]


def test_jargon_learner_preview_is_replayable_prompt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """黑话抽取日志应保存为可重放的标准 Prompt JSON。"""

    saved_preview_payloads = []

    def fake_save_preview_file(chat_id, category, content):
        saved_preview_payloads.append((chat_id, category, content))
        return tmp_path / "logs" / "maisaka_prompt" / category / chat_id / "1.json"

    monkeypatch.setattr(
        "src.maisaka.display.prompt_cli_renderer.PromptPreviewLogger.save_preview_file",
        fake_save_preview_file,
    )

    messages = [
        MessageBuilder().set_role(RoleType.System).add_text_content("system prompt").build(),
        MessageBuilder().set_role(RoleType.User).add_text_content("source message").build(),
    ]

    JargonLearner("session-a")._log_learning_context_preview(
        messages,
        session_id="session-a",
        source_message_count=1,
        source_type="test",
        output_content='[{"content":"黑话","source_id":"1"}]',
        generation_result=LLMResponseResult(
            response='[{"content":"黑话","source_id":"1"}]',
            model_name="test-model",
        ),
    )

    assert saved_preview_payloads
    assert saved_preview_payloads[0][0] == "session-a"
    assert saved_preview_payloads[0][1] == "jargon_learner"

    payload = json.loads(saved_preview_payloads[0][2])
    assert payload["schema_version"] == 3
    assert payload["request"]["kind"] == "jargon_learner"
    assert payload["messages"]
    assert payload["output"]["title"] == "黑话抽取 LLM 输出"
    assert payload["output"]["content"] == '[{"content":"黑话","source_id":"1"}]'
    assert payload["metadata"]["model_name"] == "test-model"


@pytest.mark.asyncio
async def test_jargon_learning_prompt_uses_compact_source_wrapper() -> None:
    """黑话学习 prompt 应复用 `<message ...>`，不额外包裹真实聊天消息。"""

    messages = await JargonLearner("session-a")._build_multi_learning_messages(
        [
            JargonLearningSourceItem(
                source_kind="planner_user",
                speaker_kind="USER",
                speaker_name="温衿青",
                content='<message msg_id="2002470321" time="16:45:58" user="温衿青">\n我用麦麦就会这样',
                timestamp=datetime(2026, 1, 1, 16, 45, 58),
            ),
            JargonLearningSourceItem(
                source_kind="planner_tool_result",
                speaker_kind="TOOL_RESULT",
                speaker_name="tool_result",
                content="工具结果黑话",
                timestamp=datetime(2026, 1, 1, 16, 46, 0),
            ),
        ],
        "system prompt",
    )

    first_source_text = messages[1].get_text_content()
    second_source_text = messages[2].get_text_content()

    assert first_source_text.startswith('<message source_id="1" time="16:45:58" user="温衿青">')
    assert "<learning-source" not in first_source_text
    assert "msg_id" not in first_source_text
    assert "[speaker:" not in first_source_text
    assert "[name:" not in first_source_text
    assert "[source_kind:" not in first_source_text
    assert "[time:" not in first_source_text
    assert 'source_kind="planner_tool_result"' in second_source_text
    assert "[assistant_content]" not in second_source_text
    assert "</learning-source>" not in second_source_text


def test_planner_log_source_filter_uses_chat_message_whitelist() -> None:
    """planner 离线学习只允许真实聊天消息进入，不采集内部提醒和人物画像。"""

    from scripts.jargon_learn_from_planner_logs import _build_tool_result_content, _build_user_source_item

    timestamp = datetime(2026, 1, 1, 16, 45, 58)
    allowed_message = _build_user_source_item(
        '<message msg_id="2002470321" time="16:45:58" user="温衿青">\n我用麦麦就会这样',
        timestamp,
    )

    assert allowed_message is not None
    assert allowed_message.content.endswith("我用麦麦就会这样")
    assert _build_user_source_item("<system-reminder>\ndeferred tools\n</system-reminder>", timestamp) is None
    assert _build_user_source_item("当前时间：2026-06-24 20:00:00", timestamp) is None
    assert _build_user_source_item("【人物画像-内部参考】\n画像内容", timestamp) is None
    assert (
        _build_user_source_item(
            '<message msg_id="mtm:summary" time="16:45:58">\n[消息类型]复杂消息\n聊天记录摘要',
            timestamp,
        )
        is None
    )
    assert (
        _build_tool_result_content(
            {"role": "tool", "tool_name": "wait", "tool_call_id": "call-wait", "content": "wait结果黑话"},
            timestamp,
        )
        == ""
    )
