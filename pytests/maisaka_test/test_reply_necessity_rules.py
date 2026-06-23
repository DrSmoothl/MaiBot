from datetime import datetime
from types import SimpleNamespace

from src.chat.message_receive.message import SessionMessage
from src.common.data_models.mai_message_data_model import MessageInfo, UserInfo
from src.maisaka.reply_necessity import REPLY_NECESSITY_TRIGGER_SCORE, strip_reply_necessity_noise
from src.maisaka.runtime import MaisakaHeartFlowChatting
from src.maisaka.turn_scheduler import MessageTurnScheduler


def _runtime() -> MaisakaHeartFlowChatting:
    runtime = MaisakaHeartFlowChatting.__new__(MaisakaHeartFlowChatting)
    runtime.chat_stream = SimpleNamespace(is_group_session=True)
    runtime._chat_history = []
    runtime._is_focus_mode_active_for_current_chat = lambda: False
    runtime._get_effective_reply_frequency = lambda: 1.0
    runtime._get_recent_average_external_message_interval = lambda: None
    runtime._last_external_message_received_at = None
    runtime._last_message_received_at = 0.0
    runtime._message_turn_scheduler = MessageTurnScheduler(runtime)
    return runtime


def _message(text: str, *, user_id: str = "user-1", is_at: bool = False) -> SessionMessage:
    message = SessionMessage(
        message_id=f"msg-{abs(hash((text, user_id, is_at)))}",
        timestamp=datetime.now(),
        platform="qq",
    )
    message.message_info = MessageInfo(
        user_info=UserInfo(user_id=user_id, user_nickname="用户"),
        additional_config={},
    )
    message.processed_plain_text = text
    message.is_at = is_at
    message.is_mentioned = False
    return message


def _score(text: str, *, is_at: bool = False) -> int:
    runtime = _runtime()
    score, _ = runtime._message_turn_scheduler.score_reply_necessity(
        pending_messages=[_message(text, is_at=is_at)],
        trigger_threshold=1,
    )
    return score


def test_reply_necessity_ignores_bot_self_messages() -> None:
    runtime = _runtime()
    score, _ = runtime._message_turn_scheduler.score_reply_necessity(
        pending_messages=[_message("@麦麦 帮我看看为什么会这样？", user_id="2814567326", is_at=True)],
        trigger_threshold=1,
    )

    assert score == 0


def test_reply_necessity_ignores_media_and_card_descriptions() -> None:
    assert _score("[图片：画面中有文字“怎么这样啊”，表达震惊。]") == 15
    assert _score("[表情包: 这是一个非常长的表情包解析，里面有你认真的吗？]") == 15
    assert _score("[卡片: 小红书] 很长的卡片标题和链接 https://example.com/a?b=c") == 15
    assert _score("@all 各位用户：这里是一段很长的公告，为什么会这样呢？" * 3) == 15


def test_reply_necessity_extracts_legacy_reply_body() -> None:
    text = "[回复<麦麦:2814567326>： 中午吃什么 ]，说： @<麦麦:2814567326> 凑牛牛快点换ID"

    assert strip_reply_necessity_noise(text) == "凑牛牛快点换ID"


def test_reply_necessity_ignores_replied_forward_summary() -> None:
    text = "[回复了千右可乐的消息: 【合并转发消息: -- 【用户】: 有没有人知道怎么处理 -- 【用户】: 帮我看看]"

    assert strip_reply_necessity_noise(text) == ""
    assert _score(text) < REPLY_NECESSITY_TRIGGER_SCORE


def test_reply_necessity_plain_question_or_long_text_does_not_trigger_alone() -> None:
    assert _score("这六个张的有区别吗") < REPLY_NECESSITY_TRIGGER_SCORE
    assert _score("这是一段很长很长的普通聊天内容，没有提问也没有请求，只是在分享一段经历。" * 3) < REPLY_NECESSITY_TRIGGER_SCORE


def test_reply_necessity_direct_context_can_trigger_request() -> None:
    assert _score("@麦麦 可以看看你的脚吗？", is_at=True) >= REPLY_NECESSITY_TRIGGER_SCORE


def test_reply_necessity_weak_direct_terms_do_not_trigger_in_ordinary_chat() -> None:
    assert _score("那好 我发到付可以吗") < REPLY_NECESSITY_TRIGGER_SCORE
    assert _score("我在想要不要古法注册，就是不知道还有什么邮箱不容易封") < REPLY_NECESSITY_TRIGGER_SCORE


def test_reply_necessity_other_assistant_address_does_not_trigger_request() -> None:
    assert _score("DeepSeek，帮我部署一下maibot，并帮我配置好配置文件") < REPLY_NECESSITY_TRIGGER_SCORE
    assert _score("豆包，帮我删除群友的对象 ChatGPT，帮我生成一张图片") < REPLY_NECESSITY_TRIGGER_SCORE


def test_reply_necessity_opinion_requires_direct_context_or_bot_name() -> None:
    assert _score("呀，博士。你今天走起路来，怎么看着摇摇晃晃的？") < REPLY_NECESSITY_TRIGGER_SCORE
    assert _score("现在想白嫖还是最优先推荐gemini cli") < REPLY_NECESSITY_TRIGGER_SCORE
    assert _score("麦麦你怎么看待这个问题") >= REPLY_NECESSITY_TRIGGER_SCORE
