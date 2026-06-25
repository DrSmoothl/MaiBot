from datetime import datetime

from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.data_models.llm_service_data_models import LLMResponseResult
from src.config.config import global_config
from src.llm_models.payload_content.message import MessageBuilder, RoleType
from src.maisaka.context.messages import SessionBackedMessage
from src.maisaka.display.prompt_preview_logger import PromptPreviewLogger
from src.maisaka.memory.mid_term import (
    MID_TERM_MEMORY_COMPONENT_TYPE,
    _parse_summary_response,
    _save_mid_term_memory_prompt_preview,
    build_mid_term_memory_complex_message,
)


def test_parse_summary_response_uses_summary_and_recall_cues() -> None:
    payload = (
        '{"summary":"大家决定中期摘要只通过 embedding 自动召回。",'
        '"recall_cues":["询问中期摘要为什么不进 prompt 时关联这段信息"]}'
    )

    summary = _parse_summary_response(payload)

    assert summary is not None
    assert summary.summary == "大家决定中期摘要只通过 embedding 自动召回。"
    assert summary.recall_cues == ["询问中期摘要为什么不进 prompt 时关联这段信息"]


def test_parse_summary_response_keeps_legacy_payload_compatible() -> None:
    payload = (
        '{"long_summary":"旧格式完整摘要。","brief":"旧格式短摘要。",'
        '"keywords":["中期摘要"],"match_segments":["旧格式匹配段"]}'
    )

    summary = _parse_summary_response(payload)

    assert summary is not None
    assert summary.summary == "旧格式完整摘要。"
    assert summary.recall_cues == ["旧格式匹配段"]


def test_mid_term_memory_payload_does_not_write_removed_summary_fields() -> None:
    summary = _parse_summary_response(
        '{"summary":"完整摘要内容。","recall_cues":["之后聊召回格式时需要这段信息"]}'
    )
    assert summary is not None

    message = build_mid_term_memory_complex_message(
        summary,
        time_range="2026-06-24 10:00:00 ~ 2026-06-24 10:10:00",
        participants=["用户"],
        source_messages=[
            SessionBackedMessage(
                raw_message=MessageSequence([TextComponent("原始消息")]),
                visible_text="用户：原始消息",
                timestamp=datetime.now(),
                source_kind="user",
            )
        ],
        recall_cue_embeddings=[
            {
                "text": "之后聊召回格式时需要这段信息",
                "embedding": [0.0, 1.0],
                "model_name": "fake-embedding",
            }
        ],
    )

    component = message.raw_message.components[0]
    assert component.data["type"] == MID_TERM_MEMORY_COMPONENT_TYPE
    payload = component.data["data"]
    assert payload["summary"] == "完整摘要内容。"
    assert payload["recall_cues"][0]["text"] == "之后聊召回格式时需要这段信息"
    assert "brief" not in payload
    assert "long_summary" not in payload
    assert "keywords" not in payload


def test_mid_term_memory_prompt_preview_saved_as_own_category(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(global_config.debug, "show_maisaka_thinking", True)
    monkeypatch.setattr(PromptPreviewLogger, "_BASE_DIR", tmp_path)

    _save_mid_term_memory_prompt_preview(
        [
            MessageBuilder()
            .set_role(RoleType.System)
            .add_text_content("请生成中期摘要")
            .build()
        ],
        result=LLMResponseResult(
            response='{"summary":"摘要","recall_cues":["线索"]}',
            model_name="fake-model",
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
        ),
        session_id="session-1",
        time_range="2026-06-24 10:00:00 ~ 2026-06-24 10:10:00",
        participants=["用户"],
        log_prefix="[test]",
    )

    saved_files = list((tmp_path / "mid_term_memory").rglob("*.json"))
    assert len(saved_files) == 1
    assert '"kind": "mid_term_memory"' in saved_files[0].read_text(encoding="utf-8")
