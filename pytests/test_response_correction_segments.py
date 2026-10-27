"""纠错消息在回复压缩后仍独立发送。"""

from types import SimpleNamespace
from typing import Any, Tuple

import pytest

from src.chat.utils import utils
from src.chat.utils.utils import ProcessedResponseSegment, _merge_processed_segments_to_max_count


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["rule", "llm"])
@pytest.mark.parametrize("quote", [False, True])
async def test_correction_stays_separate_through_post_processing(monkeypatch, mode: str, quote: bool) -> None:
    sentences = [(text, "。") for text in ["今天先休息吧", "我也准备睡了", "明天见", "到时候一起吃饭呀"]]

    class TypoGenerator:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def create_typo_sentence(self, sentence: str) -> Tuple[str, str]:
            return ("明天件", "见") if sentence == "明天见" else (sentence, "")

    async def split_with_llm(_text: str):
        return sentences

    monkeypatch.setattr(
        utils,
        "global_config",
        SimpleNamespace(
            response_post_process=SimpleNamespace(enable_response_post_process=True),
            response_splitter=SimpleNamespace(
                mode=mode,
                enable=True,
                enable_kaomoji_protection=True,
                max_length=512,
                max_sentence_num=9,
                max_split_num=3,
            ),
            chinese_typo=SimpleNamespace(
                enable=True,
                enable_correction_quote=quote,
                correction_quote_probability=1.0,
                error_rate=0.01,
                min_freq=9,
                tone_error_rate=0.1,
                word_replace_rate=0.006,
            ),
        ),
    )
    monkeypatch.setattr(utils, "ChineseTypoGenerator", TypoGenerator)
    monkeypatch.setattr(utils.random, "random", lambda: 0.0)
    monkeypatch.setattr(utils, "_split_into_sentence_segments", lambda _text: sentences)
    monkeypatch.setattr(utils, "split_text_with_llm", split_with_llm)

    result = await utils.process_llm_response_segments_async("今天先休息吧。我也准备睡了。明天见。到时候一起吃饭呀")

    assert [segment.text for segment in result] == [
        "今天先休息吧。我也准备睡了。明天件",
        "见",
        "到时候一起吃饭呀",
    ]
    assert [segment.quote_previous for segment in result] == [False, quote, False]
    assert [segment.is_correction for segment in result] == [False, True, False]


@pytest.mark.parametrize("max_count", [1, 3])
def test_multiple_corrections_keep_boundaries_when_limit_is_too_small(max_count: int) -> None:
    segments = [
        ProcessedResponseSegment("明天件", separator="。"),
        ProcessedResponseSegment("见", quote_previous=True, is_correction=True),
        ProcessedResponseSegment("一起次饭", separator="。"),
        ProcessedResponseSegment("吃", quote_previous=True, is_correction=True),
        ProcessedResponseSegment("到时候联系"),
    ]

    assert _merge_processed_segments_to_max_count(segments, max_count) == segments


def test_regular_sentences_still_merge_with_punctuation() -> None:
    segments = [ProcessedResponseSegment(text, separator="。") for text in ["一", "二", "三", "四", "五"]]

    result = _merge_processed_segments_to_max_count(segments, 3)

    assert [segment.text for segment in result] == ["一。二", "三。四", "五"]
    assert not any(segment.quote_previous or segment.is_correction for segment in result)


def test_quote_previous_alone_also_keeps_message_separate() -> None:
    segments = [
        ProcessedResponseSegment("明天件"),
        ProcessedResponseSegment("见", quote_previous=True),
        ProcessedResponseSegment("一起吃饭呀"),
    ]

    assert _merge_processed_segments_to_max_count(segments, 1) == segments
