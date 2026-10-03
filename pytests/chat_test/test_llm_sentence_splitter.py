"""LLM 断句器测试。"""

import pytest

from src.chat.utils import llm_sentence_splitter


class _Response:
    response = '["今天去上课了，", "然后回来睡觉"]'


class _Client:
    def __init__(self, **_kwargs: object) -> None:
        pass

    async def generate_response(self, *_args: object, **_kwargs: object) -> _Response:
        return _Response()


def _client_returning(payload: str) -> type[_Client]:
    """构造固定返回指定内容的 LLM 客户端替身。"""

    class _FixedClient(_Client):
        async def generate_response(self, *_args: object, **_kwargs: object) -> _Response:
            response = _Response()
            response.response = payload
            return response

    return _FixedClient


@pytest.mark.asyncio
async def test_llm_splitter_preserves_original_text(monkeypatch) -> None:
    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _Client)

    assert await llm_sentence_splitter.split_text_with_llm("今天去上课了，然后回来睡觉") == [
        ("今天去上课了，", ""),
        ("然后回来睡觉", ""),
    ]


@pytest.mark.asyncio
async def test_llm_splitter_rejects_rewritten_text(monkeypatch) -> None:
    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _client_returning('["改写后的文本"]'))

    with pytest.raises(ValueError, match="未完整保留原文"):
        await llm_sentence_splitter.split_text_with_llm("原始文本")


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", ["no_split", '"no_split"', "```json\nno_split\n```"])
async def test_llm_splitter_keeps_text_when_model_declines(monkeypatch, payload: str) -> None:
    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _client_returning(payload))

    assert await llm_sentence_splitter.split_text_with_llm("今天还挺好的") == [("今天还挺好的", "")]


@pytest.mark.asyncio
async def test_llm_splitter_rejects_unknown_string_payload(monkeypatch) -> None:
    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _client_returning('"不需要拆分"'))

    with pytest.raises(ValueError, match="不是合法 JSON 数组"):
        await llm_sentence_splitter.split_text_with_llm("今天还挺好的")
