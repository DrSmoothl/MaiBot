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


@pytest.mark.asyncio
async def test_llm_splitter_preserves_original_text(monkeypatch) -> None:
    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _Client)

    assert await llm_sentence_splitter.split_text_with_llm("今天去上课了，然后回来睡觉") == [
        ("今天去上课了，", ""),
        ("然后回来睡觉", ""),
    ]


@pytest.mark.asyncio
async def test_llm_splitter_rejects_rewritten_text(monkeypatch) -> None:
    class _InvalidClient(_Client):
        async def generate_response(self, *_args: object, **_kwargs: object) -> _Response:
            response = _Response()
            response.response = '["改写后的文本"]'
            return response

    monkeypatch.setattr(llm_sentence_splitter, "LLMServiceClient", _InvalidClient)

    with pytest.raises(ValueError, match="未完整保留原文"):
        await llm_sentence_splitter.split_text_with_llm("原始文本")
