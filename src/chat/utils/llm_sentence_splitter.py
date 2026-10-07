"""基于 LLM 的回复断句。"""

from typing import List, Tuple

import json

from src.common.logger import get_logger
from src.services.llm_service import LLMServiceClient

logger = get_logger("llm_sentence_splitter")

_NO_SPLIT_MARKER = "no_split"


_SPLITTER_PROMPT = """
请不要思考，快速地把下面的回复进行断句，按自然语义拆成若干条适合逐条发送的消息。
要求：
1. 只能返回 JSON 字符串数组，例如 ["第一句。", "第二句？"]，不要 Markdown、解释或代码块。
2. 必须保留原文全部字符、标点、空格和换行，只能在原文已有字符之间选择切分位置，不能改写、增删或调整顺序。
3. 不要把括号、引号、颜文字或代码从中间拆开。
4. 如果不需要拆分，返回"no_split"。

原文：
"""


def _normalize_json_payload(payload: str) -> str:
    """去除模型偶尔添加的 Markdown JSON 代码块包装。"""
    normalized = payload.strip()
    if normalized.startswith("```json") and normalized.endswith("```"):
        return normalized[7:-3].strip()
    if normalized.startswith("```") and normalized.endswith("```"):
        return normalized[3:-3].strip()
    return normalized


def _is_no_split_marker(payload: str) -> bool:
    """判断模型返回的是否为「不拆分」标记，兼容是否被写成 JSON 字符串。"""
    return payload.strip().strip('"').strip() == _NO_SPLIT_MARKER


def _extract_trailing_comma(sentence: str) -> Tuple[str, str]:
    """将段尾逗号移入分隔符，逐条发送时省略，压缩合并时恢复。"""
    content = sentence.rstrip("，, \t\r\n")
    separator = sentence[len(content) :]
    if content.strip() and ("，" in separator or "," in separator):
        return content, separator
    return sentence, ""


async def split_text_with_llm(text: str) -> List[Tuple[str, str]]:
    """调用 LLM 断句，并返回兼容规则断句器的句子元组。"""
    if not text:
        return []

    client = LLMServiceClient(task_name="fast_model", request_type="response.splitter")
    result = await client.generate_response(_SPLITTER_PROMPT + text)
    payload = _normalize_json_payload(result.response)

    # 模型判断不需要拆分时只返回标记，此时作为单独一段，并同样处理段尾逗号
    if _is_no_split_marker(payload):
        return [_extract_trailing_comma(text)]

    try:
        sentences = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("LLM 断句结果不是合法 JSON 数组") from exc

    if isinstance(sentences, str):
        raise ValueError("LLM 断句结果不是合法 JSON 数组")

    if not isinstance(sentences, list) or not sentences or not all(isinstance(item, str) for item in sentences):
        raise ValueError("LLM 断句结果必须是非空字符串数组")
    if sentences == [_NO_SPLIT_MARKER]:
        return [_extract_trailing_comma(text)]

    # 换行不影响消息内容校验，其他字符（包括空格和标点）仍必须完整保留。
    comparison_text = text.replace("\r", "").replace("\n", "")
    reconstructed_text = "".join(sentences).replace("\r", "").replace("\n", "")
    if reconstructed_text != comparison_text:
        # 保留完整性校验，并显示首处差异；repr 让空格和标点差异可见。
        mismatch_index = next(
            (
                index
                for index, (original, returned) in enumerate(zip(comparison_text, reconstructed_text, strict=False))
                if original != returned
            ),
            min(len(comparison_text), len(reconstructed_text)),
        )
        context_start = max(0, mismatch_index - 20)
        context_end = mismatch_index + 21
        raise ValueError(
            "LLM 断句结果未完整保留原文（忽略换行后）："
            f"首个差异位置={mismatch_index}（从 0 开始），"
            f"原文长度={len(comparison_text)}，结果长度={len(reconstructed_text)}，"
            f"原文片段={comparison_text[context_start:context_end]!r}，"
            f"结果片段={reconstructed_text[context_start:context_end]!r}"
        )

    # 完整性检查通过后再分离段尾逗号，避免改变模型输出校验与句内标点。
    return [_extract_trailing_comma(sentence) for sentence in sentences if sentence]
