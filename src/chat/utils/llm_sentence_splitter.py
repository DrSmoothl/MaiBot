"""基于 LLM 的回复断句。"""

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


async def split_text_with_llm(text: str) -> list[tuple[str, str]]:
    """调用 LLM 断句，并返回兼容规则断句器的句子元组。"""
    if not text:
        return []

    client = LLMServiceClient(task_name="utils", request_type="response.splitter")
    result = await client.generate_response(_SPLITTER_PROMPT + text)
    payload = _normalize_json_payload(result.response)

    # 模型判断不需要拆分时只返回标记，此时保持原文作为单独一段
    if _is_no_split_marker(payload):
        return [(text, "")]

    try:
        sentences = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("LLM 断句结果不是合法 JSON 数组") from exc

    if isinstance(sentences, str):
        raise ValueError("LLM 断句结果不是合法 JSON 数组")

    if not isinstance(sentences, list) or not sentences or not all(isinstance(item, str) for item in sentences):
        raise ValueError("LLM 断句结果必须是非空字符串数组")
    if "".join(sentences) != text:
        raise ValueError("LLM 断句结果未完整保留原文")

    return [(sentence, "") for sentence in sentences if sentence]
