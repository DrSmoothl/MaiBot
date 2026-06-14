"""工具记录 payload 历史大字段清理。"""

from __future__ import annotations

from dataclasses import dataclass
from json import dumps, loads
from typing import Any

_OMITTED_TEXT_THRESHOLD = 1024
_INLINE_MEDIA_KEYS = {
    "audio_base64",
    "base64",
    "emoji_base64",
    "image_base64",
}


@dataclass(frozen=True)
class ToolRecordPayloadCleanupResult:
    """单条工具记录 payload 清理结果。"""

    tool_data: str
    changed: bool


def clean_tool_record_payload(raw_tool_data: str) -> ToolRecordPayloadCleanupResult:
    """清理历史工具记录中的完整 prompt 与内联大媒体字段。"""

    try:
        payload = loads(raw_tool_data)
    except Exception:
        return ToolRecordPayloadCleanupResult(tool_data=raw_tool_data, changed=False)

    cleaned_payload, changed = _clean_inline_media(payload)
    if not changed:
        return ToolRecordPayloadCleanupResult(tool_data=raw_tool_data, changed=False)

    cleaned_tool_data = dumps(cleaned_payload, ensure_ascii=False, separators=(",", ":"))
    if len(cleaned_tool_data) >= len(raw_tool_data):
        return ToolRecordPayloadCleanupResult(tool_data=raw_tool_data, changed=False)
    return ToolRecordPayloadCleanupResult(tool_data=cleaned_tool_data, changed=True)


def _build_omitted_marker(value: str) -> str:
    return f"[历史工具记录大字段已省略，原始长度 {len(value)} 字符]"


def _clean_monitor_detail(detail: dict[str, Any]) -> bool:
    changed = False
    request_messages = detail.pop("request_messages", None)
    if isinstance(request_messages, list):
        detail["request_message_count"] = len(request_messages)
        detail["prompt_omitted"] = True
        changed = True

    if "prompt_text" in detail:
        detail.pop("prompt_text", None)
        detail["prompt_omitted"] = True
        changed = True

    return changed


def _clean_inline_media(value: Any) -> tuple[Any, bool]:
    if isinstance(value, dict):
        changed = False
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key)
            if normalized_key == "monitor_detail" and isinstance(item, dict):
                nested = dict(item)
                nested_changed = _clean_monitor_detail(nested)
                nested, media_changed = _clean_inline_media(nested)
                cleaned[normalized_key] = nested
                changed = changed or nested_changed or media_changed
                continue

            if (
                normalized_key in _INLINE_MEDIA_KEYS
                and isinstance(item, str)
                and len(item) > _OMITTED_TEXT_THRESHOLD
            ):
                cleaned[normalized_key] = _build_omitted_marker(item)
                changed = True
                continue

            nested, nested_changed = _clean_inline_media(item)
            cleaned[normalized_key] = nested
            changed = changed or nested_changed
        return cleaned, changed

    if isinstance(value, list):
        changed = False
        cleaned_items: list[Any] = []
        for item in value:
            nested, nested_changed = _clean_inline_media(item)
            cleaned_items.append(nested)
            changed = changed or nested_changed
        return cleaned_items, changed

    if isinstance(value, str) and value.startswith("data:image/") and len(value) > _OMITTED_TEXT_THRESHOLD:
        return _build_omitted_marker(value), True

    return value, False
