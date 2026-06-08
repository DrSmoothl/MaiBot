from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Optional

from json_repair import repair_json

import json

from src.common.logger import get_logger
from src.common.prompt_i18n import load_prompt
from src.config.config import global_config

logger = get_logger("behavior_scenario")

ScenarioAgentRunner = Callable[[str], Awaitable[str]]


@dataclass(frozen=True)
class BehaviorScenarioProfile:
    """行为表现选择前的场景画像。"""

    summary: str = ""
    user_intent: str = ""
    conversation_phase: str = ""
    domain_tags: list[str] = field(default_factory=list)
    behavior_needs: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    avoid_behaviors: list[str] = field(default_factory=list)
    retrieval_query: str = ""
    confidence: float = 0.0

    @property
    def has_signal(self) -> bool:
        return any(
            [
                self.summary,
                self.user_intent,
                self.conversation_phase,
                self.domain_tags,
                self.behavior_needs,
                self.risk_flags,
                self.avoid_behaviors,
                self.retrieval_query,
            ]
        )

    def to_retrieval_text(self, context_text: str = "") -> str:
        parts = [
            self.summary,
            self.user_intent,
            self.conversation_phase,
            " ".join(self.domain_tags),
            " ".join(self.behavior_needs),
            " ".join(self.risk_flags),
            " ".join(self.avoid_behaviors),
            self.retrieval_query,
            context_text,
        ]
        return "\n".join(part for part in parts if str(part or "").strip())

    def to_prompt_text(self) -> str:
        if not self.has_signal:
            return "无可用场景画像。"
        return json.dumps(
            {
                "summary": self.summary,
                "user_intent": self.user_intent,
                "conversation_phase": self.conversation_phase,
                "domain_tags": self.domain_tags,
                "behavior_needs": self.behavior_needs,
                "risk_flags": self.risk_flags,
                "avoid_behaviors": self.avoid_behaviors,
                "retrieval_query": self.retrieval_query,
                "confidence": self.confidence,
            },
            ensure_ascii=False,
            indent=2,
        )

    def to_learning_start_text(self, *, max_length: int = 150) -> str:
        """将场景画像压缩成可写入行为模式 trigger 的统一 start。"""

        if not self.has_signal:
            return ""

        parts = [
            self.summary,
            self.user_intent,
            self.conversation_phase,
            "、".join(self.domain_tags[:3]),
            "、".join(self.behavior_needs[:3]),
        ]
        scene_start = "；".join(part for part in parts if str(part or "").strip()).strip()
        if not scene_start:
            scene_start = self.retrieval_query.strip()
        if len(scene_start) <= max_length:
            return scene_start
        return scene_start[:max_length].rstrip()


@dataclass(frozen=True)
class BehaviorScenarioSegment:
    """一次行为学习窗口中可独立学习的场景片段。"""

    segment_id: str
    title: str
    source_ids: list[str] = field(default_factory=list)
    profile: BehaviorScenarioProfile = field(default_factory=BehaviorScenarioProfile)

    @property
    def has_signal(self) -> bool:
        return bool(self.segment_id and self.profile.has_signal)

    def to_prompt_payload(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "title": self.title,
            "source_ids": self.source_ids,
            "profile": {
                "summary": self.profile.summary,
                "user_intent": self.profile.user_intent,
                "conversation_phase": self.profile.conversation_phase,
                "domain_tags": self.profile.domain_tags,
                "behavior_needs": self.profile.behavior_needs,
                "risk_flags": self.profile.risk_flags,
                "avoid_behaviors": self.profile.avoid_behaviors,
                "retrieval_query": self.profile.retrieval_query,
                "confidence": self.profile.confidence,
            },
            "scene_start": self.profile.to_learning_start_text(),
        }


def _strip_json_code_fence(raw_response: str) -> str:
    normalized_response = raw_response.strip()
    if not normalized_response.startswith("```"):
        return normalized_response

    lines = normalized_response.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return normalized_response


def _coerce_string_list(raw_value: Any, *, max_items: int = 8) -> list[str]:
    if isinstance(raw_value, list):
        raw_items = raw_value
    elif raw_value is None:
        raw_items = []
    else:
        raw_items = [raw_value]

    values: list[str] = []
    for raw_item in raw_items:
        value = " ".join(str(raw_item or "").split()).strip()
        if not value or value in values:
            continue
        values.append(value)
        if len(values) >= max_items:
            break
    return values


def _coerce_float(raw_value: Any) -> float:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, value))


def _coerce_segment_id(raw_value: Any, *, fallback_index: int) -> str:
    segment_id = " ".join(str(raw_value or "").split()).strip()
    if segment_id:
        return segment_id[:40]
    return f"s{fallback_index}"


def _coerce_source_ids(raw_value: Any, *, max_items: int = 24) -> list[str]:
    raw_items = raw_value if isinstance(raw_value, list) else [raw_value]
    source_ids: list[str] = []
    for raw_item in raw_items:
        if isinstance(raw_item, str) and "," in raw_item:
            split_items = raw_item.split(",")
        else:
            split_items = [raw_item]
        for split_item in split_items:
            source_id = str(split_item or "").strip()
            if source_id and source_id not in source_ids:
                source_ids.append(source_id)
                if len(source_ids) >= max_items:
                    return source_ids
    return source_ids


def _profile_from_mapping(parsed_response: dict[str, Any]) -> BehaviorScenarioProfile:
    return BehaviorScenarioProfile(
        summary=" ".join(str(parsed_response.get("summary") or "").split()).strip(),
        user_intent=" ".join(str(parsed_response.get("user_intent") or "").split()).strip(),
        conversation_phase=" ".join(str(parsed_response.get("conversation_phase") or "").split()).strip(),
        domain_tags=_coerce_string_list(parsed_response.get("domain_tags")),
        behavior_needs=_coerce_string_list(parsed_response.get("behavior_needs")),
        risk_flags=_coerce_string_list(parsed_response.get("risk_flags")),
        avoid_behaviors=_coerce_string_list(parsed_response.get("avoid_behaviors")),
        retrieval_query=" ".join(str(parsed_response.get("retrieval_query") or "").split()).strip(),
        confidence=_coerce_float(parsed_response.get("confidence")),
    )


def parse_behavior_scenario_response(response: str) -> BehaviorScenarioProfile:
    """解析场景分析模型返回的 JSON。"""

    normalized_response = _strip_json_code_fence(response or "")
    if not normalized_response:
        return BehaviorScenarioProfile()

    try:
        parsed_response = json.loads(repair_json(normalized_response))
    except Exception:
        logger.warning(f"行为表现情景画像解析失败: {normalized_response!r}")
        return BehaviorScenarioProfile()

    if not isinstance(parsed_response, dict):
        return BehaviorScenarioProfile()

    if isinstance(parsed_response.get("segments"), list):
        segments = parse_behavior_scenario_segments_response(response)
        return segments[0].profile if segments else BehaviorScenarioProfile()

    return _profile_from_mapping(parsed_response)


def parse_behavior_scenario_segments_response(response: str) -> list[BehaviorScenarioSegment]:
    """解析场景分析模型返回的多场景片段。"""

    normalized_response = _strip_json_code_fence(response or "")
    if not normalized_response:
        return []

    try:
        parsed_response = json.loads(repair_json(normalized_response))
    except Exception:
        logger.warning(f"行为表现多场景片段解析失败: {normalized_response!r}")
        return []

    if isinstance(parsed_response, dict) and isinstance(parsed_response.get("segments"), list):
        raw_segments = parsed_response.get("segments") or []
    elif isinstance(parsed_response, list):
        raw_segments = parsed_response
    elif isinstance(parsed_response, dict):
        raw_segments = [
            {
                "segment_id": "s1",
                "title": parsed_response.get("summary") or "主场景",
                "source_ids": parsed_response.get("source_ids") or [],
                "profile": parsed_response,
            }
        ]
    else:
        return []

    segments: list[BehaviorScenarioSegment] = []
    seen_ids: set[str] = set()
    for index, raw_segment in enumerate(raw_segments[:3], start=1):
        if not isinstance(raw_segment, dict):
            continue
        raw_profile = raw_segment.get("profile")
        if not isinstance(raw_profile, dict):
            raw_profile = raw_segment
        profile = _profile_from_mapping(raw_profile)
        if not profile.has_signal:
            continue
        segment_id = _coerce_segment_id(raw_segment.get("segment_id") or raw_segment.get("id"), fallback_index=index)
        if segment_id in seen_ids:
            segment_id = f"{segment_id}_{index}"
        seen_ids.add(segment_id)
        title = " ".join(str(raw_segment.get("title") or profile.summary or segment_id).split()).strip()
        segments.append(
            BehaviorScenarioSegment(
                segment_id=segment_id,
                title=title[:120],
                source_ids=_coerce_source_ids(raw_segment.get("source_ids")),
                profile=profile,
            )
        )

    return segments


class BehaviorScenarioAnalyzer:
    """用 LLM 将最近上下文抽象成行为选择所需的场景画像。"""

    @staticmethod
    def _context_placeholder_text() -> str:
        return "上下文已作为后续多条消息提供；请只分析这些消息，不要把本句当作聊天内容。"

    async def analyze(
        self,
        *,
        context_text: str,
        sub_agent_runner: Optional[ScenarioAgentRunner],
        include_context_in_prompt: bool = True,
    ) -> BehaviorScenarioProfile:
        if sub_agent_runner is None:
            return BehaviorScenarioProfile()
        normalized_context = str(context_text or "").strip()
        if not normalized_context:
            return BehaviorScenarioProfile()

        prompt = load_prompt(
            "behavior_scene_analyze",
            bot_name=global_config.bot.nickname,
            context_text=normalized_context if include_context_in_prompt else self._context_placeholder_text(),
        )
        try:
            raw_response = await sub_agent_runner(prompt)
        except Exception as exc:
            logger.debug(f"行为表现情景画像子代理失败，已退回本地检索: {exc}")
            return BehaviorScenarioProfile()
        return parse_behavior_scenario_response(raw_response)

    async def analyze_segments(
        self,
        *,
        context_text: str,
        sub_agent_runner: Optional[ScenarioAgentRunner],
    ) -> list[BehaviorScenarioSegment]:
        """将一次学习窗口拆成 1~3 个可独立学习的场景片段。"""

        if sub_agent_runner is None:
            return []
        normalized_context = str(context_text or "").strip()
        if not normalized_context:
            return []

        prompt = load_prompt(
            "behavior_scene_analyze",
            bot_name=global_config.bot.nickname,
            context_text=normalized_context,
        )
        try:
            raw_response = await sub_agent_runner(prompt)
        except Exception as exc:
            logger.debug(f"行为表现多场景片段分析失败，跳过本轮场景切分: {exc}")
            return []
        return parse_behavior_scenario_segments_response(raw_response)


behavior_scenario_analyzer = BehaviorScenarioAnalyzer()
