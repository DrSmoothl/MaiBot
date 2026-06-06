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


class BehaviorScenarioAnalyzer:
    """用 LLM 将最近上下文抽象成行为选择所需的场景画像。"""

    async def analyze(
        self,
        *,
        context_text: str,
        sub_agent_runner: Optional[ScenarioAgentRunner],
    ) -> BehaviorScenarioProfile:
        if sub_agent_runner is None:
            return BehaviorScenarioProfile()
        normalized_context = str(context_text or "").strip()
        if not normalized_context:
            return BehaviorScenarioProfile()

        prompt = load_prompt(
            "behavior_scene_analyze",
            bot_name=global_config.bot.nickname,
            context_text=normalized_context,
        )
        try:
            raw_response = await sub_agent_runner(prompt)
        except Exception as exc:
            logger.debug(f"行为表现情景画像子代理失败，已退回本地检索: {exc}")
            return BehaviorScenarioProfile()
        return parse_behavior_scenario_response(raw_response)


behavior_scenario_analyzer = BehaviorScenarioAnalyzer()
