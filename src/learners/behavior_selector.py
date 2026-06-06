from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional

from json_repair import repair_json

import json
import random
import re

from src.common.logger import get_logger
from src.common.prompt_i18n import load_prompt
from src.common.utils.utils_config import BehaviorConfigUtils, ChatConfigUtils
from src.config.config import global_config

from .behavior_pattern_maintenance import behavior_pattern_maintenance
from .behavior_pattern_store import (
    behavior_pattern_to_dict,
    list_behavior_patterns_for_sessions,
    mark_behavior_pattern_selected,
)
from .behavior_scenario import BehaviorScenarioProfile, behavior_scenario_analyzer

logger = get_logger("behavior_selector")

SubAgentRunner = Callable[[str], Awaitable[str]]
MAX_SELECTOR_CANDIDATES = 12
CONTEXT_RETRIEVAL_MIN_SCORE = 0.03
MAX_CONTEXT_RETRIEVAL_TERMS = 240


@dataclass
class BehaviorPatternSelectionResult:
    """planner 侧行为表现选择结果。"""

    reference_text: str = ""
    selected_behavior_id: Optional[int] = None
    selected_behavior: Optional[dict[str, Any]] = None
    selection_reason: str = ""


class BehaviorPatternSelector:
    """根据当前 planner 上下文挑选可选行为表现参考。"""

    def _can_use_behaviors(self, session_id: str) -> bool:
        try:
            use_behavior, _ = BehaviorConfigUtils.get_behavior_config_for_chat(session_id)
            return use_behavior
        except Exception as exc:
            logger.error(f"检查行为表现使用开关失败: {exc}")
            return False

    @staticmethod
    def _is_global_expression_group_marker(platform: str, item_id: str) -> bool:
        return platform == "*" and item_id == "*"

    def _resolve_behavior_group_scope(self, session_id: str) -> tuple[set[str], bool]:
        related_session_ids = {session_id}
        has_global_share = False
        expression_groups = global_config.expression.expression_groups

        for expression_group in expression_groups:
            target_items = expression_group.targets
            group_session_ids: set[str] = set()
            contains_current_session = False
            contains_global_share_marker = False

            for target_item in target_items:
                platform = target_item.platform.strip()
                item_id = target_item.item_id.strip()
                if self._is_global_expression_group_marker(platform, item_id):
                    contains_global_share_marker = True
                    continue
                if not platform or not item_id:
                    continue

                target_session_ids = ChatConfigUtils.get_target_session_ids(target_item)
                group_session_ids.update(target_session_ids)
                if ChatConfigUtils.target_matches_session(target_item, session_id):
                    contains_current_session = True

            if contains_global_share_marker:
                has_global_share = True
            if contains_current_session:
                related_session_ids.update(group_session_ids)

        return related_session_ids, has_global_share

    @staticmethod
    def _candidate_weight(candidate: dict[str, Any]) -> float:
        count = max(float(candidate.get("count") or 0.0), 0.0)
        score = float(candidate.get("score") or 0.0)
        success_count = max(float(candidate.get("success_count") or 0.0), 0.0)
        failure_count = max(float(candidate.get("failure_count") or 0.0), 0.0)
        activation_count = max(float(candidate.get("activation_count") or 0.0), 0.0)
        return max(
            0.2,
            1.0 + count * 0.15 + score * 0.7 + success_count * 0.4 - failure_count * 0.6 - activation_count * 0.03,
        )

    def _weighted_sample_candidates(
        self,
        candidates: list[dict[str, Any]],
        max_count: int,
    ) -> list[dict[str, Any]]:
        if len(candidates) <= max_count:
            return list(candidates)

        remaining_candidates = list(candidates)
        selected_candidates: list[dict[str, Any]] = []
        for _ in range(max_count):
            weights = [self._candidate_weight(candidate) for candidate in remaining_candidates]
            total_weight = sum(weights)
            if total_weight <= 0:
                selected_candidates.append(remaining_candidates.pop(random.randrange(len(remaining_candidates))))
                continue

            threshold = random.uniform(0, total_weight)
            cumulative_weight = 0.0
            selected_index = len(remaining_candidates) - 1
            for index, weight in enumerate(weights):
                cumulative_weight += weight
                if cumulative_weight >= threshold:
                    selected_index = index
                    break
            selected_candidates.append(remaining_candidates.pop(selected_index))

        return selected_candidates

    @staticmethod
    def _normalize_retrieval_text(text: str) -> str:
        return " ".join(str(text or "").lower().split()).strip()

    @classmethod
    def _extract_retrieval_terms(cls, text: str) -> set[str]:
        normalized_text = cls._normalize_retrieval_text(text)
        if not normalized_text:
            return set()

        terms: set[str] = set()
        for token in re.findall(r"[a-z0-9_./:-]{2,}", normalized_text):
            terms.add(token)

        chinese_segments = re.findall(r"[\u4e00-\u9fff]+", normalized_text)
        for segment in chinese_segments:
            if len(segment) == 1:
                terms.add(segment)
                continue
            for ngram_length in (2, 3, 4):
                if len(segment) < ngram_length:
                    continue
                for index in range(len(segment) - ngram_length + 1):
                    terms.add(segment[index : index + ngram_length])
        if len(terms) <= MAX_CONTEXT_RETRIEVAL_TERMS:
            return terms
        return set(sorted(terms)[:MAX_CONTEXT_RETRIEVAL_TERMS])

    @classmethod
    def _context_relevance_score(cls, candidate: dict[str, Any], context_text: str) -> float:
        normalized_context = cls._normalize_retrieval_text(context_text)
        if not normalized_context:
            return 0.0

        candidate_text = "\n".join(
            [
                str(candidate.get("trigger") or ""),
                str(candidate.get("action") or ""),
                str(candidate.get("outcome") or ""),
            ]
        )
        query_terms = cls._extract_retrieval_terms(normalized_context)
        candidate_terms = cls._extract_retrieval_terms(candidate_text)
        if not query_terms or not candidate_terms:
            return 0.0

        overlap_count = len(query_terms & candidate_terms)
        if overlap_count <= 0:
            return 0.0

        overlap_score = overlap_count / (len(query_terms) ** 0.5 * len(candidate_terms) ** 0.5)
        trigger_terms = cls._extract_retrieval_terms(str(candidate.get("trigger") or ""))
        action_terms = cls._extract_retrieval_terms(str(candidate.get("action") or ""))
        trigger_bonus = len(query_terms & trigger_terms) / max(len(trigger_terms), 1)
        action_bonus = len(query_terms & action_terms) / max(len(action_terms), 1)
        return overlap_score + trigger_bonus * 0.45 + action_bonus * 0.2

    def _rank_candidates_by_context(
        self,
        candidates: list[dict[str, Any]],
        *,
        context_text: str,
        max_count: int,
    ) -> list[dict[str, Any]]:
        if len(candidates) <= max_count:
            return list(candidates)

        scored_candidates: list[tuple[float, float, dict[str, Any]]] = []
        for candidate in candidates:
            relevance_score = self._context_relevance_score(candidate, context_text)
            scored_candidates.append((relevance_score, self._candidate_weight(candidate), candidate))

        best_relevance = max((score for score, _, _ in scored_candidates), default=0.0)
        if best_relevance < CONTEXT_RETRIEVAL_MIN_SCORE:
            return self._weighted_sample_candidates(candidates, max_count)

        scored_candidates.sort(
            key=lambda item: (
                item[0],
                item[1],
                int(item[2].get("success_count") or 0),
                int(item[2].get("id") or 0),
            ),
            reverse=True,
        )
        selected_candidates: list[dict[str, Any]] = []
        selected_ids: set[int] = set()
        for relevance_score, _, candidate in scored_candidates:
            if relevance_score < CONTEXT_RETRIEVAL_MIN_SCORE and selected_candidates:
                break
            candidate_id = int(candidate.get("id") or 0)
            candidate = dict(candidate)
            candidate["context_match_score"] = round(relevance_score, 4)
            selected_candidates.append(candidate)
            selected_ids.add(candidate_id)
            if len(selected_candidates) >= max_count:
                return selected_candidates

        remaining_candidates = [
            candidate
            for _, _, candidate in scored_candidates
            if int(candidate.get("id") or 0) not in selected_ids
        ]
        if remaining_candidates:
            selected_candidates.extend(
                self._weighted_sample_candidates(
                    remaining_candidates,
                    max_count - len(selected_candidates),
                )
            )
        return selected_candidates

    def _load_behavior_candidates(
        self,
        session_id: str,
        *,
        context_text: str = "",
        scenario_profile: Optional[BehaviorScenarioProfile] = None,
    ) -> list[dict[str, Any]]:
        related_session_ids, has_global_share = self._resolve_behavior_group_scope(session_id)
        behavior_pattern_maintenance.maybe_maintain_session(
            session_id=session_id,
            related_session_ids=related_session_ids,
        )
        patterns = list_behavior_patterns_for_sessions(
            session_ids=related_session_ids,
            include_global=has_global_share,
        )
        candidates = [
            behavior_pattern_to_dict(pattern)
            for pattern in patterns
            if pattern.id is not None and pattern.trigger and pattern.action and pattern.outcome
        ]
        retrieval_text = (
            scenario_profile.to_retrieval_text(context_text)
            if scenario_profile is not None and scenario_profile.has_signal
            else context_text
        )
        return self._rank_candidates_by_context(
            candidates,
            context_text=retrieval_text,
            max_count=MAX_SELECTOR_CANDIDATES,
        )

    @staticmethod
    def _format_candidate_preview(candidates: list[dict[str, Any]]) -> str:
        preview_items: list[str] = []
        for candidate in candidates[:5]:
            preview_items.append(
                "id={id}, score={score}, trigger={trigger!r}, action={action!r}".format(
                    id=candidate.get("id"),
                    score=candidate.get("score"),
                    trigger=str(candidate.get("trigger") or "").strip(),
                    action=str(candidate.get("action") or "").strip(),
                )
            )
        return "; ".join(preview_items)

    def _build_selector_prompt(
        self,
        candidates: list[dict[str, Any]],
        *,
        scenario_profile: Optional[BehaviorScenarioProfile] = None,
    ) -> str:
        behavior_candidates = json.dumps(candidates, ensure_ascii=False, indent=2)
        return load_prompt(
            "behavior_select",
            bot_name=global_config.bot.nickname,
            behavior_candidates=behavior_candidates,
            scenario_profile=(
                scenario_profile.to_prompt_text()
                if scenario_profile is not None and scenario_profile.has_signal
                else "无可用场景画像。"
            ),
        )

    @staticmethod
    def _is_json_object_response(raw_response: str) -> bool:
        if not raw_response.strip():
            return False
        try:
            parsed_result = json.loads(repair_json(raw_response))
        except Exception:
            return False
        return isinstance(parsed_result, dict)

    @staticmethod
    def _build_json_retry_prompt(
        *,
        original_prompt: str,
        raw_response: str,
    ) -> str:
        return (
            "你刚才没有按要求输出 JSON。请重新完成同一个行为表现选择任务。\n"
            "必须只输出一个 JSON 对象，不要解释、不要 Markdown、不要项目符号。\n"
            '格式只能是 {"selected_id": 123, "reason": "..."} 或 {"selected_id": null, "reason": "..."}。\n\n'
            f"原始任务：\n{original_prompt}\n\n"
            f"你刚才的非 JSON 输出：\n{raw_response.strip()}"
        )

    @staticmethod
    def _parse_selection_response(
        raw_response: str,
        candidates: list[dict[str, Any]],
    ) -> tuple[Optional[int], str]:
        if not raw_response.strip():
            return None, ""

        try:
            parsed_result = json.loads(repair_json(raw_response))
        except Exception:
            logger.warning(f"行为表现选择结果解析失败: {raw_response!r}")
            return None, ""

        if not isinstance(parsed_result, dict):
            return None, ""

        reason = str(parsed_result.get("reason") or parsed_result.get("selection_reason") or "").strip()
        raw_selected_id = parsed_result.get("selected_id", parsed_result.get("behavior_id"))
        if raw_selected_id is None or str(raw_selected_id).strip().lower() in {"", "0", "null", "none"}:
            return None, reason

        try:
            selected_id = int(raw_selected_id)
        except (TypeError, ValueError):
            return None, reason

        candidate_ids = {
            int(candidate["id"])
            for candidate in candidates
            if isinstance(candidate.get("id"), int)
        }
        if selected_id not in candidate_ids:
            logger.debug(f"行为表现选择结果不在候选中: selected_id={selected_id}, candidate_ids={candidate_ids}")
            return None, reason
        return selected_id, reason

    @staticmethod
    def _build_reference_text(
        *,
        behavior: dict[str, Any],
        selection_reason: str,
    ) -> str:
        behavior_id = behavior.get("id")
        trigger = str(behavior.get("trigger") or "").strip()
        action = str(behavior.get("action") or "").strip()
        outcome = str(behavior.get("outcome") or "").strip()
        reason = selection_reason.strip() or "选择器认为它可能贴合当前情境。"

        return (
            f'<behavior_pattern_reference id="{behavior_id}">\n'
            "这是一条可选的行为表现参考，不是强制任务；只有在当前情境自然匹配时才采纳。\n"
            f"场景：{trigger}\n"
            f"行为：{action}\n"
            f"预期结果：{outcome}\n"
            f"选择理由：{reason}\n"
            "如果你采纳、尝试、放弃或发现无法继续，请调用 behavior_feedback；"
            f"behavior_id={behavior_id}，并说明 status、score、reason、outcome。\n"
            "</behavior_pattern_reference>"
        )

    async def select_for_planner(
        self,
        *,
        session_id: str,
        sub_agent_runner: Optional[SubAgentRunner],
        scenario_agent_runner: Optional[SubAgentRunner] = None,
        context_text: str = "",
    ) -> BehaviorPatternSelectionResult:
        if not session_id:
            return BehaviorPatternSelectionResult()
        if not self._can_use_behaviors(session_id):
            logger.debug(f"行为表现选择已跳过：当前会话未启用表达使用，session_id={session_id}")
            return BehaviorPatternSelectionResult()
        if sub_agent_runner is None:
            logger.debug("行为表现选择已跳过：缺少子代理执行器")
            return BehaviorPatternSelectionResult()

        scenario_profile = await behavior_scenario_analyzer.analyze(
            context_text=context_text,
            sub_agent_runner=scenario_agent_runner,
        )
        candidates = self._load_behavior_candidates(
            session_id,
            context_text=context_text,
            scenario_profile=scenario_profile,
        )
        if not candidates:
            logger.debug(f"行为表现选择已跳过：本地候选为空，session_id={session_id}")
            return BehaviorPatternSelectionResult()

        selector_prompt = self._build_selector_prompt(
            candidates,
            scenario_profile=scenario_profile,
        )
        try:
            raw_response = await sub_agent_runner(selector_prompt)
        except Exception as exc:
            logger.debug(f"行为表现选择子代理执行失败，已跳过: {exc}")
            return BehaviorPatternSelectionResult()

        if raw_response.strip() and not self._is_json_object_response(raw_response):
            retry_prompt = self._build_json_retry_prompt(
                original_prompt=selector_prompt,
                raw_response=raw_response,
            )
            try:
                raw_response = await sub_agent_runner(retry_prompt)
            except Exception as exc:
                logger.debug(f"行为表现选择 JSON 重试失败，已跳过: {exc}")
                return BehaviorPatternSelectionResult()

        selected_id, selection_reason = self._parse_selection_response(raw_response, candidates)
        if selected_id is None:
            logger.debug(
                f"行为表现选择器未选择候选：session_id={session_id} "
                f"reason={selection_reason!r} 候选预览={self._format_candidate_preview(candidates)}"
            )
            return BehaviorPatternSelectionResult(selection_reason=selection_reason)

        selected_pattern = mark_behavior_pattern_selected(selected_id)
        selected_behavior = (
            behavior_pattern_to_dict(selected_pattern)
            if selected_pattern is not None
            else next((candidate for candidate in candidates if candidate.get("id") == selected_id), None)
        )
        if selected_behavior is None:
            return BehaviorPatternSelectionResult(selection_reason=selection_reason)

        reference_text = self._build_reference_text(
            behavior=selected_behavior,
            selection_reason=selection_reason,
        )
        logger.debug(
            f"行为表现参考已选择：session_id={session_id} selected_id={selected_id} "
            f"reason={selection_reason!r}"
        )
        return BehaviorPatternSelectionResult(
            reference_text=reference_text,
            selected_behavior_id=selected_id,
            selected_behavior=selected_behavior,
            selection_reason=selection_reason,
        )


behavior_pattern_selector = BehaviorPatternSelector()
