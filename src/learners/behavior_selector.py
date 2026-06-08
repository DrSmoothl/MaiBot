from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import re

from src.common.logger import get_logger
from src.common.utils.utils_config import BehaviorConfigUtils, ChatConfigUtils
from src.config.config import global_config

from .behavior_pattern_maintenance import behavior_pattern_maintenance
from .behavior_pattern_store import (
    behavior_pattern_to_dict,
    list_behavior_patterns_for_sessions,
    mark_behavior_pattern_selected,
)
from .behavior_scene_graph_store import retrieve_behavior_scores_from_scene_graph
from .behavior_scenario import BehaviorScenarioProfile, behavior_scenario_analyzer

logger = get_logger("behavior_selector")

ScenarioAgentRunner = Callable[[str], Awaitable[str]]
MAX_SELECTOR_CANDIDATES = 12
CONTEXT_RETRIEVAL_MIN_SCORE = 0.03
MAX_CONTEXT_RETRIEVAL_TERMS = 240


@dataclass
class BehaviorPatternRetrievalResult:
    """planner 侧行为表现召回结果。"""

    reference_text: str = ""
    behaviors: list[dict[str, Any]] = field(default_factory=list)
    scenario_profile: BehaviorScenarioProfile = field(default_factory=BehaviorScenarioProfile)


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
        scored_candidates: list[tuple[float, float, dict[str, Any]]] = []
        for candidate in candidates:
            relevance_score = self._context_relevance_score(candidate, context_text)
            scored_candidates.append((relevance_score, self._candidate_weight(candidate), candidate))

        best_relevance = max((score for score, _, _ in scored_candidates), default=0.0)
        if best_relevance < CONTEXT_RETRIEVAL_MIN_SCORE:
            return []

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
        for relevance_score, _, candidate in scored_candidates:
            if relevance_score < CONTEXT_RETRIEVAL_MIN_SCORE and selected_candidates:
                break
            candidate = dict(candidate)
            candidate["context_match_score"] = round(relevance_score, 4)
            selected_candidates.append(candidate)
            if len(selected_candidates) >= max_count:
                return selected_candidates

        return selected_candidates

    def _rank_candidates_by_scene_graph(
        self,
        candidates: list[dict[str, Any]],
        *,
        scene_graph_scores: dict[int, float],
        context_text: str,
        max_count: int,
    ) -> list[dict[str, Any]]:
        if not scene_graph_scores:
            return []

        graph_candidates: list[dict[str, Any]] = []
        remaining_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            candidate_id = candidate.get("id")
            if not isinstance(candidate_id, int):
                continue
            graph_score = scene_graph_scores.get(candidate_id)
            if graph_score is None:
                remaining_candidates.append(candidate)
                continue
            candidate = dict(candidate)
            candidate["scene_graph_score"] = round(graph_score, 4)
            graph_candidates.append(candidate)

        if not graph_candidates:
            return []

        graph_candidates.sort(
            key=lambda candidate: (
                float(candidate.get("scene_graph_score") or 0.0),
                self._candidate_weight(candidate),
                int(candidate.get("success_count") or 0),
                int(candidate.get("id") or 0),
            ),
            reverse=True,
        )
        selected_candidates = graph_candidates[:max_count]
        if len(selected_candidates) >= max_count:
            return selected_candidates

        selected_ids = {int(candidate.get("id") or 0) for candidate in selected_candidates}
        fill_candidates = [
            candidate
            for candidate in remaining_candidates
            if int(candidate.get("id") or 0) not in selected_ids
        ]
        selected_candidates.extend(
            self._rank_candidates_by_context(
                fill_candidates,
                context_text=context_text,
                max_count=max_count - len(selected_candidates),
            )
        )
        return selected_candidates

    def _load_behavior_candidates(
        self,
        session_id: str,
        *,
        context_text: str = "",
        scenario_profile: BehaviorScenarioProfile | None = None,
        max_count: int = MAX_SELECTOR_CANDIDATES,
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
        candidates: list[dict[str, Any]] = []
        for pattern in patterns:
            if pattern.id is None:
                continue
            candidate = behavior_pattern_to_dict(pattern)
            if not candidate:
                continue
            if not candidate.get("trigger") or not candidate.get("action") or not candidate.get("outcome"):
                continue
            candidates.append(candidate)
        retrieval_text = (
            scenario_profile.to_retrieval_text(context_text)
            if scenario_profile is not None and scenario_profile.has_signal
            else context_text
        )
        if scenario_profile is not None and scenario_profile.has_signal:
            scene_graph_scores = retrieve_behavior_scores_from_scene_graph(
                session_ids=related_session_ids,
                include_global=has_global_share,
                profile=scenario_profile,
            )
            scene_graph_ranked_candidates = self._rank_candidates_by_scene_graph(
                candidates,
                scene_graph_scores=scene_graph_scores,
                context_text=retrieval_text,
                max_count=max_count,
            )
            if scene_graph_ranked_candidates:
                return scene_graph_ranked_candidates

        return self._rank_candidates_by_context(
            candidates,
            context_text=retrieval_text,
            max_count=max_count,
        )

    @staticmethod
    def _build_group_reference_text(
        *,
        behaviors: list[dict[str, Any]],
        scenario_profile: BehaviorScenarioProfile,
    ) -> str:
        reference_items: list[str] = []
        for index, behavior in enumerate(behaviors, start=1):
            behavior_id = behavior.get("id")
            trigger = str(behavior.get("trigger") or "").strip()
            action = str(behavior.get("action") or "").strip()
            outcome = str(behavior.get("outcome") or "").strip()
            scene_graph_score = behavior.get("scene_graph_score")
            context_match_score = behavior.get("context_match_score")
            score_parts = []
            if scene_graph_score is not None:
                score_parts.append(f"scene_graph_score={scene_graph_score}")
            if context_match_score is not None:
                score_parts.append(f"context_match_score={context_match_score}")
            score_text = f"\n匹配分数：{', '.join(score_parts)}" if score_parts else ""
            reference_items.append(
                f"{index}. <behavior_pattern_reference id=\"{behavior_id}\">\n"
                f"场景：{trigger}\n"
                f"行为：{action}\n"
                f"预期结果：{outcome}"
                f"{score_text}\n"
                "</behavior_pattern_reference>"
            )

        scenario_text = (
            scenario_profile.to_prompt_text()
            if scenario_profile.has_signal
            else "无可用场景画像。"
        )
        return (
            "<behavior_pattern_reference_group>\n"
            "以下是基于本轮 planner 已裁切上下文召回的行为表现参考，不是强制任务；"
            "只有在当前情境自然匹配时才采纳。\n"
            f"当前场景画像：\n{scenario_text}\n\n"
            "候选行为表现：\n"
            f"{chr(10).join(reference_items)}\n\n"
            "如果你采纳、尝试、放弃或发现无法继续其中任一行为表现，请调用 behavior_feedback，"
            "并填写对应 behavior_id、status、score、reason、outcome。\n"
            "</behavior_pattern_reference_group>"
        )

    async def retrieve_for_planner(
        self,
        *,
        session_id: str,
        scenario_agent_runner: ScenarioAgentRunner | None = None,
        context_text: str = "",
        include_context_in_prompt: bool = True,
        max_count: int = 3,
    ) -> BehaviorPatternRetrievalResult:
        """基于裁切后的 planner 上下文召回行为表现，不再使用 LLM 做最终选择。"""

        if not session_id:
            return BehaviorPatternRetrievalResult()
        if not self._can_use_behaviors(session_id):
            logger.debug(f"行为表现召回已跳过：当前会话未启用表达使用，session_id={session_id}")
            return BehaviorPatternRetrievalResult()

        scenario_profile = await behavior_scenario_analyzer.analyze(
            context_text=context_text,
            sub_agent_runner=scenario_agent_runner,
            include_context_in_prompt=include_context_in_prompt,
        )
        candidates = self._load_behavior_candidates(
            session_id,
            context_text=context_text,
            scenario_profile=scenario_profile,
            max_count=max(1, min(3, int(max_count))),
        )
        if not candidates:
            logger.debug(f"行为表现召回未命中候选：session_id={session_id}")
            return BehaviorPatternRetrievalResult(scenario_profile=scenario_profile)

        selected_behaviors: list[dict[str, Any]] = []
        for candidate in candidates[: max(1, min(3, int(max_count)))]:
            candidate_id = int(candidate.get("id") or 0)
            marked_pattern = mark_behavior_pattern_selected(candidate_id)
            selected_behavior = (
                behavior_pattern_to_dict(marked_pattern)
                if marked_pattern is not None
                else candidate
            )
            if selected_behavior:
                for score_key in ("scene_graph_score", "context_match_score"):
                    if score_key in candidate:
                        selected_behavior[score_key] = candidate[score_key]
                selected_behaviors.append(selected_behavior)

        if not selected_behaviors:
            return BehaviorPatternRetrievalResult(scenario_profile=scenario_profile)

        reference_text = self._build_group_reference_text(
            behaviors=selected_behaviors,
            scenario_profile=scenario_profile,
        )
        logger.debug(
            f"行为表现参考已召回：session_id={session_id} "
            f"ids={[behavior.get('id') for behavior in selected_behaviors]}"
        )
        return BehaviorPatternRetrievalResult(
            reference_text=reference_text,
            behaviors=selected_behaviors,
            scenario_profile=scenario_profile,
        )

behavior_pattern_selector = BehaviorPatternSelector()
