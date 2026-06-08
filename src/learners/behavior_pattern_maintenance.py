from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlmodel import select

import difflib
import json
import time

from src.common.database.database import get_db_session
from src.common.database.database_model import BehaviorExperiencePath, BehaviorSceneCluster
from src.common.logger import get_logger

from .behavior_pattern_store import (
    EVIDENCE_HISTORY_LIMIT,
    FEEDBACK_HISTORY_LIMIT,
    MAX_BEHAVIOR_SCORE,
    MIN_BEHAVIOR_SCORE,
    behavior_pattern_to_dict,
)

logger = get_logger("behavior_pattern_maintenance")

MAINTENANCE_COOLDOWN_SECONDS = 60 * 60
DECAY_COOLDOWN_DAYS = 7
UNUSED_DECAY_AFTER_DAYS = 14
UNUSED_DISABLE_AFTER_DAYS = 60
UNRESPONDED_DECAY_AFTER_DAYS = 21
POSITIVE_STALE_DECAY_AFTER_DAYS = 60
MERGE_SIMILARITY_THRESHOLD = 0.86
MERGE_TRIGGER_MIN_SIMILARITY = 0.78
MERGE_ACTION_MIN_SIMILARITY = 0.78
MERGE_OUTCOME_MIN_SIMILARITY = 0.82
MERGE_CLUSTER_DISTRIBUTION_MIN_OVERLAP = 0.72
MERGE_CLUSTER_ACTION_MIN_SIMILARITY = 0.92
MERGE_CLUSTER_OUTCOME_MIN_SIMILARITY = 0.9
MERGE_CLUSTER_SIMILARITY_THRESHOLD = 0.92
MAINTENANCE_SOURCE = "behavior_pattern_maintenance"


@dataclass(frozen=True)
class BehaviorPatternMaintenanceResult:
    """行为表现维护任务的摘要结果。"""

    session_id: str
    scanned_count: int = 0
    decayed_count: int = 0
    disabled_count: int = 0
    merged_count: int = 0
    skipped_reason: str = ""
    touched_pattern_ids: list[int] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return self.decayed_count > 0 or self.disabled_count > 0 or self.merged_count > 0


@dataclass(frozen=True)
class BehaviorPatternMergeGroup:
    """一组由外部整合器确认的行为表现合并指令。"""

    keeper_id: int
    merge_ids: list[int] = field(default_factory=list)
    trigger: str = ""
    action: str = ""
    outcome: str = ""
    reason: str = ""


class BehaviorPatternMaintenanceService:
    """集中维护行为表现的用进废退与相似项整合规则。"""

    def __init__(self) -> None:
        self._last_run_at_by_session_id: dict[str, float] = {}

    def maybe_maintain_session(
        self,
        *,
        session_id: str,
        related_session_ids: Optional[set[str]] = None,
        force: bool = False,
    ) -> BehaviorPatternMaintenanceResult:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return BehaviorPatternMaintenanceResult(session_id="", skipped_reason="empty_session_id")

        current_time = time.time()
        if not force:
            last_run_at = self._last_run_at_by_session_id.get(normalized_session_id, 0.0)
            if current_time - last_run_at < MAINTENANCE_COOLDOWN_SECONDS:
                return BehaviorPatternMaintenanceResult(
                    session_id=normalized_session_id,
                    skipped_reason="cooldown",
                )

        result = self.maintain_session(
            session_id=normalized_session_id,
            related_session_ids=related_session_ids,
        )
        self._last_run_at_by_session_id[normalized_session_id] = current_time
        return result

    def maintain_session(
        self,
        *,
        session_id: str,
        related_session_ids: Optional[set[str]] = None,
        now: Optional[datetime] = None,
    ) -> BehaviorPatternMaintenanceResult:
        normalized_session_id = str(session_id or "").strip()
        target_session_ids = self._normalize_session_ids(related_session_ids)
        target_session_ids.add(normalized_session_id)
        if not normalized_session_id or not target_session_ids:
            return BehaviorPatternMaintenanceResult(session_id=normalized_session_id, skipped_reason="empty_session_id")

        maintenance_time = now or datetime.now()
        result = BehaviorPatternMaintenanceResult(session_id=normalized_session_id)

        try:
            with get_db_session() as session:
                statement = select(BehaviorExperiencePath).where(
                    BehaviorExperiencePath.session_id.in_(target_session_ids)  # type: ignore[attr-defined]
                )
                patterns = list(session.exec(statement).all())
                result = BehaviorPatternMaintenanceResult(
                    session_id=normalized_session_id,
                    scanned_count=len(patterns),
                )
                if not patterns:
                    return result

                touched_pattern_ids: list[int] = []
                decayed_count = 0
                disabled_count = 0
                for pattern in patterns:
                    if not pattern.enabled:
                        continue
                    decay_result = self._apply_decay(pattern, now=maintenance_time)
                    if decay_result.decayed:
                        decayed_count += 1
                        if pattern.id is not None:
                            touched_pattern_ids.append(pattern.id)
                    if decay_result.disabled:
                        disabled_count += 1
                        if pattern.id is not None:
                            touched_pattern_ids.append(pattern.id)
                    if decay_result.decayed or decay_result.disabled:
                        session.add(pattern)

                merged_count = self._merge_similar_patterns(
                    session_patterns=patterns,
                    now=maintenance_time,
                    touched_pattern_ids=touched_pattern_ids,
                )

                result = BehaviorPatternMaintenanceResult(
                    session_id=normalized_session_id,
                    scanned_count=len(patterns),
                    decayed_count=decayed_count,
                    disabled_count=disabled_count,
                    merged_count=merged_count,
                    touched_pattern_ids=self._dedupe_ids(touched_pattern_ids),
                )
                if result.changed:
                    logger.info(
                        f"行为表现维护完成: session_id={normalized_session_id} "
                        f"扫描={result.scanned_count} 衰减={result.decayed_count} "
                        f"禁用={result.disabled_count} 合并={result.merged_count}"
                    )
                return result
        except Exception as exc:
            logger.error(f"行为表现维护失败: session_id={normalized_session_id} error={exc}")
            return BehaviorPatternMaintenanceResult(session_id=normalized_session_id, skipped_reason="error")

    def apply_merge_groups(
        self,
        *,
        session_id: str,
        merge_groups: Sequence[BehaviorPatternMergeGroup],
        now: Optional[datetime] = None,
    ) -> BehaviorPatternMaintenanceResult:
        """应用语义整合器确认的合并方案，只合并当前聊天流内的行为表现。"""

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return BehaviorPatternMaintenanceResult(session_id="", skipped_reason="empty_session_id")
        if not merge_groups:
            return BehaviorPatternMaintenanceResult(session_id=normalized_session_id, skipped_reason="empty_merge_groups")

        merge_time = now or datetime.now()
        try:
            with get_db_session() as session:
                statement = select(BehaviorExperiencePath).where(
                    BehaviorExperiencePath.session_id == normalized_session_id
                )
                patterns = list(session.exec(statement).all())
                pattern_by_id = {path.id: path for path in patterns if path.id is not None}
                used_pattern_ids: set[int] = set()
                touched_pattern_ids: list[int] = []
                merged_count = 0

                for merge_group in merge_groups:
                    keeper_id = int(merge_group.keeper_id or 0)
                    merge_ids = [
                        merge_id
                        for merge_id in self._dedupe_ids([int(raw_id or 0) for raw_id in merge_group.merge_ids])
                        if merge_id > 0 and merge_id != keeper_id
                    ]
                    if keeper_id <= 0 or not merge_ids:
                        continue
                    if keeper_id in used_pattern_ids or any(merge_id in used_pattern_ids for merge_id in merge_ids):
                        continue

                    keeper = pattern_by_id.get(keeper_id)
                    if keeper is None or not keeper.enabled:
                        continue

                    duplicates: list[BehaviorExperiencePath] = []
                    for merge_id in merge_ids:
                        duplicate = pattern_by_id.get(merge_id)
                        if duplicate is None or not duplicate.enabled:
                            continue
                        if not self._should_merge(keeper, duplicate):
                            logger.debug(
                                f"跳过语义整合建议：keeper_id={keeper_id} merge_id={merge_id} "
                                "未满足维护器合并规则"
                            )
                            continue
                        duplicates.append(duplicate)
                    if not duplicates:
                        continue

                    for duplicate in duplicates:
                        self._merge_into_keeper(keeper, duplicate, now=merge_time)
                        session.add(duplicate)
                        if duplicate.id is not None:
                            used_pattern_ids.add(duplicate.id)
                            touched_pattern_ids.append(duplicate.id)

                    self._apply_merge_group_summary(keeper, merge_group)
                    self._append_maintenance_event(
                        keeper,
                        now=merge_time,
                        score_delta=0.0,
                        status="semantic_consolidation",
                        reason=merge_group.reason or "语义整合器确认这些行为表现描述的是同一类可复用行为。",
                        extra={
                            "merged_behavior_ids": [duplicate.id for duplicate in duplicates if duplicate.id is not None],
                        },
                    )
                    keeper.update_time = merge_time
                    session.add(keeper)
                    if keeper.id is not None:
                        used_pattern_ids.add(keeper.id)
                        touched_pattern_ids.append(keeper.id)
                    merged_count += len(duplicates)

                result = BehaviorPatternMaintenanceResult(
                    session_id=normalized_session_id,
                    scanned_count=len(patterns),
                    merged_count=merged_count,
                    touched_pattern_ids=self._dedupe_ids(touched_pattern_ids),
                )
                if result.changed:
                    logger.info(
                        f"行为表现语义整合已应用: session_id={normalized_session_id} "
                        f"扫描={result.scanned_count} 合并={result.merged_count}"
                    )
                return result
        except Exception as exc:
            logger.error(f"行为表现语义整合应用失败: session_id={normalized_session_id} error={exc}")
            return BehaviorPatternMaintenanceResult(session_id=normalized_session_id, skipped_reason="error")

    @staticmethod
    def _normalize_session_ids(session_ids: Optional[set[str]]) -> set[str]:
        if not session_ids:
            return set()
        return {str(session_id or "").strip() for session_id in session_ids if str(session_id or "").strip()}

    @staticmethod
    def _normalize_text(text: str, *, max_length: int) -> str:
        normalized_text = " ".join(str(text or "").split()).strip()
        if len(normalized_text) <= max_length:
            return normalized_text
        return normalized_text[:max_length].rstrip()

    @staticmethod
    def _dedupe_ids(pattern_ids: Sequence[int]) -> list[int]:
        deduped_ids: list[int] = []
        for pattern_id in pattern_ids:
            if pattern_id not in deduped_ids:
                deduped_ids.append(pattern_id)
        return deduped_ids

    @staticmethod
    def _load_json_list(raw_value: Any) -> list[Any]:
        if not raw_value:
            return []
        if isinstance(raw_value, list):
            return raw_value
        if not isinstance(raw_value, str):
            return []
        try:
            parsed_value = json.loads(raw_value)
        except (TypeError, ValueError):
            return []
        return parsed_value if isinstance(parsed_value, list) else []

    @staticmethod
    def _dump_json_list(items: Sequence[Any]) -> str:
        return json.dumps(list(items), ensure_ascii=False)

    @staticmethod
    def _clamp_score(score: float) -> float:
        return min(MAX_BEHAVIOR_SCORE, max(MIN_BEHAVIOR_SCORE, score))

    @staticmethod
    def _days_since(now: datetime, timestamp: Optional[datetime]) -> int:
        if timestamp is None:
            return 0
        return max(0, (now - timestamp).days)

    @staticmethod
    def _latest_activity_time(pattern: BehaviorExperiencePath) -> datetime:
        timestamps = [
            timestamp
            for timestamp in [pattern.last_active_time, pattern.last_feedback_time, pattern.create_time]
            if timestamp is not None
        ]
        return max(timestamps) if timestamps else datetime.now()

    def _last_maintenance_time(self, pattern: BehaviorExperiencePath) -> Optional[datetime]:
        feedback_items = self._load_json_list(pattern.feedback_list)
        maintenance_times: list[datetime] = []
        for feedback_item in feedback_items:
            if not isinstance(feedback_item, dict):
                continue
            if feedback_item.get("source") != MAINTENANCE_SOURCE:
                continue
            raw_created_at = str(feedback_item.get("created_at") or "").strip()
            if not raw_created_at:
                continue
            try:
                maintenance_times.append(datetime.fromisoformat(raw_created_at))
            except ValueError:
                continue
        return max(maintenance_times) if maintenance_times else None

    def _append_maintenance_event(
        self,
        pattern: BehaviorExperiencePath,
        *,
        now: datetime,
        score_delta: float,
        status: str,
        reason: str,
        outcome: str = "",
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        feedback_items = self._load_json_list(pattern.feedback_list)
        event = {
            "score_delta": score_delta,
            "status": status,
            "reason": reason,
            "outcome": outcome,
            "session_id": pattern.session_id,
            "created_at": now.isoformat(timespec="seconds"),
            "source": MAINTENANCE_SOURCE,
        }
        if extra:
            event.update(extra)
        feedback_items.append(event)
        pattern.feedback_list = self._dump_json_list(feedback_items[-FEEDBACK_HISTORY_LIMIT:])

    @dataclass(frozen=True)
    class _DecayResult:
        decayed: bool = False
        disabled: bool = False

    def _apply_decay(self, pattern: BehaviorExperiencePath, *, now: datetime) -> _DecayResult:
        last_maintenance_time = self._last_maintenance_time(pattern)
        if last_maintenance_time is not None and self._days_since(now, last_maintenance_time) < DECAY_COOLDOWN_DAYS:
            return self._DecayResult()

        latest_activity_time = self._latest_activity_time(pattern)
        inactive_days = self._days_since(now, latest_activity_time)
        score_delta, reason = self._calculate_decay(pattern, inactive_days=inactive_days)
        decayed = score_delta < 0
        disabled = False

        if decayed:
            pattern.score = self._clamp_score(float(pattern.score or 0.0) + score_delta)
            self._append_maintenance_event(
                pattern,
                now=now,
                score_delta=score_delta,
                status="maintenance_decay",
                reason=reason,
            )

        if self._should_disable(pattern, inactive_days=inactive_days):
            pattern.enabled = False
            disabled = True
            self._append_maintenance_event(
                pattern,
                now=now,
                score_delta=0.0,
                status="maintenance_disable",
                reason="长期缺少有效强化或负反馈过多，暂时停止作为行为表现候选。",
            )

        if decayed or disabled:
            pattern.update_time = now
        return self._DecayResult(decayed=decayed, disabled=disabled)

    @staticmethod
    def _calculate_decay(pattern: BehaviorExperiencePath, *, inactive_days: int) -> tuple[float, str]:
        count = int(pattern.count or 0)
        activation_count = int(pattern.activation_count or 0)
        success_count = int(pattern.success_count or 0)
        failure_count = int(pattern.failure_count or 0)

        if count <= 1 and activation_count <= 0 and inactive_days >= UNUSED_DECAY_AFTER_DAYS:
            periods = min(4, max(1, inactive_days // UNUSED_DECAY_AFTER_DAYS))
            return -0.35 * periods, "一次性观察长期未被再次强化，按用进废退规则轻度衰减。"

        if activation_count > 0 and success_count <= 0 and inactive_days >= UNRESPONDED_DECAY_AFTER_DAYS:
            periods = min(3, max(1, inactive_days // UNRESPONDED_DECAY_AFTER_DAYS))
            return -0.25 * periods, "被选择后长期没有成功反馈，降低后续抽样权重。"

        if success_count > 0 and failure_count <= success_count and inactive_days >= POSITIVE_STALE_DECAY_AFTER_DAYS:
            return -0.15, "曾经有效但长期未再出现，轻微衰减以给新行为让路。"

        return 0.0, ""

    @staticmethod
    def _should_disable(pattern: BehaviorExperiencePath, *, inactive_days: int) -> bool:
        score = float(pattern.score or 0.0)
        count = int(pattern.count or 0)
        activation_count = int(pattern.activation_count or 0)
        success_count = int(pattern.success_count or 0)
        failure_count = int(pattern.failure_count or 0)

        if score <= MIN_BEHAVIOR_SCORE and (failure_count >= 2 or activation_count >= 3):
            return True
        if count <= 1 and activation_count <= 0 and inactive_days >= UNUSED_DISABLE_AFTER_DAYS and score <= -3.0:
            return True
        if failure_count >= 3 and success_count <= 0 and score <= -4.0:
            return True
        return False

    def _merge_similar_patterns(
        self,
        *,
        session_patterns: list[BehaviorExperiencePath],
        now: datetime,
        touched_pattern_ids: list[int],
    ) -> int:
        merged_count = 0
        merged_pattern_ids: set[int] = set()
        patterns_by_session_id: dict[str, list[BehaviorExperiencePath]] = {}
        for pattern in session_patterns:
            if pattern.id is None or not pattern.enabled:
                continue
            session_id = str(pattern.session_id or "").strip()
            if not session_id:
                continue
            patterns_by_session_id.setdefault(session_id, []).append(pattern)

        for patterns in patterns_by_session_id.values():
            patterns.sort(key=lambda pattern: pattern.id or 0)
            for left_index, left_pattern in enumerate(patterns):
                if left_pattern.id in merged_pattern_ids or not left_pattern.enabled:
                    continue
                for right_pattern in patterns[left_index + 1 :]:
                    if right_pattern.id in merged_pattern_ids or not right_pattern.enabled:
                        continue
                    if not self._should_merge(left_pattern, right_pattern):
                        continue

                    keeper, duplicate = self._choose_keeper(left_pattern, right_pattern)
                    self._merge_into_keeper(keeper, duplicate, now=now)
                    if duplicate.id is not None:
                        merged_pattern_ids.add(duplicate.id)
                        touched_pattern_ids.append(duplicate.id)
                    if keeper.id is not None:
                        touched_pattern_ids.append(keeper.id)
                    merged_count += 1

                    if left_pattern.id == duplicate.id:
                        break

        return merged_count

    @staticmethod
    def _text_similarity(left_text: str, right_text: str) -> float:
        normalized_left = " ".join(str(left_text or "").split()).strip()
        normalized_right = " ".join(str(right_text or "").split()).strip()
        if not normalized_left or not normalized_right:
            return 0.0
        return difflib.SequenceMatcher(None, normalized_left, normalized_right).ratio()

    @staticmethod
    def _path_payload(path: BehaviorExperiencePath) -> dict[str, str]:
        payload = behavior_pattern_to_dict(path)
        return {
            "trigger": str(payload.get("trigger") or ""),
            "action": str(payload.get("action") or ""),
            "outcome": str(payload.get("outcome") or ""),
        }

    @staticmethod
    def _cluster_distribution(path: BehaviorExperiencePath) -> str:
        try:
            with get_db_session(auto_commit=False) as session:
                scene_cluster = session.get(BehaviorSceneCluster, path.scene_cluster_id)
                if scene_cluster is None:
                    return ""
                return str(scene_cluster.tag_distribution or "")
        except Exception as exc:
            logger.debug(f"读取行为表现所属场景簇失败: behavior_id={path.id} error={exc}")
            return ""

    @classmethod
    def _cluster_distribution_overlap(cls, left_pattern: BehaviorExperiencePath, right_pattern: BehaviorExperiencePath) -> float:
        left_distribution = cls._load_cluster_distribution(cls._cluster_distribution(left_pattern))
        right_distribution = cls._load_cluster_distribution(cls._cluster_distribution(right_pattern))
        if not left_distribution or not right_distribution:
            return 0.0
        shared_tags = set(left_distribution) & set(right_distribution)
        return sum(min(left_distribution[tag], right_distribution[tag]) for tag in shared_tags)

    @staticmethod
    def _load_cluster_distribution(raw_value: str) -> dict[str, float]:
        try:
            raw_distribution = json.loads(raw_value or "[]")
        except (TypeError, ValueError):
            return {}
        if not isinstance(raw_distribution, list):
            return {}
        distribution: dict[str, float] = {}
        for item in raw_distribution:
            if not isinstance(item, dict):
                continue
            tag = str(item.get("tag") or "").strip()
            if not tag:
                continue
            try:
                probability = float(item.get("probability") or 0.0)
            except (TypeError, ValueError):
                continue
            if probability > 0:
                distribution[tag] = distribution.get(tag, 0.0) + probability
        total_probability = sum(distribution.values())
        if total_probability <= 0:
            return {}
        return {tag: probability / total_probability for tag, probability in distribution.items()}

    def _should_merge(self, left_pattern: BehaviorExperiencePath, right_pattern: BehaviorExperiencePath) -> bool:
        left_payload = self._path_payload(left_pattern)
        right_payload = self._path_payload(right_pattern)
        cluster_overlap = self._cluster_distribution_overlap(left_pattern, right_pattern)
        if cluster_overlap < MERGE_CLUSTER_DISTRIBUTION_MIN_OVERLAP:
            return False

        trigger_similarity = self._text_similarity(left_payload["trigger"], right_payload["trigger"])
        action_similarity = self._text_similarity(left_payload["action"], right_payload["action"])
        outcome_similarity = self._text_similarity(left_payload["outcome"], right_payload["outcome"])
        combined_similarity = self._text_similarity(
            f"{left_payload['trigger']}\n{left_payload['action']}",
            f"{right_payload['trigger']}\n{right_payload['action']}",
        )
        return (
            trigger_similarity >= MERGE_TRIGGER_MIN_SIMILARITY
            and action_similarity >= MERGE_CLUSTER_ACTION_MIN_SIMILARITY
            and outcome_similarity >= MERGE_CLUSTER_OUTCOME_MIN_SIMILARITY
            and combined_similarity >= MERGE_CLUSTER_SIMILARITY_THRESHOLD
        )

    @staticmethod
    def _keeper_rank(pattern: BehaviorExperiencePath) -> tuple[float, int, int, int, int, datetime]:
        return (
            float(pattern.score or 0.0),
            int(pattern.success_count or 0) - int(pattern.failure_count or 0),
            int(pattern.activation_count or 0),
            int(pattern.count or 0),
            1 if pattern.enabled else 0,
            pattern.last_active_time or pattern.create_time or datetime.min,
        )

    def _choose_keeper(
        self,
        left_pattern: BehaviorExperiencePath,
        right_pattern: BehaviorExperiencePath,
    ) -> tuple[BehaviorExperiencePath, BehaviorExperiencePath]:
        if self._keeper_rank(left_pattern) >= self._keeper_rank(right_pattern):
            return left_pattern, right_pattern
        return right_pattern, left_pattern

    def _merge_into_keeper(
        self,
        keeper: BehaviorExperiencePath,
        duplicate: BehaviorExperiencePath,
        *,
        now: datetime,
    ) -> None:
        keeper_weight = self._pattern_weight(keeper)
        duplicate_weight = self._pattern_weight(duplicate)
        total_weight = keeper_weight + duplicate_weight
        keeper.score = self._clamp_score(
            (float(keeper.score or 0.0) * keeper_weight + float(duplicate.score or 0.0) * duplicate_weight)
            / total_weight
        )

        keeper.count = int(keeper.count or 0) + int(duplicate.count or 0)
        keeper.activation_count = int(keeper.activation_count or 0) + int(duplicate.activation_count or 0)
        keeper.success_count = int(keeper.success_count or 0) + int(duplicate.success_count or 0)
        keeper.failure_count = int(keeper.failure_count or 0) + int(duplicate.failure_count or 0)
        keeper.evidence_list = self._merge_json_histories(
            keeper.evidence_list,
            duplicate.evidence_list,
            limit=EVIDENCE_HISTORY_LIMIT,
        )
        keeper.feedback_list = self._merge_json_histories(
            keeper.feedback_list,
            duplicate.feedback_list,
            limit=FEEDBACK_HISTORY_LIMIT - 1,
        )
        self._append_maintenance_event(
            keeper,
            now=now,
            score_delta=0.0,
            status="maintenance_merge",
            reason=f"合并相似行为表现，来源 behavior_id={duplicate.id}。",
            extra={"merged_behavior_id": duplicate.id},
        )

        keeper.last_active_time = max(keeper.last_active_time, duplicate.last_active_time)
        keeper.last_feedback_time = self._max_optional_datetime(keeper.last_feedback_time, duplicate.last_feedback_time)
        keeper.update_time = now

        duplicate.enabled = False
        duplicate.update_time = now
        self._append_maintenance_event(
            duplicate,
            now=now,
            score_delta=0.0,
            status="maintenance_merged_into",
            reason=f"已整合到更合适的行为表现 behavior_id={keeper.id}。",
            extra={"merged_into_behavior_id": keeper.id},
        )

    def _apply_merge_group_summary(
        self,
        keeper: BehaviorExperiencePath,
        merge_group: BehaviorPatternMergeGroup,
    ) -> None:
        del keeper, merge_group

    @staticmethod
    def _pattern_weight(pattern: BehaviorExperiencePath) -> float:
        return max(
            1.0,
            float(pattern.count or 0)
            + float(pattern.activation_count or 0)
            + float(pattern.success_count or 0)
            + float(pattern.failure_count or 0),
        )

    @staticmethod
    def _merge_outcomes(left_outcome: str, right_outcome: str) -> str:
        outcomes: list[str] = []
        for outcome in [left_outcome, right_outcome]:
            normalized_outcome = " ".join(str(outcome or "").split()).strip()
            if not normalized_outcome:
                continue
            if any(difflib.SequenceMatcher(None, normalized_outcome, existing).ratio() >= 0.88 for existing in outcomes):
                continue
            outcomes.append(normalized_outcome)
        merged_outcome = "；".join(outcomes)
        if len(merged_outcome) <= 240:
            return merged_outcome
        return merged_outcome[:240].rstrip("； ")

    def _merge_json_histories(self, left_raw: Any, right_raw: Any, *, limit: int) -> str:
        merged_items: list[Any] = []
        seen_keys: set[str] = set()
        for item in [*self._load_json_list(left_raw), *self._load_json_list(right_raw)]:
            item_key = self._history_item_key(item)
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            merged_items.append(item)
        return self._dump_json_list(merged_items[-limit:])

    @staticmethod
    def _history_item_key(item: Any) -> str:
        try:
            return json.dumps(item, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            return str(item)

    @staticmethod
    def _max_optional_datetime(
        left_time: Optional[datetime],
        right_time: Optional[datetime],
    ) -> Optional[datetime]:
        if left_time is None:
            return right_time
        if right_time is None:
            return left_time
        return max(left_time, right_time)


behavior_pattern_maintenance = BehaviorPatternMaintenanceService()
