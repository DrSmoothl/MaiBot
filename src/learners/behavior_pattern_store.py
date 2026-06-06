from datetime import datetime
from typing import Any, Optional, Sequence

from sqlmodel import select

import difflib
import json

from src.common.database.database import get_db_session
from src.common.database.database_model import BehaviorPattern
from src.common.logger import get_logger

logger = get_logger("behavior_pattern_store")

BEHAVIOR_REFERENCE_SOURCE = "behavior_pattern"
BEHAVIOR_REFERENCE_DISPLAY_PREFIX = "[行为表现参考]"
BEHAVIOR_REFERENCE_MAX_USES = 4
EVIDENCE_HISTORY_LIMIT = 20
FEEDBACK_HISTORY_LIMIT = 30
MIN_BEHAVIOR_SCORE = -6.0
MAX_BEHAVIOR_SCORE = 8.0
NEGATIVE_FEEDBACK_STATUSES = {"failed", "blocked", "abandoned"}
POSITIVE_FEEDBACK_STATUSES = {"success", "succeeded", "completed"}


def _load_json_list(raw_value: Any) -> list[Any]:
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return raw_value
    if not isinstance(raw_value, str):
        return []
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _dump_json_list(items: Sequence[Any]) -> str:
    return json.dumps(list(items), ensure_ascii=False)


def _clamp_score(score: float) -> float:
    return min(MAX_BEHAVIOR_SCORE, max(MIN_BEHAVIOR_SCORE, score))


def _normalize_text(text: str, *, max_length: int = 240) -> str:
    normalized_text = " ".join(str(text or "").split()).strip()
    if len(normalized_text) <= max_length:
        return normalized_text
    return normalized_text[:max_length].rstrip()


def _normalize_source_ids(source_ids: Sequence[str]) -> list[str]:
    normalized_ids: list[str] = []
    for source_id in source_ids:
        normalized_id = str(source_id or "").strip()
        if not normalized_id or normalized_id in normalized_ids:
            continue
        normalized_ids.append(normalized_id)
    return normalized_ids


def _build_evidence_item(
    *,
    trigger: str,
    action: str,
    outcome: str,
    source_ids: Sequence[str],
) -> dict[str, Any]:
    return {
        "trigger": trigger,
        "action": action,
        "outcome": outcome,
        "source_ids": _normalize_source_ids(source_ids),
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


def _pattern_similarity(pattern: BehaviorPattern, trigger: str, action: str) -> float:
    source_text = f"{pattern.trigger}\n{pattern.action}".strip()
    target_text = f"{trigger}\n{action}".strip()
    if not source_text or not target_text:
        return 0.0
    return difflib.SequenceMatcher(None, source_text, target_text).ratio()


def behavior_pattern_to_dict(pattern: BehaviorPattern) -> dict[str, Any]:
    return {
        "id": pattern.id,
        "trigger": pattern.trigger,
        "action": pattern.action,
        "outcome": pattern.outcome,
        "count": pattern.count,
        "activation_count": pattern.activation_count,
        "success_count": pattern.success_count,
        "failure_count": pattern.failure_count,
        "score": pattern.score,
        "enabled": pattern.enabled,
        "session_id": pattern.session_id,
        "last_active_time": pattern.last_active_time.isoformat() if pattern.last_active_time else "",
        "last_feedback_time": pattern.last_feedback_time.isoformat() if pattern.last_feedback_time else "",
    }


def find_similar_behavior_pattern(
    *,
    trigger: str,
    action: str,
    session_id: str,
    similarity_threshold: float = 0.76,
) -> Optional[tuple[BehaviorPattern, float]]:
    normalized_trigger = _normalize_text(trigger, max_length=160)
    normalized_action = _normalize_text(action, max_length=240)
    if not normalized_trigger or not normalized_action:
        return None

    try:
        with get_db_session(auto_commit=False) as session:
            statement = select(BehaviorPattern).where(BehaviorPattern.session_id == session_id)
            patterns = session.exec(statement).all()
            best_pattern: Optional[BehaviorPattern] = None
            best_similarity = 0.0
            for pattern in patterns:
                similarity = _pattern_similarity(pattern, normalized_trigger, normalized_action)
                if similarity > similarity_threshold and similarity > best_similarity:
                    best_pattern = pattern
                    best_similarity = similarity
            if best_pattern is None:
                return None
            return best_pattern, best_similarity
    except Exception as exc:
        logger.error(f"查找相似行为表现失败: {exc}")
        return None


def upsert_behavior_pattern(
    *,
    trigger: str,
    action: str,
    outcome: str,
    source_ids: Sequence[str],
    session_id: str,
) -> Optional[BehaviorPattern]:
    normalized_trigger = _normalize_text(trigger, max_length=160)
    normalized_action = _normalize_text(action, max_length=240)
    normalized_outcome = _normalize_text(outcome, max_length=200)
    normalized_source_ids = _normalize_source_ids(source_ids)
    if not normalized_trigger or not normalized_action or not normalized_outcome:
        return None

    similar_result = find_similar_behavior_pattern(
        trigger=normalized_trigger,
        action=normalized_action,
        session_id=session_id,
    )
    similar_pattern_id = similar_result[0].id if similar_result is not None else None
    now = datetime.now()
    evidence_item = _build_evidence_item(
        trigger=normalized_trigger,
        action=normalized_action,
        outcome=normalized_outcome,
        source_ids=normalized_source_ids,
    )

    try:
        with get_db_session() as session:
            if similar_pattern_id is not None:
                pattern = session.get(BehaviorPattern, similar_pattern_id)
                if pattern is None:
                    return None
                evidence_items = _load_json_list(pattern.evidence_list)
                evidence_items.append(evidence_item)
                pattern.evidence_list = _dump_json_list(evidence_items[-EVIDENCE_HISTORY_LIMIT:])
                pattern.count += 1
                pattern.last_active_time = now
                pattern.update_time = now
                if normalized_outcome and normalized_outcome not in pattern.outcome:
                    pattern.outcome = normalized_outcome
                session.add(pattern)
                session.flush()
                session.refresh(pattern)
                session.expunge(pattern)
                return pattern

            pattern = BehaviorPattern(
                trigger=normalized_trigger,
                action=normalized_action,
                outcome=normalized_outcome,
                evidence_list=_dump_json_list([evidence_item]),
                feedback_list=_dump_json_list([]),
                count=1,
                activation_count=0,
                success_count=0,
                failure_count=0,
                score=0.0,
                enabled=True,
                session_id=session_id,
                last_active_time=now,
                create_time=now,
                update_time=now,
            )
            session.add(pattern)
            session.flush()
            session.refresh(pattern)
            session.expunge(pattern)
            return pattern
    except Exception as exc:
        logger.error(f"写入行为表现失败: {exc}")
        return None


def list_behavior_patterns_for_sessions(
    *,
    session_ids: set[str],
    include_global: bool = False,
    min_score: float = -4.0,
) -> list[BehaviorPattern]:
    try:
        with get_db_session(auto_commit=False) as session:
            statement = select(BehaviorPattern).where(BehaviorPattern.enabled.is_(True))  # type: ignore[attr-defined]
            statement = statement.where(BehaviorPattern.score >= min_score)
            if include_global:
                pass
            elif session_ids:
                statement = statement.where(
                    (BehaviorPattern.session_id.in_(session_ids))  # type: ignore[attr-defined]
                    | (BehaviorPattern.session_id.is_(None))  # type: ignore[attr-defined]
                )
            else:
                statement = statement.where(BehaviorPattern.session_id.is_(None))  # type: ignore[attr-defined]
            patterns = session.exec(statement).all()
            for pattern in patterns:
                session.expunge(pattern)
            return list(patterns)
    except Exception as exc:
        logger.error(f"读取行为表现候选失败: {exc}")
        return []


def get_behavior_pattern(pattern_id: int) -> Optional[BehaviorPattern]:
    if pattern_id <= 0:
        return None
    try:
        with get_db_session(auto_commit=False) as session:
            pattern = session.get(BehaviorPattern, pattern_id)
            if pattern is not None:
                session.expunge(pattern)
            return pattern
    except Exception as exc:
        logger.error(f"读取行为表现失败: id={pattern_id} error={exc}")
        return None


def mark_behavior_pattern_selected(pattern_id: int) -> Optional[BehaviorPattern]:
    if pattern_id <= 0:
        return None
    now = datetime.now()
    try:
        with get_db_session() as session:
            pattern = session.get(BehaviorPattern, pattern_id)
            if pattern is None:
                return None
            pattern.activation_count += 1
            pattern.last_active_time = now
            pattern.update_time = now
            session.add(pattern)
            session.flush()
            session.refresh(pattern)
            session.expunge(pattern)
            return pattern
    except Exception as exc:
        logger.error(f"更新行为表现激活状态失败: id={pattern_id} error={exc}")
        return None


def apply_behavior_feedback(
    *,
    pattern_id: int,
    score_delta: float,
    status: str,
    reason: str,
    outcome: str,
    session_id: str,
) -> Optional[BehaviorPattern]:
    normalized_status = str(status or "").strip().lower()
    normalized_reason = _normalize_text(reason, max_length=300)
    normalized_outcome = _normalize_text(outcome, max_length=240)
    now = datetime.now()

    try:
        with get_db_session() as session:
            pattern = session.get(BehaviorPattern, pattern_id)
            if pattern is None:
                return None

            feedback_items = _load_json_list(pattern.feedback_list)
            feedback_items.append(
                {
                    "score_delta": float(score_delta),
                    "status": normalized_status,
                    "reason": normalized_reason,
                    "outcome": normalized_outcome,
                    "session_id": session_id,
                    "created_at": now.isoformat(timespec="seconds"),
                }
            )
            pattern.feedback_list = _dump_json_list(feedback_items[-FEEDBACK_HISTORY_LIMIT:])
            pattern.score = _clamp_score(float(pattern.score or 0.0) + float(score_delta))
            pattern.last_feedback_time = now
            pattern.update_time = now
            if normalized_status in POSITIVE_FEEDBACK_STATUSES:
                pattern.success_count += 1
            elif normalized_status in NEGATIVE_FEEDBACK_STATUSES:
                pattern.failure_count += 1
            if normalized_outcome:
                pattern.outcome = normalized_outcome
            if pattern.score <= MIN_BEHAVIOR_SCORE and pattern.failure_count >= 3:
                pattern.enabled = False

            session.add(pattern)
            session.flush()
            session.refresh(pattern)
            session.expunge(pattern)
            return pattern
    except Exception as exc:
        logger.error(f"写入行为表现反馈失败: id={pattern_id} error={exc}")
        return None
