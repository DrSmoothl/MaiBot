from datetime import datetime
from typing import Any, Optional, Sequence

from sqlmodel import Session, select

import difflib
import json

from src.common.database.database import get_db_session
from src.common.database.database_model import (
    BehaviorActionNode,
    BehaviorExperiencePath,
    BehaviorOutcomeNode,
    BehaviorSceneNode,
)
from src.common.logger import get_logger

from .behavior_scenario import BehaviorScenarioProfile
from .behavior_scene_graph_store import (
    apply_behavior_scene_feedback,
    link_behavior_experience_to_scene_graph,
    mark_behavior_scene_links_selected,
    upsert_behavior_graph_refs,
)

logger = get_logger("behavior_pattern_store")

BEHAVIOR_REFERENCE_SOURCE = "behavior_pattern"
BEHAVIOR_REFERENCE_DISPLAY_PREFIX = "[行为表现参考]"
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


def _path_texts_from_session(
    session: Session,
    path: BehaviorExperiencePath,
) -> tuple[str, str, str]:
    scene_node = session.get(BehaviorSceneNode, path.start_scene_node_id)
    action_node = session.get(BehaviorActionNode, path.action_node_id)
    outcome_node = session.get(BehaviorOutcomeNode, path.outcome_node_id)
    trigger = scene_node.name if scene_node is not None else ""
    action = action_node.action if action_node is not None else ""
    outcome = outcome_node.outcome if outcome_node is not None else ""
    return trigger, action, outcome


def _path_similarity(
    session: Session,
    path: BehaviorExperiencePath,
    trigger: str,
    action: str,
) -> float:
    path_trigger, path_action, _ = _path_texts_from_session(session, path)
    source_text = f"{path_trigger}\n{path_action}".strip()
    target_text = f"{trigger}\n{action}".strip()
    if not source_text or not target_text:
        return 0.0
    return difflib.SequenceMatcher(None, source_text, target_text).ratio()


def _path_to_dict_from_session(
    session: Session,
    path: BehaviorExperiencePath,
) -> dict[str, Any]:
    trigger, action, outcome = _path_texts_from_session(session, path)
    return {
        "id": path.id,
        "trigger": trigger,
        "action": action,
        "outcome": outcome,
        "start_scene_node_id": path.start_scene_node_id,
        "action_node_id": path.action_node_id,
        "outcome_node_id": path.outcome_node_id,
        "count": path.count,
        "activation_count": path.activation_count,
        "success_count": path.success_count,
        "failure_count": path.failure_count,
        "score": path.score,
        "enabled": path.enabled,
        "session_id": path.session_id,
        "last_active_time": path.last_active_time.isoformat() if path.last_active_time else "",
        "last_feedback_time": path.last_feedback_time.isoformat() if path.last_feedback_time else "",
    }


def behavior_experience_to_dict(path: BehaviorExperiencePath) -> dict[str, Any]:
    if path.id is None:
        return {
            "id": None,
            "trigger": "",
            "action": "",
            "outcome": "",
            "count": path.count,
            "activation_count": path.activation_count,
            "success_count": path.success_count,
            "failure_count": path.failure_count,
            "score": path.score,
            "enabled": path.enabled,
            "session_id": path.session_id,
            "last_active_time": path.last_active_time.isoformat() if path.last_active_time else "",
            "last_feedback_time": path.last_feedback_time.isoformat() if path.last_feedback_time else "",
        }

    try:
        with get_db_session(auto_commit=False) as session:
            attached_path = session.get(BehaviorExperiencePath, path.id)
            if attached_path is None:
                return {}
            return _path_to_dict_from_session(session, attached_path)
    except Exception as exc:
        logger.error(f"读取行为经验路径文本失败: id={path.id} error={exc}")
        return {}


def find_similar_behavior_experience(
    *,
    trigger: str,
    action: str,
    session_id: str,
    similarity_threshold: float = 0.76,
) -> Optional[tuple[BehaviorExperiencePath, float]]:
    normalized_trigger = _normalize_text(trigger, max_length=180)
    normalized_action = _normalize_text(action, max_length=240)
    if not normalized_trigger or not normalized_action:
        return None

    try:
        with get_db_session(auto_commit=False) as session:
            statement = select(BehaviorExperiencePath).where(BehaviorExperiencePath.session_id == session_id)
            paths = session.exec(statement).all()
            best_path: Optional[BehaviorExperiencePath] = None
            best_similarity = 0.0
            for path in paths:
                similarity = _path_similarity(session, path, normalized_trigger, normalized_action)
                if similarity > similarity_threshold and similarity > best_similarity:
                    best_path = path
                    best_similarity = similarity
            if best_path is None:
                return None
            session.expunge(best_path)
            return best_path, best_similarity
    except Exception as exc:
        logger.error(f"查找相似行为经验路径失败: {exc}")
        return None


def upsert_behavior_experience(
    *,
    trigger: str,
    action: str,
    outcome: str,
    source_ids: Sequence[str],
    session_id: str,
    scenario_profile: BehaviorScenarioProfile,
    scene_start: str,
) -> Optional[BehaviorExperiencePath]:
    normalized_trigger = _normalize_text(trigger, max_length=180)
    normalized_action = _normalize_text(action, max_length=240)
    normalized_outcome = _normalize_text(outcome, max_length=240)
    normalized_source_ids = _normalize_source_ids(source_ids)
    if not normalized_trigger or not normalized_action or not normalized_outcome:
        logger.warning(
            "跳过写入行为经验路径：归一化后字段为空 "
            f"trigger={normalized_trigger!r} action={normalized_action!r} outcome={normalized_outcome!r}"
        )
        return None

    now = datetime.now()
    evidence_item = _build_evidence_item(
        trigger=normalized_trigger,
        action=normalized_action,
        outcome=normalized_outcome,
        source_ids=normalized_source_ids,
    )

    try:
        with get_db_session() as session:
            graph_refs = upsert_behavior_graph_refs(
                session=session,
                session_id=session_id,
                profile=scenario_profile,
                scene_start=scene_start,
                action=normalized_action,
                outcome=normalized_outcome,
            )
            if graph_refs is None:
                logger.warning(
                    "跳过写入行为经验路径：场景图引用生成失败 "
                    f"session_id={session_id} action={normalized_action} outcome={normalized_outcome}"
                )
                return None

            statement = (
                select(BehaviorExperiencePath)
                .where(BehaviorExperiencePath.session_id == session_id)
                .where(BehaviorExperiencePath.start_scene_node_id == graph_refs.start_scene_node_id)
                .where(BehaviorExperiencePath.action_node_id == graph_refs.action_node_id)
                .where(BehaviorExperiencePath.outcome_node_id == graph_refs.outcome_node_id)
            )
            path = session.exec(statement).first()
            if path is None:
                path = BehaviorExperiencePath(
                    session_id=session_id,
                    start_scene_node_id=graph_refs.start_scene_node_id,
                    action_node_id=graph_refs.action_node_id,
                    outcome_node_id=graph_refs.outcome_node_id,
                    evidence_list=_dump_json_list([evidence_item]),
                    feedback_list=_dump_json_list([]),
                    count=1,
                    activation_count=0,
                    success_count=0,
                    failure_count=0,
                    score=0.0,
                    enabled=True,
                    last_active_time=now,
                    create_time=now,
                    update_time=now,
                )
            else:
                evidence_items = _load_json_list(path.evidence_list)
                evidence_items.append(evidence_item)
                path.evidence_list = _dump_json_list(evidence_items[-EVIDENCE_HISTORY_LIMIT:])
                path.count += 1
                path.last_active_time = now
                path.update_time = now

            session.add(path)
            session.flush()
            session.refresh(path)
            if path.id is not None:
                link_behavior_experience_to_scene_graph(
                    session=session,
                    experience_path_id=path.id,
                    session_id=session_id,
                    refs=graph_refs,
                )
            session.expunge(path)
            return path
    except Exception as exc:
        logger.exception(
            "写入行为经验路径失败: "
            f"session_id={session_id} trigger={normalized_trigger} "
            f"action={normalized_action} outcome={normalized_outcome} "
            f"source_ids={normalized_source_ids} error={exc}"
        )
        return None


def list_behavior_experiences_for_sessions(
    *,
    session_ids: set[str],
    include_global: bool = False,
    min_score: float = -4.0,
) -> list[BehaviorExperiencePath]:
    try:
        with get_db_session(auto_commit=False) as session:
            statement = select(BehaviorExperiencePath).where(BehaviorExperiencePath.enabled.is_(True))  # type: ignore[attr-defined]
            statement = statement.where(BehaviorExperiencePath.score >= min_score)
            if include_global:
                pass
            elif session_ids:
                statement = statement.where(
                    (BehaviorExperiencePath.session_id.in_(session_ids))  # type: ignore[attr-defined]
                    | (BehaviorExperiencePath.session_id.is_(None))  # type: ignore[attr-defined]
                )
            else:
                statement = statement.where(BehaviorExperiencePath.session_id.is_(None))  # type: ignore[attr-defined]
            paths = session.exec(statement).all()
            for path in paths:
                session.expunge(path)
            return list(paths)
    except Exception as exc:
        logger.error(f"读取行为经验路径候选失败: {exc}")
        return []


def get_behavior_experience(path_id: int) -> Optional[BehaviorExperiencePath]:
    if path_id <= 0:
        return None
    try:
        with get_db_session(auto_commit=False) as session:
            path = session.get(BehaviorExperiencePath, path_id)
            if path is not None:
                session.expunge(path)
            return path
    except Exception as exc:
        logger.error(f"读取行为经验路径失败: id={path_id} error={exc}")
        return None


def mark_behavior_experience_selected(path_id: int) -> Optional[BehaviorExperiencePath]:
    if path_id <= 0:
        return None
    now = datetime.now()
    try:
        with get_db_session() as session:
            path = session.get(BehaviorExperiencePath, path_id)
            if path is None:
                return None
            path.activation_count += 1
            path.last_active_time = now
            path.update_time = now
            session.add(path)
            session.flush()
            session.refresh(path)
            session.expunge(path)
            selected_path = path
    except Exception as exc:
        logger.error(f"更新行为经验路径激活状态失败: id={path_id} error={exc}")
        return None

    mark_behavior_scene_links_selected(path_id)
    return selected_path


def apply_behavior_feedback(
    *,
    pattern_id: int,
    score_delta: float,
    status: str,
    reason: str,
    outcome: str,
    session_id: str,
) -> Optional[BehaviorExperiencePath]:
    normalized_status = str(status or "").strip().lower()
    normalized_reason = _normalize_text(reason, max_length=300)
    normalized_outcome = _normalize_text(outcome, max_length=240)
    now = datetime.now()

    try:
        with get_db_session() as session:
            path = session.get(BehaviorExperiencePath, pattern_id)
            if path is None:
                return None

            feedback_items = _load_json_list(path.feedback_list)
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
            path.feedback_list = _dump_json_list(feedback_items[-FEEDBACK_HISTORY_LIMIT:])
            path.score = _clamp_score(float(path.score or 0.0) + float(score_delta))
            path.last_feedback_time = now
            path.update_time = now
            if normalized_status in POSITIVE_FEEDBACK_STATUSES:
                path.success_count += 1
            elif normalized_status in NEGATIVE_FEEDBACK_STATUSES:
                path.failure_count += 1
            if path.score <= MIN_BEHAVIOR_SCORE and path.failure_count >= 3:
                path.enabled = False

            session.add(path)
            session.flush()
            session.refresh(path)
            session.expunge(path)
            feedback_path = path
    except Exception as exc:
        logger.error(f"写入行为经验路径反馈失败: id={pattern_id} error={exc}")
        return None

    apply_behavior_scene_feedback(
        experience_path_id=pattern_id,
        score_delta=score_delta,
        status=normalized_status,
    )
    return feedback_path


# 兼容旧调用命名；运行时实体已经是 BehaviorExperiencePath。
behavior_pattern_to_dict = behavior_experience_to_dict
find_similar_behavior_pattern = find_similar_behavior_experience
upsert_behavior_pattern = upsert_behavior_experience
list_behavior_patterns_for_sessions = list_behavior_experiences_for_sessions
get_behavior_pattern = get_behavior_experience
mark_behavior_pattern_selected = mark_behavior_experience_selected
