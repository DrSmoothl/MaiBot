from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Optional, Sequence

from sqlmodel import Session, select

import json

from src.common.database.database import get_db_session
from src.common.database.database_model import (
    BehaviorAction,
    BehaviorExperiencePath,
    BehaviorOutcome,
    BehaviorSceneCluster,
)
from src.common.logger import get_logger

from .behavior_scenario import BehaviorScenarioProfile
from .behavior_scene_cluster_store import (
    _load_tag_cluster_lookup,
    apply_behavior_scene_feedback,
    build_profile_tag_distribution,
    mark_behavior_scene_links_selected,
    upsert_behavior_graph_refs,
)

logger = get_logger("behavior_pattern_store")

ACTOR_OTHER_USER = "other_user"
ACTOR_MAIBOT_SELF = "maibot_self"
ACTOR_GROUP_COLLECTIVE = "group_collective"
ACTOR_UNKNOWN = "unknown"
LEARNING_OBSERVED = "observed_behavior"
LEARNING_SELF_REFLECTION = "self_reflection"
ALLOWED_ACTOR_TYPES = {ACTOR_OTHER_USER, ACTOR_MAIBOT_SELF, ACTOR_GROUP_COLLECTIVE, ACTOR_UNKNOWN}
ALLOWED_LEARNING_TYPES = {LEARNING_OBSERVED, LEARNING_SELF_REFLECTION}
EVIDENCE_HISTORY_LIMIT = 20
FEEDBACK_HISTORY_LIMIT = 30
MIN_BEHAVIOR_SCORE = -6.0
MAX_BEHAVIOR_SCORE = 8.0
NEGATIVE_FEEDBACK_STATUSES = {"failed", "blocked", "abandoned"}
POSITIVE_FEEDBACK_STATUSES = {"success", "succeeded", "completed"}
PARTIAL_POSITIVE_FEEDBACK_STATUSES = {"partial_success"}
LOW_SIGNAL_SCENE_MAX_SOURCE_COUNT = 1


@dataclass(frozen=True)
class BehaviorExperienceUpsertItem:
    """待写入的一条行为经验路径。"""

    action: str
    outcome: str
    source_ids: Sequence[str]
    scenario_profile: BehaviorScenarioProfile
    scene_start: str
    actor_type: str = ACTOR_OTHER_USER
    learning_type: str = LEARNING_OBSERVED


@dataclass(frozen=True)
class _NormalizedBehaviorExperienceItem:
    action: str
    outcome: str
    source_ids: list[str]
    scenario_profile: BehaviorScenarioProfile
    scene_start: str
    actor_type: str
    learning_type: str


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


def _coerce_actor_type(actor_type: str) -> str:
    normalized_actor_type = str(actor_type or "").strip().lower()
    if normalized_actor_type in ALLOWED_ACTOR_TYPES:
        return normalized_actor_type
    return ACTOR_UNKNOWN


def _coerce_learning_type(learning_type: str, *, actor_type: str) -> str:
    normalized_learning_type = str(learning_type or "").strip().lower()
    if normalized_learning_type in ALLOWED_LEARNING_TYPES:
        return normalized_learning_type
    return LEARNING_SELF_REFLECTION if actor_type == ACTOR_MAIBOT_SELF else LEARNING_OBSERVED


def _build_evidence_item(
    *,
    action: str,
    outcome: str,
    source_ids: Sequence[str],
    actor_type: str,
    learning_type: str,
    profile_tag_distribution: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    evidence_item = {
        "action": action,
        "outcome": outcome,
        "source_ids": _normalize_source_ids(source_ids),
        "actor_type": actor_type,
        "learning_type": learning_type,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    if profile_tag_distribution:
        evidence_item["profile_tag_distribution"] = list(profile_tag_distribution)
    return evidence_item


def _normalize_behavior_experience_item(
    item: BehaviorExperienceUpsertItem,
    *,
    profile_source_count: int | None = None,
) -> Optional[_NormalizedBehaviorExperienceItem]:
    normalized_action = _normalize_text(item.action, max_length=240)
    normalized_outcome = _normalize_text(item.outcome, max_length=240)
    normalized_source_ids = _normalize_source_ids(item.source_ids)
    normalized_actor_type = _coerce_actor_type(item.actor_type)
    normalized_learning_type = _coerce_learning_type(item.learning_type, actor_type=normalized_actor_type)
    if not normalized_action or not normalized_outcome:
        logger.warning(
            "跳过写入行为经验路径：归一化后字段为空 "
            f"action={normalized_action!r} outcome={normalized_outcome!r}"
        )
        return None
    if _is_low_signal_scenario_profile(
        item.scenario_profile,
        source_count=profile_source_count if profile_source_count is not None else len(normalized_source_ids),
    ):
        logger.info(
            "跳过写入行为经验路径：场景画像信号过少 "
            f"source_count={profile_source_count if profile_source_count is not None else len(normalized_source_ids)} "
            f"action={normalized_action!r}"
        )
        return None

    return _NormalizedBehaviorExperienceItem(
        action=normalized_action,
        outcome=normalized_outcome,
        source_ids=normalized_source_ids,
        scenario_profile=item.scenario_profile,
        scene_start=item.scene_start,
        actor_type=normalized_actor_type,
        learning_type=normalized_learning_type,
    )


def _normalize_profile_tag_kind(raw_value: object) -> str:
    return " ".join(str(raw_value or "").lower().split()).strip()


def _is_low_signal_scenario_profile(
    profile: BehaviorScenarioProfile,
    *,
    source_count: int,
) -> bool:
    """过滤后只有单个 domain 簇且证据不足时，不写入新行为经验。"""

    if not profile.has_signal:
        return True

    domain_cluster_count = 0
    other_signal_count = 0
    for cluster in profile.tag_clusters:
        if not cluster.all_values():
            continue
        if _normalize_profile_tag_kind(cluster.kind) == "domain":
            domain_cluster_count += 1
        else:
            other_signal_count += 1

    if domain_cluster_count == 0:
        return True
    return (
        domain_cluster_count == 1
        and other_signal_count == 0
        and source_count <= LOW_SIGNAL_SCENE_MAX_SOURCE_COUNT
    )


def _merge_profile_tag_distribution_from_evidence(evidence_list: Any) -> list[dict[str, float | str]]:
    tag_totals: dict[str, float] = {}
    distribution_count = 0
    for evidence_item in _load_json_list(evidence_list):
        if not isinstance(evidence_item, dict):
            continue
        raw_distribution = evidence_item.get("profile_tag_distribution")
        if not isinstance(raw_distribution, list):
            continue
        local_tags: dict[str, float] = {}
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
                local_tags[tag] = local_tags.get(tag, 0.0) + probability
        if not local_tags:
            continue
        distribution_count += 1
        for tag, probability in local_tags.items():
            tag_totals[tag] = tag_totals.get(tag, 0.0) + probability
    if distribution_count <= 0:
        return []
    averaged_tags = {tag: probability / float(distribution_count) for tag, probability in tag_totals.items()}
    total_probability = sum(averaged_tags.values())
    if total_probability <= 0:
        return []
    return [
        {
            "tag": tag,
            "probability": round(probability / total_probability, 6),
        }
        for tag, probability in sorted(averaged_tags.items())
    ]


def _path_texts_from_session(
    session: Session,
    path: BehaviorExperiencePath,
) -> tuple[str, str]:
    action = session.get(BehaviorAction, path.action_id)
    outcome = session.get(BehaviorOutcome, path.outcome_id)
    action_text = action.action if action is not None else ""
    outcome_text = outcome.outcome if outcome is not None else ""
    return action_text, outcome_text


def _path_to_dict_from_session(
    session: Session,
    path: BehaviorExperiencePath,
) -> dict[str, Any]:
    action, outcome = _path_texts_from_session(session, path)
    return {
        "id": path.id,
        "action": action,
        "outcome": outcome,
        "scene_cluster_id": path.scene_cluster_id,
        "action_id": path.action_id,
        "outcome_id": path.outcome_id,
        "actor_type": path.actor_type,
        "learning_type": path.learning_type,
        "count": path.count,
        "activation_count": path.activation_count,
        "success_count": path.success_count,
        "failure_count": path.failure_count,
        "score": path.score,
        "enabled": path.enabled,
        "session_id": path.session_id,
        "profile_tag_distribution": _merge_profile_tag_distribution_from_evidence(path.evidence_list),
        "last_active_time": path.last_active_time.isoformat() if path.last_active_time else "",
        "last_feedback_time": path.last_feedback_time.isoformat() if path.last_feedback_time else "",
    }


def behavior_experience_to_dict(path: BehaviorExperiencePath) -> dict[str, Any]:
    if path.id is None:
        return {
            "id": None,
            "action": "",
            "outcome": "",
            "actor_type": path.actor_type,
            "learning_type": path.learning_type,
            "count": path.count,
            "activation_count": path.activation_count,
            "success_count": path.success_count,
            "failure_count": path.failure_count,
            "score": path.score,
            "enabled": path.enabled,
            "session_id": path.session_id,
            "profile_tag_distribution": [],
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


def upsert_behavior_experience(
    *,
    action: str,
    outcome: str,
    source_ids: Sequence[str],
    session_id: str,
    scenario_profile: BehaviorScenarioProfile,
    scene_start: str,
    actor_type: str = ACTOR_OTHER_USER,
    learning_type: str = LEARNING_OBSERVED,
) -> Optional[BehaviorExperiencePath]:
    paths = upsert_behavior_experiences(
        session_id=session_id,
        items=[
            BehaviorExperienceUpsertItem(
                action=action,
                outcome=outcome,
                source_ids=source_ids,
                scenario_profile=scenario_profile,
                scene_start=scene_start,
                actor_type=actor_type,
                learning_type=learning_type,
            )
        ],
    )
    return paths[0] if paths else None


def upsert_behavior_experiences(
    *,
    session_id: str,
    items: Sequence[BehaviorExperienceUpsertItem],
) -> list[Optional[BehaviorExperiencePath]]:
    """批量写入行为经验路径，复用同一批次中相同场景画像的场景簇引用。"""

    source_ids_by_profile = _collect_source_ids_by_profile(items)
    normalized_items = [
        _normalize_behavior_experience_item(
            item,
            profile_source_count=len(source_ids_by_profile.get(id(item.scenario_profile), [])),
        )
        for item in items
    ]
    results: list[Optional[BehaviorExperiencePath]] = [None for _ in normalized_items]
    if not any(item is not None for item in normalized_items):
        return results

    try:
        with get_db_session() as session:
            write_start_time = perf_counter()
            scene_cluster_by_profile: dict[int, Any] = {}
            scene_cluster_candidates = list(session.exec(select(BehaviorSceneCluster)).all())
            tag_distribution_by_profile: dict[int, list[dict[str, float | str]]] = {}
            scene_cluster_count_before = len(scene_cluster_candidates)

            for index, normalized_item in enumerate(normalized_items):
                if normalized_item is None:
                    continue

                profile_key = id(normalized_item.scenario_profile)
                graph_refs = upsert_behavior_graph_refs(
                    session=session,
                    session_id=session_id,
                    profile=normalized_item.scenario_profile,
                    scene_start=normalized_item.scene_start,
                    action=normalized_item.action,
                    outcome=normalized_item.outcome,
                    scene_cluster=scene_cluster_by_profile.get(profile_key),
                    scene_cluster_candidates=scene_cluster_candidates,
                )
                if graph_refs is None:
                    logger.warning(
                        "跳过写入行为经验路径：场景簇引用生成失败 "
                        f"session_id={session_id} action={normalized_item.action} outcome={normalized_item.outcome}"
                    )
                    continue
                if all(candidate is not graph_refs.scene_cluster for candidate in scene_cluster_candidates):
                    scene_cluster_candidates.append(graph_refs.scene_cluster)
                scene_cluster_by_profile.setdefault(profile_key, graph_refs.scene_cluster)

                if profile_key not in tag_distribution_by_profile:
                    tag_distribution_by_profile[profile_key] = build_profile_tag_distribution(
                        normalized_item.scenario_profile,
                        tag_lookup=_load_tag_cluster_lookup(session),
                    )

                path = _upsert_normalized_behavior_experience(
                    session=session,
                    session_id=session_id,
                    item=normalized_item,
                    scene_cluster_id=graph_refs.scene_cluster_id,
                    action_id=graph_refs.action_id,
                    outcome_id=graph_refs.outcome_id,
                    profile_tag_distribution=tag_distribution_by_profile[profile_key],
                )
                if path is None:
                    continue
                session.flush()
                session.refresh(path)
                session.expunge(path)
                results[index] = path
            elapsed_ms = int((perf_counter() - write_start_time) * 1000)
            if elapsed_ms >= 1000:
                logger.info(
                    "行为经验路径批量写入耗时: "
                    f"session_id={session_id} 候选={len(items)} "
                    f"成功={sum(path is not None for path in results)} "
                    f"场景画像={len(scene_cluster_by_profile)} "
                    f"预加载场景簇={scene_cluster_count_before} "
                    f"耗时={elapsed_ms}ms"
                )
            return results
    except Exception as exc:
        logger.exception(
            "写入行为经验路径失败: "
            f"session_id={session_id} count={len(items)} error={exc}"
        )
        return results


def _collect_source_ids_by_profile(items: Sequence[BehaviorExperienceUpsertItem]) -> dict[int, list[str]]:
    source_ids_by_profile: dict[int, list[str]] = {}
    for item in items:
        profile_key = id(item.scenario_profile)
        source_ids = source_ids_by_profile.setdefault(profile_key, [])
        for source_id in _normalize_source_ids(item.source_ids):
            if source_id not in source_ids:
                source_ids.append(source_id)
    return source_ids_by_profile


def _upsert_normalized_behavior_experience(
    *,
    session: Session,
    session_id: str,
    item: _NormalizedBehaviorExperienceItem,
    scene_cluster_id: int,
    action_id: int,
    outcome_id: int,
    profile_tag_distribution: Sequence[dict[str, Any]],
) -> Optional[BehaviorExperiencePath]:
    now = datetime.now()
    evidence_item = _build_evidence_item(
        action=item.action,
        outcome=item.outcome,
        source_ids=item.source_ids,
        actor_type=item.actor_type,
        learning_type=item.learning_type,
        profile_tag_distribution=profile_tag_distribution,
    )

    statement = (
        select(BehaviorExperiencePath)
        .where(BehaviorExperiencePath.session_id == session_id)
        .where(BehaviorExperiencePath.scene_cluster_id == scene_cluster_id)
        .where(BehaviorExperiencePath.action_id == action_id)
        .where(BehaviorExperiencePath.outcome_id == outcome_id)
        .where(BehaviorExperiencePath.actor_type == item.actor_type)
        .where(BehaviorExperiencePath.learning_type == item.learning_type)
    )
    path = session.exec(statement).first()
    if path is None:
        path = BehaviorExperiencePath(
            session_id=session_id,
            scene_cluster_id=scene_cluster_id,
            action_id=action_id,
            outcome_id=outcome_id,
            actor_type=item.actor_type,
            learning_type=item.learning_type,
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
    return path


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
    source_ids: Sequence[str] = (),
) -> Optional[BehaviorExperiencePath]:
    normalized_status = str(status or "").strip().lower()
    normalized_reason = _normalize_text(reason, max_length=300)
    normalized_outcome = _normalize_text(outcome, max_length=240)
    normalized_source_ids = _normalize_source_ids(source_ids)
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
                    "source_ids": normalized_source_ids,
                    "created_at": now.isoformat(timespec="seconds"),
                }
            )
            path.feedback_list = _dump_json_list(feedback_items[-FEEDBACK_HISTORY_LIMIT:])
            path.score = _clamp_score(float(path.score or 0.0) + float(score_delta))
            path.last_feedback_time = now
            path.update_time = now
            if normalized_status in POSITIVE_FEEDBACK_STATUSES:
                path.success_count += 1
            elif normalized_status in PARTIAL_POSITIVE_FEEDBACK_STATUSES:
                pass
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
upsert_behavior_pattern = upsert_behavior_experience
upsert_behavior_patterns = upsert_behavior_experiences
list_behavior_patterns_for_sessions = list_behavior_experiences_for_sessions
get_behavior_pattern = get_behavior_experience
mark_behavior_pattern_selected = mark_behavior_experience_selected
