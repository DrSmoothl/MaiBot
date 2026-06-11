from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional, Sequence

from sqlmodel import Session, select

import hashlib
import json
import uuid

from src.common.database.database import get_db_session
from src.common.database.database_model import (
    BehaviorAction,
    BehaviorExperiencePath,
    BehaviorOutcome,
    BehaviorSceneCluster,
    BehaviorSceneTagCluster,
)
from src.common.logger import get_logger

from .behavior_scenario import BehaviorScenarioProfile, BehaviorScenarioTagCluster

logger = get_logger("behavior_scene_cluster")

MAX_SCENE_CLUSTER_BEHAVIOR_IDS = 48
SCENE_CLUSTER_REUSE_THRESHOLD = 0.72
MIN_TAG_CLUSTER_MERGE_OVERLAP = 2
MAX_TAG_CLUSTER_MEMBERS = 24
DIRECT_DOMAIN_OVERLAP_THRESHOLD = 0.30
DIRECT_DOMAIN_OVERLAP_TOPK = 8

BehaviorSceneRetrievalMode = Literal["direct_domain_overlap"]
DEFAULT_BEHAVIOR_SCENE_RETRIEVAL_MODE: BehaviorSceneRetrievalMode = "direct_domain_overlap"


def _normalize_retrieval_mode(mode: str | None) -> BehaviorSceneRetrievalMode:
    if mode == "direct_domain_overlap":
        return mode
    return DEFAULT_BEHAVIOR_SCENE_RETRIEVAL_MODE


def _normalize_name(value: str, *, max_length: int = 160) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if len(normalized) <= max_length:
        return normalized
    return normalized[:max_length].rstrip()


def _normalize_display_text(value: str, *, max_length: int = 180) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    if len(normalized) <= max_length:
        return normalized
    return normalized[:max_length].rstrip()


def _text_hash(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


TAG_KIND_ALIASES = {
    "attitude": "attitude",
    "domain": "domain",
    "need": "need",
    "other_attitude": "attitude",
    "other_traits": "attitude",
}

TAG_KIND_WEIGHTS = {
    "attitude": 1.1,
    "domain": 1.25,
    "need": 1.25,
}


def _normalize_tag_kind(tag_kind: str) -> str:
    normalized_kind = _normalize_name(tag_kind, max_length=40)
    return TAG_KIND_ALIASES.get(normalized_kind, normalized_kind)


def _normalize_tag_value(value: str) -> str:
    display_value = _normalize_display_text(value, max_length=80)
    return _normalize_name(display_value, max_length=80)


def _load_tag_cluster_lookup(session: Session) -> dict[tuple[str, str], str]:
    rows = session.exec(select(BehaviorSceneTagCluster)).all()
    return {
        (row.tag_kind, row.tag): row.cluster_key
        for row in rows
        if row.tag_kind and row.tag and row.cluster_key
    }


def _tag_cluster_values(cluster: BehaviorScenarioTagCluster) -> list[str]:
    values: list[str] = []
    for value in cluster.tags:
        display_value = _normalize_display_text(value, max_length=80)
        if display_value and display_value not in values:
            values.append(display_value)
    return values


def _select_tag_cluster_rows(
    session: Session,
    *,
    tag_kind: str,
    normalized_tags: set[str],
) -> list[BehaviorSceneTagCluster]:
    if not tag_kind or not normalized_tags:
        return []
    rows = session.exec(
        select(BehaviorSceneTagCluster)
        .where(BehaviorSceneTagCluster.tag_kind == tag_kind)
        .where(BehaviorSceneTagCluster.tag.in_(normalized_tags))  # type: ignore[attr-defined]
    ).all()
    return list(rows)


def _select_tag_cluster_rows_by_keys(
    session: Session,
    *,
    tag_kind: str,
    cluster_keys: set[str],
) -> list[BehaviorSceneTagCluster]:
    if not tag_kind or not cluster_keys:
        return []
    rows = session.exec(
        select(BehaviorSceneTagCluster)
        .where(BehaviorSceneTagCluster.tag_kind == tag_kind)
        .where(BehaviorSceneTagCluster.cluster_key.in_(cluster_keys))  # type: ignore[attr-defined]
    ).all()
    return list(rows)


def _new_tag_cluster_key() -> str:
    return f"tc_{uuid.uuid4().hex}"


def _choose_merge_tag_cluster_key(
    *,
    values: Sequence[str],
    existing_rows: Sequence[BehaviorSceneTagCluster],
) -> str:
    incoming_tags = {_normalize_tag_value(value) for value in values if _normalize_tag_value(value)}
    rows_by_cluster: dict[str, list[BehaviorSceneTagCluster]] = {}
    for row in existing_rows:
        if not row.cluster_key:
            continue
        rows_by_cluster.setdefault(row.cluster_key, []).append(row)

    best_key = ""
    best_score = -1
    for cluster_key, rows in rows_by_cluster.items():
        row_tags = {row.tag for row in rows if row.tag}
        overlap_count = len(incoming_tags & row_tags)
        if overlap_count < MIN_TAG_CLUSTER_MERGE_OVERLAP:
            continue
        if overlap_count > best_score:
            best_key = cluster_key
            best_score = overlap_count
    return best_key


def _upsert_profile_tag_clusters(session: Session, profile: BehaviorScenarioProfile) -> None:
    if not profile.tag_clusters:
        return

    now = datetime.now()
    for cluster in profile.tag_clusters:
        tag_kind = _normalize_tag_kind(cluster.kind)
        if tag_kind not in TAG_KIND_WEIGHTS:
            continue
        values = _tag_cluster_values(cluster)
        if not values:
            continue

        normalized_tags = {_normalize_tag_value(value) for value in values if _normalize_tag_value(value)}
        existing_rows = _select_tag_cluster_rows(session, tag_kind=tag_kind, normalized_tags=normalized_tags)
        existing_keys = {row.cluster_key for row in existing_rows if row.cluster_key}
        related_rows = _select_tag_cluster_rows_by_keys(
            session,
            tag_kind=tag_kind,
            cluster_keys=existing_keys,
        )
        related_rows_by_id = {id(row): row for row in [*existing_rows, *related_rows]}
        candidate_rows = list(related_rows_by_id.values())
        merge_cluster_key = _choose_merge_tag_cluster_key(values=values, existing_rows=candidate_rows)
        selected_rows = [row for row in candidate_rows if row.cluster_key == merge_cluster_key] if merge_cluster_key else []
        if selected_rows:
            chosen_row = max(selected_rows, key=lambda row: int(row.source_count or 0))
            cluster_key = chosen_row.cluster_key
        else:
            cluster_key = _new_tag_cluster_key()
        if not cluster_key:
            continue

        members: list[str] = []
        for value in values:
            normalized_value = _normalize_tag_value(value)
            if normalized_value and normalized_value not in members:
                members.append(normalized_value)
        for row in selected_rows:
            if row.tag and row.tag not in members:
                members.append(row.tag)
            if len(members) >= MAX_TAG_CLUSTER_MEMBERS:
                break
        members = members[:MAX_TAG_CLUSTER_MEMBERS]
        row_by_key = {(row.tag_kind, row.tag): row for row in selected_rows}
        blocked_row_keys = {
            (row.tag_kind, row.tag)
            for row in existing_rows
            if row.cluster_key != cluster_key and row.tag_kind and row.tag
        }
        for member in members:
            normalized_member = _normalize_tag_value(member)
            if not normalized_member:
                continue
            row_key = (tag_kind, normalized_member)
            if row_key in blocked_row_keys:
                continue
            row = row_by_key.get(row_key)
            if row is None:
                row = BehaviorSceneTagCluster(
                    tag_kind=tag_kind,
                    tag=normalized_member,
                    cluster_key=cluster_key,
                    source_count=1,
                    update_time=now,
                )
            else:
                row.tag = normalized_member
                row.cluster_key = cluster_key
                row.source_count = int(row.source_count or 0) + 1
                row.update_time = now
            session.add(row)
    session.flush()


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, value))


def _normalize_tag_name(
    tag_kind: str,
    value: str,
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
    normalized_kind = _normalize_tag_kind(tag_kind)
    normalized_key = _normalize_tag_value(value)
    if normalized_kind not in TAG_KIND_WEIGHTS or not normalized_key:
        return ""
    cluster_key = (tag_lookup or {}).get((normalized_kind, normalized_key), normalized_key)
    return f"{normalized_kind}:{cluster_key}"


def _normalize_stored_tag_name(
    tag_name: str,
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
    normalized_tag = str(tag_name or "").strip()
    if ":" not in normalized_tag:
        return ""
    tag_kind, tag_value = normalized_tag.split(":", 1)
    normalized_kind = _normalize_tag_kind(tag_kind)
    if normalized_kind not in TAG_KIND_WEIGHTS:
        return ""
    normalized_value = _normalize_tag_value(tag_value)
    if normalized_kind and normalized_value.startswith("tc_"):
        return f"{normalized_kind}:{normalized_value}"
    return _normalize_tag_name(tag_kind, tag_value, tag_lookup=tag_lookup)


def _build_cluster_tag_weights(
    profile: BehaviorScenarioProfile,
    *,
    allowed_kinds: set[str] | None = None,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> dict[str, float]:
    tag_weights: dict[str, float] = {}

    for cluster in profile.tag_clusters:
        values = _tag_cluster_values(cluster)
        if not values:
            continue
        normalized_kind = _normalize_tag_kind(cluster.kind)
        if normalized_kind not in TAG_KIND_WEIGHTS:
            continue
        if allowed_kinds is not None and normalized_kind not in allowed_kinds:
            continue
        normalized_values = []
        for value in values:
            normalized_value = _normalize_tag_value(value)
            if normalized_value and normalized_value not in normalized_values:
                normalized_values.append(normalized_value)
        if not normalized_values:
            continue
        mapped_cluster_key = ""
        for normalized_value in normalized_values:
            mapped_cluster_key = (tag_lookup or {}).get((normalized_kind, normalized_value), "")
            if mapped_cluster_key:
                break
        cluster_key = mapped_cluster_key or normalized_values[0]
        tag_name = f"{normalized_kind}:{cluster_key}"
        tag_weights[tag_name] = max(tag_weights.get(tag_name, 0.0), TAG_KIND_WEIGHTS[normalized_kind])
    return tag_weights


def build_scene_cluster_distribution(
    profile: BehaviorScenarioProfile,
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> list[dict[str, float | str]]:
    """将场景画像转成 domain-only tag 概率分布，用于匹配稳定场景簇。"""

    tag_weights = _build_cluster_tag_weights(profile, allowed_kinds={"domain"}, tag_lookup=tag_lookup)
    return _tag_weights_to_distribution(tag_weights)


def build_profile_tag_distribution(
    profile: BehaviorScenarioProfile,
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> list[dict[str, float | str]]:
    """将完整场景画像转成 tag 概率分布，保留 need/attitude 供路径调制使用。"""

    tag_weights = _build_cluster_tag_weights(profile, tag_lookup=tag_lookup)
    return _tag_weights_to_distribution(tag_weights)


def _tag_weights_to_distribution(tag_weights: dict[str, float]) -> list[dict[str, float | str]]:
    total_weight = sum(tag_weights.values())
    if total_weight <= 0:
        return []
    return [
        {
            "tag": tag,
            "probability": round(weight / total_weight, 6),
        }
        for tag, weight in sorted(tag_weights.items())
    ]


def _distribution_to_mapping(
    distribution: Sequence[dict[str, Any]],
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> dict[str, float]:
    tag_probs: dict[str, float] = {}
    for item in distribution:
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag") or "").strip()
        if not tag:
            continue
        tag = _normalize_stored_tag_name(tag, tag_lookup=tag_lookup)
        if not tag:
            continue
        tag_kind, _ = tag.split(":", 1)
        if tag_kind not in TAG_KIND_WEIGHTS:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability <= 0:
            continue
        tag_probs[tag] = tag_probs.get(tag, 0.0) + probability
    total_probability = sum(tag_probs.values())
    if total_probability <= 0:
        return {}
    return {tag: probability / total_probability for tag, probability in tag_probs.items()}


def _mapping_to_distribution(tag_probs: dict[str, float]) -> list[dict[str, float | str]]:
    total_probability = sum(max(probability, 0.0) for probability in tag_probs.values())
    if total_probability <= 0:
        return []
    return [
        {
            "tag": tag,
            "probability": round(max(probability, 0.0) / total_probability, 6),
        }
        for tag, probability in sorted(tag_probs.items())
        if probability > 0
    ]


def _dump_cluster_distribution(distribution: Sequence[dict[str, Any]]) -> str:
    return json.dumps(list(distribution), ensure_ascii=False, sort_keys=True)


def _load_cluster_distribution(raw_value: Any) -> list[dict[str, Any]]:
    if isinstance(raw_value, list):
        return [item for item in raw_value if isinstance(item, dict)]
    if not isinstance(raw_value, str) or not raw_value.strip():
        return []
    try:
        parsed_value = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    return [item for item in parsed_value if isinstance(item, dict)] if isinstance(parsed_value, list) else []


def format_scene_cluster_distribution(
    distribution: Sequence[dict[str, Any]],
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
    """将场景簇 tag 概率分布格式化为展示文本。"""

    tag_probs = _distribution_to_mapping(distribution, tag_lookup=tag_lookup)
    if not tag_probs:
        return ""
    parts = [
        f"{tag}={probability:.3f}"
        for tag, probability in sorted(tag_probs.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    return "；".join(parts)


def _cluster_distribution_overlap(
    left_distribution: Sequence[dict[str, Any]],
    right_distribution: Sequence[dict[str, Any]],
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> float:
    left_probs = _distribution_to_mapping(left_distribution, tag_lookup=tag_lookup)
    right_probs = _distribution_to_mapping(right_distribution, tag_lookup=tag_lookup)
    if not left_probs or not right_probs:
        return 0.0
    shared_tags = set(left_probs) & set(right_probs)
    return round(sum(min(left_probs[tag], right_probs[tag]) for tag in shared_tags), 4)


def _session_scope_condition(model: Any, session_ids: set[str]):
    if session_ids:
        return (model.session_id.in_(session_ids)) | (model.session_id.is_(None))  # type: ignore[attr-defined]
    return model.session_id.is_(None)  # type: ignore[attr-defined]


@dataclass(frozen=True)
class BehaviorGraphRefs:
    """一次行为经验路径对应的场景簇、动作和结果引用。"""

    scene_cluster: BehaviorSceneCluster
    scene_cluster_id: int
    action_id: int
    outcome_id: int


def upsert_behavior_graph_refs(
    *,
    session: Session,
    session_id: str,
    profile: BehaviorScenarioProfile,
    scene_start: str,
    action: str,
    outcome: str,
) -> Optional[BehaviorGraphRefs]:
    """写入场景簇、动作和结果，并返回可用于创建经验路径的引用。"""

    normalized_action = _normalize_display_text(action, max_length=240)
    normalized_outcome = _normalize_display_text(outcome, max_length=220)
    scene_cluster = _upsert_scene_cluster(session, session_id=session_id, profile=profile)
    del scene_start
    if scene_cluster is None or scene_cluster.id is None:
        return None
    if not normalized_action or not normalized_outcome:
        return None

    action = _upsert_action(session, session_id=session_id, action=normalized_action)
    outcome = _upsert_outcome(session, session_id=session_id, outcome=normalized_outcome)
    if action.id is None or outcome.id is None:
        return None

    return BehaviorGraphRefs(
        scene_cluster=scene_cluster,
        scene_cluster_id=int(scene_cluster.id),
        action_id=int(action.id),
        outcome_id=int(outcome.id),
    )


def retrieve_behavior_scores_from_scene_clusters(
    *,
    session_ids: set[str],
    include_global: bool,
    profile: BehaviorScenarioProfile,
    max_count: int = MAX_SCENE_CLUSTER_BEHAVIOR_IDS,
    retrieval_mode: BehaviorSceneRetrievalMode = DEFAULT_BEHAVIOR_SCENE_RETRIEVAL_MODE,
) -> dict[int, float]:
    """根据当前场景画像匹配场景簇，召回行为经验路径 ID 及分数。"""

    if not profile.tag_clusters:
        return {}

    active_retrieval_mode = _normalize_retrieval_mode(retrieval_mode)
    try:
        with get_db_session(auto_commit=False) as session:
            behavior_scores: dict[int, float] = {}
            if active_retrieval_mode == "direct_domain_overlap":
                cluster_scores, _ = _score_scene_clusters_by_direct_domain_overlap(
                    session,
                    profile=profile,
                    session_ids=session_ids,
                    include_global=include_global,
                )
                behavior_cluster_scores = _score_behavior_clusters(
                    session,
                    cluster_scores=cluster_scores,
                    session_ids=session_ids,
                    include_global=include_global,
                )
                for experience_path_id, score in behavior_cluster_scores.items():
                    behavior_scores[experience_path_id] = behavior_scores.get(experience_path_id, 0.0) + score
    except Exception as exc:
        logger.error(f"行为场景簇检索失败: session_ids={session_ids} mode={active_retrieval_mode} error={exc}")
        return {}

    return dict(sorted(behavior_scores.items(), key=lambda item: item[1], reverse=True)[:max_count])


def debug_retrieve_behavior_scores_from_scene_clusters(
    *,
    session_ids: set[str],
    include_global: bool,
    profile: BehaviorScenarioProfile,
    max_count: int = MAX_SCENE_CLUSTER_BEHAVIOR_IDS,
    retrieval_mode: BehaviorSceneRetrievalMode = DEFAULT_BEHAVIOR_SCENE_RETRIEVAL_MODE,
) -> dict[str, Any]:
    """返回行为场景簇检索的中间过程，供 WebUI 浏览和调试。"""

    if not profile.tag_clusters:
        return {
            "retrieval_mode": _normalize_retrieval_mode(retrieval_mode),
            "descriptors": [],
            "matched_clusters": [],
            "candidate_scores": [],
        }

    active_retrieval_mode = _normalize_retrieval_mode(retrieval_mode)
    try:
        with get_db_session(auto_commit=False) as session:
            behavior_scores: dict[int, float] = {}

            cluster_scores: dict[int, float] = {}
            direct_overlap_debug: dict[str, Any] = {}
            if active_retrieval_mode == "direct_domain_overlap":
                cluster_scores, direct_overlap_debug = _score_scene_clusters_by_direct_domain_overlap(
                    session,
                    profile=profile,
                    session_ids=session_ids,
                    include_global=include_global,
                )
                behavior_cluster_scores = _score_behavior_clusters(
                    session,
                    cluster_scores=cluster_scores,
                    session_ids=session_ids,
                    include_global=include_global,
                )
                for experience_path_id, score in behavior_cluster_scores.items():
                    behavior_scores[experience_path_id] = behavior_scores.get(experience_path_id, 0.0) + score

        candidate_scores = sorted(behavior_scores.items(), key=lambda item: item[1], reverse=True)[:max_count]
        return {
            "retrieval_mode": active_retrieval_mode,
            "descriptors": [],
            "matched_clusters": _debug_cluster_scores(cluster_scores),
            "candidate_scores": [
                {"behavior_id": experience_path_id, "score": round(score, 4)}
                for experience_path_id, score in candidate_scores
            ],
            "direct_domain_overlap": direct_overlap_debug,
        }
    except Exception as exc:
        logger.error(f"行为场景簇调试检索失败: session_ids={session_ids} mode={active_retrieval_mode} error={exc}")
        return {
            "retrieval_mode": active_retrieval_mode,
            "descriptors": [],
            "matched_clusters": [],
            "candidate_scores": [],
            "error": str(exc),
        }


def _debug_cluster_scores(cluster_scores: dict[int, float]) -> list[dict[str, Any]]:
    if not cluster_scores:
        return []
    try:
        with get_db_session(auto_commit=False) as session:
            clusters = session.exec(
                select(BehaviorSceneCluster).where(BehaviorSceneCluster.id.in_(set(cluster_scores)))  # type: ignore[attr-defined]
            ).all()
            cluster_by_id = {cluster.id: cluster for cluster in clusters if cluster.id is not None}
    except Exception:
        cluster_by_id = {}
    return [
        {
            "cluster_id": cluster_id,
            "name": format_scene_cluster_distribution(
                _load_cluster_distribution(cluster_by_id[cluster_id].tag_distribution)
            )
            if cluster_id in cluster_by_id
            else "",
            "score": round(score, 4),
        }
        for cluster_id, score in sorted(cluster_scores.items(), key=lambda item: item[1], reverse=True)
    ]


def mark_behavior_scene_links_selected(experience_path_id: int) -> None:
    """行为被选中后，刷新其所属场景簇的活跃度。"""

    if experience_path_id <= 0:
        return
    now = datetime.now()
    try:
        with get_db_session() as session:
            path = session.get(BehaviorExperiencePath, experience_path_id)
            if path is None:
                return
            cluster = session.get(BehaviorSceneCluster, path.scene_cluster_id)
            if cluster is None:
                return
            cluster.update_time = now
            session.add(cluster)

    except Exception as exc:
        logger.error(f"更新行为场景簇选中状态失败: experience_id={experience_path_id} error={exc}")


def apply_behavior_scene_feedback(
    *,
    experience_path_id: int,
    score_delta: float,
    status: str,
) -> None:
    """反馈行为效果时，同步强化或削弱行为所属场景簇。"""

    if experience_path_id <= 0:
        return
    del status
    now = datetime.now()

    try:
        with get_db_session() as session:
            path = session.get(BehaviorExperiencePath, experience_path_id)
            if path is None:
                return
            cluster = session.get(BehaviorSceneCluster, path.scene_cluster_id)
            if cluster is None:
                return
            cluster.score = _clamp(float(cluster.score or 0.0) + float(score_delta) * 0.08, -4.0, 6.0)
            cluster.update_time = now
            session.add(cluster)

    except Exception as exc:
        logger.error(f"更新行为场景簇反馈失败: experience_id={experience_path_id} error={exc}")


def _upsert_scene_cluster(
    session: Session,
    *,
    session_id: str,
    profile: BehaviorScenarioProfile,
) -> Optional[BehaviorSceneCluster]:
    _upsert_profile_tag_clusters(session, profile)
    tag_lookup = _load_tag_cluster_lookup(session)
    distribution = build_scene_cluster_distribution(profile, tag_lookup=tag_lookup)
    if not distribution:
        return None

    cluster_candidates = session.exec(
        select(BehaviorSceneCluster).where(BehaviorSceneCluster.session_id == session_id)
    ).all()
    best_cluster: Optional[BehaviorSceneCluster] = None
    best_overlap = 0.0
    for candidate in cluster_candidates:
        overlap = _cluster_distribution_overlap(
            _load_cluster_distribution(candidate.tag_distribution),
            distribution,
            tag_lookup=tag_lookup,
        )
        if overlap > best_overlap:
            best_cluster = candidate
            best_overlap = overlap
    cluster = best_cluster if best_cluster is not None and best_overlap >= SCENE_CLUSTER_REUSE_THRESHOLD else None

    now = datetime.now()
    if cluster is None:
        cluster = BehaviorSceneCluster(
            session_id=session_id,
            tag_distribution=_dump_cluster_distribution(distribution),
            source_count=1,
            update_time=now,
        )
    else:
        cluster.tag_distribution = _merge_cluster_distributions(
            _load_cluster_distribution(cluster.tag_distribution),
            distribution,
            existing_weight=max(int(cluster.source_count or 0), 1),
            tag_lookup=tag_lookup,
        )
        cluster.source_count += 1
        cluster.update_time = now
    session.add(cluster)
    session.flush()
    return cluster


def _merge_cluster_distributions(
    existing_distribution: Sequence[dict[str, Any]],
    new_distribution: Sequence[dict[str, Any]],
    *,
    existing_weight: int,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
    existing_probs = _distribution_to_mapping(existing_distribution, tag_lookup=tag_lookup)
    new_probs = _distribution_to_mapping(new_distribution, tag_lookup=tag_lookup)
    if not existing_probs:
        return _dump_cluster_distribution(new_distribution)
    merged_probs: dict[str, float] = {}
    all_tags = set(existing_probs) | set(new_probs)
    for tag in all_tags:
        merged_probs[tag] = (
            existing_probs.get(tag, 0.0) * float(existing_weight)
            + new_probs.get(tag, 0.0)
        ) / (float(existing_weight) + 1.0)
    return _dump_cluster_distribution(_mapping_to_distribution(merged_probs))


def _upsert_action(
    session: Session,
    *,
    session_id: str,
    action: str,
) -> BehaviorAction:
    normalized_action = _normalize_display_text(action, max_length=240)
    action_hash = _text_hash(normalized_action)
    statement = (
        select(BehaviorAction)
        .where(BehaviorAction.session_id == session_id)
        .where(BehaviorAction.action_hash == action_hash)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorAction(
            session_id=session_id,
            action=normalized_action,
            action_hash=action_hash,
            source_count=1,
            create_time=now,
            update_time=now,
        )
    else:
        node.source_count += 1
        node.action = normalized_action
        node.update_time = now
    session.add(node)
    session.flush()
    return node


def _upsert_outcome(
    session: Session,
    *,
    session_id: str,
    outcome: str,
) -> BehaviorOutcome:
    normalized_outcome = _normalize_display_text(outcome, max_length=220)
    outcome_hash = _text_hash(normalized_outcome)
    statement = (
        select(BehaviorOutcome)
        .where(BehaviorOutcome.session_id == session_id)
        .where(BehaviorOutcome.outcome_hash == outcome_hash)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorOutcome(
            session_id=session_id,
            outcome=normalized_outcome,
            outcome_hash=outcome_hash,
            source_count=1,
            create_time=now,
            update_time=now,
        )
    else:
        node.source_count += 1
        node.outcome = normalized_outcome
        node.update_time = now
    session.add(node)
    session.flush()
    return node


def _score_scene_clusters_by_direct_domain_overlap(
    session: Session,
    *,
    profile: BehaviorScenarioProfile,
    session_ids: set[str],
    include_global: bool,
) -> tuple[dict[int, float], dict[str, Any]]:
    tag_lookup = _load_tag_cluster_lookup(session)
    direct_tags = _distribution_to_mapping(
        build_scene_cluster_distribution(profile, tag_lookup=tag_lookup),
        tag_lookup=tag_lookup,
    )
    if not direct_tags:
        return {}, {"direct_tag_count": 0, "cluster_count": 0}

    statement = select(BehaviorSceneCluster)
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorSceneCluster, session_ids))

    cluster_scores: dict[int, float] = {}
    for cluster in session.exec(statement).all():
        if cluster.id is None:
            continue
        cluster_tags = _distribution_to_mapping(
            _load_cluster_distribution(cluster.tag_distribution),
            tag_lookup=tag_lookup,
        )
        if not cluster_tags:
            continue
        direct_shared_tags = set(direct_tags) & set(cluster_tags)
        if not direct_shared_tags:
            continue
        direct_overlap = sum(min(direct_tags[tag], cluster_tags[tag]) for tag in direct_shared_tags)
        score = direct_overlap
        if score < DIRECT_DOMAIN_OVERLAP_THRESHOLD:
            continue
        cluster_scores[int(cluster.id)] = round(score * 2.0, 4)

    sorted_cluster_scores = dict(
        sorted(cluster_scores.items(), key=lambda item: item[1], reverse=True)[:DIRECT_DOMAIN_OVERLAP_TOPK]
    )
    debug_payload = {
        "direct_tag_count": len(direct_tags),
        "cluster_count": len(sorted_cluster_scores),
    }
    return sorted_cluster_scores, debug_payload


def _score_behavior_clusters(
    session: Session,
    *,
    cluster_scores: dict[int, float],
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    if not cluster_scores:
        return {}

    statement = select(BehaviorExperiencePath).where(
        BehaviorExperiencePath.scene_cluster_id.in_(set(cluster_scores))  # type: ignore[attr-defined]
    )
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorExperiencePath, session_ids))

    behavior_scores: dict[int, float] = {}
    for path in session.exec(statement).all():
        if path.id is None or not path.enabled:
            continue
        cluster_score = cluster_scores.get(path.scene_cluster_id, 0.0)
        if cluster_score <= 0:
            continue
        history_bonus = 1.0 + min(float(path.count or 0), 20.0) * 0.02
        behavior_scores[path.id] = behavior_scores.get(path.id, 0.0) + cluster_score * history_bonus
    return behavior_scores
