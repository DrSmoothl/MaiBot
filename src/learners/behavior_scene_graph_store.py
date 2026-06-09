from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import or_
from sqlmodel import Session, select

import json
import uuid

from src.common.database.database import get_db_session
from src.common.database.database_model import (
    BehaviorActionNode,
    BehaviorActionOutcomeEdge,
    BehaviorExperiencePath,
    BehaviorExperienceSceneLink,
    BehaviorOutcomeNode,
    BehaviorSceneActionEdge,
    BehaviorSceneCluster,
    BehaviorSceneEdge,
    BehaviorSceneNode,
    BehaviorSceneNodeTag,
    BehaviorSceneTagCluster,
)
from src.common.logger import get_logger

from .behavior_scenario import BehaviorScenarioProfile, BehaviorScenarioTagCluster

logger = get_logger("behavior_scene_graph")

MAX_SCENE_GRAPH_NODES_PER_PROFILE = 12
MAX_SCENE_GRAPH_MATCHED_NODES = 24
MAX_SCENE_GRAPH_BEHAVIOR_IDS = 48
SCENE_EDGE_SPREAD_FACTOR = 0.35
MIN_LINK_WEIGHT = 0.1
MAX_LINK_WEIGHT = 8.0
MIN_NODE_SCORE = -4.0
MAX_NODE_SCORE = 6.0
SCENE_CLUSTER_MATCH_THRESHOLD = 0.58
SCENE_CLUSTER_REUSE_THRESHOLD = 0.72
MIN_TAG_CLUSTER_MERGE_OVERLAP = 2
MAX_TAG_CLUSTER_MEMBERS = 24


@dataclass(frozen=True)
class SceneDescriptor:
    """从场景画像中抽取出的可落库节点描述。"""

    node_kind: str
    name: str
    weight: float = 1.0


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


TAG_KIND_ALIASES = {
    "phase": "phase",
    "domain": "domain",
    "need": "need",
    "risk": "risk",
}

TAG_KIND_WEIGHTS = {
    "phase": 1.4,
    "domain": 1.0,
    "need": 1.1,
    "risk": 0.8,
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

        members = [_normalize_tag_value(value) for value in values if _normalize_tag_value(value)]
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
    if not normalized_kind or not normalized_key:
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
    normalized_value = _normalize_tag_value(tag_value)
    if normalized_kind and normalized_value.startswith("tc_"):
        return f"{normalized_kind}:{normalized_value}"
    return _normalize_tag_name(tag_kind, tag_value, tag_lookup=tag_lookup)


def _build_cluster_tag_weights(
    profile: BehaviorScenarioProfile,
    *,
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
    """将场景画像转成 tag 概率分布，用于匹配稳定场景簇。"""

    tag_weights = _build_cluster_tag_weights(profile, tag_lookup=tag_lookup)
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


def _distribution_tag_entries(distribution: Sequence[dict[str, Any]]) -> list[tuple[str, str, float]]:
    entries: list[tuple[str, str, float]] = []
    for item in distribution:
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag") or "").strip()
        if ":" not in tag:
            continue
        tag_kind, cluster_key = tag.split(":", 1)
        tag_kind = _normalize_tag_kind(tag_kind)
        cluster_key = _normalize_tag_value(cluster_key)
        if not tag_kind or not cluster_key:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability <= 0:
            continue
        entries.append((tag_kind, cluster_key, probability))
    return entries


def _cluster_normalized_tags(
    distribution: Sequence[dict[str, Any]],
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
    return "|".join(sorted(_distribution_to_mapping(distribution, tag_lookup=tag_lookup)))


def _cluster_name_from_distribution(
    distribution: Sequence[dict[str, Any]],
    *,
    tag_lookup: dict[tuple[str, str], str] | None = None,
) -> str:
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


def build_scene_descriptors(
    profile: BehaviorScenarioProfile,
    *,
    scene_start: str = "",
) -> list[SceneDescriptor]:
    """将场景画像拆成稳定、可复用的图节点描述。"""

    descriptors: list[SceneDescriptor] = []
    seen_keys: set[tuple[str, str]] = set()

    def add(node_kind: str, name: str, *, weight: float = 1.0) -> None:
        display_name = _normalize_display_text(name)
        normalized_name = _normalize_name(display_name)
        if not display_name or len(display_name) < 2:
            return
        key = (node_kind, normalized_name)
        if key in seen_keys:
            return
        seen_keys.add(key)
        descriptors.append(
            SceneDescriptor(
                node_kind=node_kind,
                name=display_name,
                weight=weight,
            )
        )

    for cluster in profile.tag_clusters:
        values = _tag_cluster_values(cluster)
        if values:
            add(cluster.kind, values[0], weight=TAG_KIND_WEIGHTS.get(cluster.kind, 1.0))

    return descriptors[:MAX_SCENE_GRAPH_NODES_PER_PROFILE]


@dataclass(frozen=True)
class BehaviorGraphRefs:
    """一次行为经验路径对应的图节点引用。"""

    scene_cluster: BehaviorSceneCluster
    scene_nodes: list[tuple[BehaviorSceneNode, SceneDescriptor]]
    scene_cluster_id: int
    action_node_id: int
    outcome_node_id: int


def upsert_behavior_graph_refs(
    *,
    session: Session,
    session_id: str,
    profile: BehaviorScenarioProfile,
    scene_start: str,
    action: str,
    outcome: str,
) -> Optional[BehaviorGraphRefs]:
    """写入场景/动作/结果节点，并返回可用于创建经验路径的节点引用。"""

    normalized_action = _normalize_display_text(action, max_length=240)
    normalized_outcome = _normalize_display_text(outcome, max_length=220)
    scene_cluster = _upsert_scene_cluster(session, session_id=session_id, profile=profile)
    descriptors = build_scene_descriptors(profile, scene_start=scene_start)
    if scene_cluster is None or scene_cluster.id is None:
        return None
    if not normalized_action or not normalized_outcome:
        return None

    scene_nodes = [
        (_upsert_scene_node(session, session_id=session_id, descriptor=descriptor), descriptor)
        for descriptor in descriptors
    ]
    scene_nodes = [(node, descriptor) for node, descriptor in scene_nodes if node.id is not None]
    if not scene_nodes:
        return None

    tag_lookup = _load_tag_cluster_lookup(session)
    tag_distribution = build_scene_cluster_distribution(profile, tag_lookup=tag_lookup)
    for node, descriptor in scene_nodes:
        if node.id is None:
            continue
        _upsert_scene_node_tags(
            session,
            session_id=session_id,
            scene_node_id=node.id,
            descriptor=descriptor,
            tag_distribution=tag_distribution,
        )

    action_node = _upsert_action_node(session, session_id=session_id, action=normalized_action)
    outcome_node = _upsert_outcome_node(session, session_id=session_id, outcome=normalized_outcome)
    if action_node.id is None or outcome_node.id is None:
        return None

    return BehaviorGraphRefs(
        scene_cluster=scene_cluster,
        scene_nodes=scene_nodes,
        scene_cluster_id=int(scene_cluster.id),
        action_node_id=int(action_node.id),
        outcome_node_id=int(outcome_node.id),
    )


def link_behavior_experience_to_scene_graph(
    *,
    session: Session,
    experience_path_id: int,
    session_id: str,
    refs: BehaviorGraphRefs,
) -> None:
    """学习行为后，将经验路径连接到本批次场景图。"""

    if experience_path_id <= 0:
        return

    primary_node = refs.scene_nodes[0][0] if refs.scene_nodes else None
    for node, descriptor in refs.scene_nodes:
        if node.id is None:
            continue
        _upsert_experience_scene_link(
            session,
            session_id=session_id,
            experience_path_id=experience_path_id,
            scene_node_id=node.id,
            link_role=descriptor.node_kind,
            weight=descriptor.weight,
        )
        _upsert_scene_action_edge(
            session,
            session_id=session_id,
            experience_path_id=experience_path_id,
            scene_node_id=node.id,
            action_node_id=refs.action_node_id,
            weight=descriptor.weight,
        )
        if primary_node is not None and node.id != primary_node.id and primary_node.id is not None:
            _upsert_scene_edge(
                session,
                session_id=session_id,
                source_scene_id=primary_node.id,
                target_scene_id=node.id,
                edge_type="co_occurs",
                weight=descriptor.weight,
            )
    _upsert_action_outcome_edge(
        session,
        session_id=session_id,
        experience_path_id=experience_path_id,
        action_node_id=refs.action_node_id,
        outcome_node_id=refs.outcome_node_id,
        weight=1.0,
    )
    logger.debug(
        f"行为经验图已写入: session_id={session_id} experience_id={experience_path_id} nodes={len(refs.scene_nodes)}"
    )


def retrieve_behavior_scores_from_scene_graph(
    *,
    session_ids: set[str],
    include_global: bool,
    profile: BehaviorScenarioProfile,
    max_count: int = MAX_SCENE_GRAPH_BEHAVIOR_IDS,
) -> dict[int, float]:
    """根据当前场景画像在场景图中召回行为经验路径 ID 及图分数。"""

    if not profile.tag_clusters:
        return {}

    try:
        with get_db_session(auto_commit=False) as session:
            active_node_scores = _score_scene_nodes_by_tag_clusters(
                session,
                profile=profile,
                session_ids=session_ids,
                include_global=include_global,
            )
            expanded_node_scores = _expand_scene_scores(
                session,
                active_node_scores=active_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            behavior_scores = _score_behavior_links(
                session,
                node_scores=expanded_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            path_scores = _score_behavior_paths(
                session,
                node_scores=expanded_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            for experience_path_id, score in path_scores.items():
                behavior_scores[experience_path_id] = behavior_scores.get(experience_path_id, 0.0) + score
            cluster_scores = _score_scene_clusters(
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
        logger.error(f"行为场景图检索失败: session_ids={session_ids} error={exc}")
        return {}

    return dict(sorted(behavior_scores.items(), key=lambda item: item[1], reverse=True)[:max_count])


def debug_retrieve_behavior_scores_from_scene_graph(
    *,
    session_ids: set[str],
    include_global: bool,
    profile: BehaviorScenarioProfile,
    max_count: int = MAX_SCENE_GRAPH_BEHAVIOR_IDS,
) -> dict[str, Any]:
    """返回行为场景图检索的中间过程，供 WebUI 浏览和调试。"""

    if not profile.tag_clusters:
        return {
            "descriptors": [],
            "matched_clusters": [],
            "matched_nodes": [],
            "expanded_nodes": [],
            "candidate_scores": [],
        }

    descriptors = build_scene_descriptors(profile)
    try:
        with get_db_session(auto_commit=False) as session:
            active_node_scores = _score_scene_nodes_by_tag_clusters(
                session,
                profile=profile,
                session_ids=session_ids,
                include_global=include_global,
            )
            node_map = _load_scene_node_map(session, set(active_node_scores))
            expanded_node_scores = _expand_scene_scores(
                session,
                active_node_scores=active_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            behavior_scores = _score_behavior_links(
                session,
                node_scores=expanded_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            path_scores = _score_behavior_paths(
                session,
                node_scores=expanded_node_scores,
                session_ids=session_ids,
                include_global=include_global,
            )
            for experience_path_id, score in path_scores.items():
                behavior_scores[experience_path_id] = behavior_scores.get(experience_path_id, 0.0) + score
            cluster_scores = _score_scene_clusters(
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

            missing_node_ids = set(expanded_node_scores) - set(node_map)
            if missing_node_ids:
                node_map.update(_load_scene_node_map(session, missing_node_ids))

        candidate_scores = sorted(behavior_scores.items(), key=lambda item: item[1], reverse=True)[:max_count]
        return {
            "descriptors": [
                {"node_kind": descriptor.node_kind, "name": descriptor.name, "weight": descriptor.weight}
                for descriptor in descriptors
            ],
            "matched_clusters": _debug_cluster_scores(cluster_scores),
            "matched_nodes": [
                _debug_scene_node_payload(node_map.get(node_id), score)
                for node_id, score in active_node_scores.items()
            ],
            "expanded_nodes": [
                _debug_scene_node_payload(node_map.get(node_id), score)
                for node_id, score in sorted(expanded_node_scores.items(), key=lambda item: item[1], reverse=True)
            ],
            "candidate_scores": [
                {"behavior_id": experience_path_id, "score": round(score, 4)}
                for experience_path_id, score in candidate_scores
            ],
        }
    except Exception as exc:
        logger.error(f"行为场景图调试检索失败: session_ids={session_ids} error={exc}")
        return {
            "descriptors": [
                {"node_kind": descriptor.node_kind, "name": descriptor.name, "weight": descriptor.weight}
                for descriptor in descriptors
            ],
            "matched_clusters": [],
            "matched_nodes": [],
            "expanded_nodes": [],
            "candidate_scores": [],
            "error": str(exc),
        }


def _debug_scene_node_payload(node: Optional[BehaviorSceneNode], score: float) -> dict[str, Any]:
    if node is None:
        return {
            "id": None,
            "node_kind": "",
            "name": "",
            "source_count": 0,
            "node_score": 0.0,
            "match_score": round(score, 4),
        }
    return {
        "id": node.id,
        "node_kind": node.node_kind,
        "name": node.name,
        "source_count": node.source_count,
        "node_score": round(float(node.score or 0.0), 4),
        "match_score": round(score, 4),
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
            "name": cluster_by_id.get(cluster_id).name if cluster_id in cluster_by_id else "",
            "score": round(score, 4),
        }
        for cluster_id, score in sorted(cluster_scores.items(), key=lambda item: item[1], reverse=True)
    ]


def mark_behavior_scene_links_selected(experience_path_id: int) -> None:
    """行为被选中后，提升相关场景链接的活跃度。"""

    if experience_path_id <= 0:
        return
    now = datetime.now()
    try:
        with get_db_session() as session:
            links = session.exec(
                select(BehaviorExperienceSceneLink).where(
                    BehaviorExperienceSceneLink.behavior_experience_path_id == experience_path_id
                )
            ).all()
            scene_node_ids: set[int] = set()
            for link in links:
                link.update_time = now
                link.weight = _clamp(float(link.weight or 1.0) + 0.03, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
                scene_node_ids.add(link.scene_node_id)
                session.add(link)

            if scene_node_ids:
                nodes = session.exec(select(BehaviorSceneNode).where(BehaviorSceneNode.id.in_(scene_node_ids))).all()  # type: ignore[attr-defined]
                for node in nodes:
                    node.update_time = now
                    session.add(node)

            scene_action_edges = session.exec(
                select(BehaviorSceneActionEdge).where(
                    BehaviorSceneActionEdge.behavior_experience_path_id == experience_path_id
                )
            ).all()
            action_node_ids: set[int] = set()
            for edge in scene_action_edges:
                edge.update_time = now
                edge.weight = _clamp(float(edge.weight or 1.0) + 0.04, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
                action_node_ids.add(edge.action_node_id)
                session.add(edge)

            if action_node_ids:
                action_nodes = session.exec(select(BehaviorActionNode).where(BehaviorActionNode.id.in_(action_node_ids))).all()  # type: ignore[attr-defined]
                for node in action_nodes:
                    node.update_time = now
                    session.add(node)
    except Exception as exc:
        logger.error(f"更新行为场景图选中状态失败: experience_id={experience_path_id} error={exc}")


def apply_behavior_scene_feedback(
    *,
    experience_path_id: int,
    score_delta: float,
    status: str,
) -> None:
    """反馈行为效果时，同步强化或削弱行为-场景链接。"""

    if experience_path_id <= 0:
        return
    del status
    now = datetime.now()
    weight_delta = float(score_delta) * 0.18

    try:
        with get_db_session() as session:
            links = session.exec(
                select(BehaviorExperienceSceneLink).where(
                    BehaviorExperienceSceneLink.behavior_experience_path_id == experience_path_id
                )
            ).all()
            scene_node_ids: set[int] = set()
            for link in links:
                link.weight = _clamp(float(link.weight or 1.0) + weight_delta, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
                link.update_time = now
                scene_node_ids.add(link.scene_node_id)
                session.add(link)

            if scene_node_ids:
                nodes = session.exec(select(BehaviorSceneNode).where(BehaviorSceneNode.id.in_(scene_node_ids))).all()  # type: ignore[attr-defined]
                for node in nodes:
                    node.score = _clamp(float(node.score or 0.0) + float(score_delta) * 0.08, MIN_NODE_SCORE, MAX_NODE_SCORE)
                    node.update_time = now
                    session.add(node)

            scene_action_edges = session.exec(
                select(BehaviorSceneActionEdge).where(
                    BehaviorSceneActionEdge.behavior_experience_path_id == experience_path_id
                )
            ).all()
            action_node_ids: set[int] = set()
            for edge in scene_action_edges:
                edge.weight = _clamp(float(edge.weight or 1.0) + weight_delta, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
                edge.update_time = now
                action_node_ids.add(edge.action_node_id)
                session.add(edge)

            action_outcome_edges = session.exec(
                select(BehaviorActionOutcomeEdge).where(
                    BehaviorActionOutcomeEdge.behavior_experience_path_id == experience_path_id
                )
            ).all()
            outcome_node_ids: set[int] = set()
            for edge in action_outcome_edges:
                edge.weight = _clamp(float(edge.weight or 1.0) + weight_delta * 0.75, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
                edge.update_time = now
                outcome_node_ids.add(edge.outcome_node_id)
                session.add(edge)

            if action_node_ids:
                action_nodes = session.exec(select(BehaviorActionNode).where(BehaviorActionNode.id.in_(action_node_ids))).all()  # type: ignore[attr-defined]
                for node in action_nodes:
                    node.score = _clamp(float(node.score or 0.0) + float(score_delta) * 0.1, MIN_NODE_SCORE, MAX_NODE_SCORE)
                    node.update_time = now
                    session.add(node)

            if outcome_node_ids:
                outcome_nodes = session.exec(select(BehaviorOutcomeNode).where(BehaviorOutcomeNode.id.in_(outcome_node_ids))).all()  # type: ignore[attr-defined]
                for node in outcome_nodes:
                    node.score = _clamp(float(node.score or 0.0) + float(score_delta) * 0.08, MIN_NODE_SCORE, MAX_NODE_SCORE)
                    node.update_time = now
                    session.add(node)
    except Exception as exc:
        logger.error(f"更新行为场景图反馈失败: experience_id={experience_path_id} error={exc}")


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
    normalized_tags = _cluster_normalized_tags(distribution, tag_lookup=tag_lookup)
    if not normalized_tags:
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
            name=_cluster_name_from_distribution(distribution, tag_lookup=tag_lookup),
            normalized_tags=normalized_tags,
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
        merged_distribution = _load_cluster_distribution(cluster.tag_distribution)
        merged_normalized_tags = _cluster_normalized_tags(merged_distribution, tag_lookup=tag_lookup)
        same_normalized_cluster = session.exec(
            select(BehaviorSceneCluster)
            .where(BehaviorSceneCluster.session_id == session_id)
            .where(BehaviorSceneCluster.normalized_tags == merged_normalized_tags)
        ).first()
        if same_normalized_cluster is None or same_normalized_cluster.id == cluster.id:
            cluster.normalized_tags = merged_normalized_tags
        cluster.name = _cluster_name_from_distribution(
            merged_distribution,
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


def _upsert_scene_node(
    session: Session,
    *,
    session_id: str,
    descriptor: SceneDescriptor,
) -> BehaviorSceneNode:
    node_name = _normalize_name(descriptor.name)
    statement = (
        select(BehaviorSceneNode)
        .where(BehaviorSceneNode.session_id == session_id)
        .where(BehaviorSceneNode.node_kind == descriptor.node_kind)
        .where(BehaviorSceneNode.name == node_name)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorSceneNode(
            session_id=session_id,
            node_kind=descriptor.node_kind,
            name=node_name,
            source_count=1,
            update_time=now,
        )
    else:
        node.name = node_name
        node.source_count += 1
        node.update_time = now
    session.add(node)
    session.flush()
    return node


def _upsert_scene_node_tags(
    session: Session,
    *,
    session_id: str,
    scene_node_id: int,
    descriptor: SceneDescriptor,
    tag_distribution: Sequence[dict[str, Any]],
) -> None:
    now = datetime.now()
    for tag_kind, cluster_key, probability in _distribution_tag_entries(tag_distribution):
        weight = _clamp(float(descriptor.weight) * probability, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
        statement = (
            select(BehaviorSceneNodeTag)
            .where(BehaviorSceneNodeTag.scene_node_id == scene_node_id)
            .where(BehaviorSceneNodeTag.tag_kind == tag_kind)
            .where(BehaviorSceneNodeTag.cluster_key == cluster_key)
        )
        row = session.exec(statement).first()
        if row is None:
            row = BehaviorSceneNodeTag(
                session_id=session_id,
                scene_node_id=scene_node_id,
                tag_kind=tag_kind,
                cluster_key=cluster_key,
                weight=weight,
                count=1,
                update_time=now,
            )
        else:
            row.count += 1
            row.weight = _clamp(float(row.weight or 1.0) + weight * 0.04, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
            row.update_time = now
        session.add(row)


def _upsert_scene_edge(
    session: Session,
    *,
    session_id: str,
    source_scene_id: int,
    target_scene_id: int,
    edge_type: str,
    weight: float,
) -> None:
    if source_scene_id == target_scene_id:
        return
    source_id, target_id = sorted([source_scene_id, target_scene_id])
    statement = (
        select(BehaviorSceneEdge)
        .where(BehaviorSceneEdge.session_id == session_id)
        .where(BehaviorSceneEdge.source_scene_id == source_id)
        .where(BehaviorSceneEdge.target_scene_id == target_id)
        .where(BehaviorSceneEdge.edge_type == edge_type)
    )
    edge = session.exec(statement).first()
    now = datetime.now()
    if edge is None:
        edge = BehaviorSceneEdge(
            session_id=session_id,
            source_scene_id=source_id,
            target_scene_id=target_id,
            edge_type=edge_type,
            weight=_clamp(weight, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT),
            count=1,
            update_time=now,
        )
    else:
        edge.count += 1
        edge.weight = _clamp(float(edge.weight or 1.0) + weight * 0.08, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
        edge.update_time = now
    session.add(edge)


def _upsert_experience_scene_link(
    session: Session,
    *,
    session_id: str,
    experience_path_id: int,
    scene_node_id: int,
    link_role: str,
    weight: float,
) -> None:
    statement = (
        select(BehaviorExperienceSceneLink)
        .where(BehaviorExperienceSceneLink.behavior_experience_path_id == experience_path_id)
        .where(BehaviorExperienceSceneLink.scene_node_id == scene_node_id)
        .where(BehaviorExperienceSceneLink.link_role == link_role)
    )
    link = session.exec(statement).first()
    now = datetime.now()
    if link is None:
        link = BehaviorExperienceSceneLink(
            session_id=session_id,
            behavior_experience_path_id=experience_path_id,
            scene_node_id=scene_node_id,
            link_role=link_role,
            weight=_clamp(weight, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT),
            count=1,
            update_time=now,
        )
    else:
        link.count += 1
        link.weight = _clamp(float(link.weight or 1.0) + weight * 0.06, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
        link.update_time = now
    session.add(link)


def _upsert_action_node(
    session: Session,
    *,
    session_id: str,
    action: str,
) -> BehaviorActionNode:
    normalized_action = _normalize_display_text(action, max_length=240)
    statement = (
        select(BehaviorActionNode)
        .where(BehaviorActionNode.session_id == session_id)
        .where(BehaviorActionNode.action == normalized_action)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorActionNode(
            session_id=session_id,
            action=normalized_action,
            source_count=1,
            update_time=now,
        )
    else:
        node.source_count += 1
        node.update_time = now
    session.add(node)
    session.flush()
    return node


def _upsert_outcome_node(
    session: Session,
    *,
    session_id: str,
    outcome: str,
) -> BehaviorOutcomeNode:
    normalized_outcome = _normalize_display_text(outcome, max_length=220)
    statement = (
        select(BehaviorOutcomeNode)
        .where(BehaviorOutcomeNode.session_id == session_id)
        .where(BehaviorOutcomeNode.outcome == normalized_outcome)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorOutcomeNode(
            session_id=session_id,
            outcome=normalized_outcome,
            source_count=1,
            update_time=now,
        )
    else:
        node.source_count += 1
        node.update_time = now
    session.add(node)
    session.flush()
    return node


def _upsert_scene_action_edge(
    session: Session,
    *,
    session_id: str,
    experience_path_id: int,
    scene_node_id: int,
    action_node_id: int,
    weight: float,
) -> None:
    statement = (
        select(BehaviorSceneActionEdge)
        .where(BehaviorSceneActionEdge.session_id == session_id)
        .where(BehaviorSceneActionEdge.scene_node_id == scene_node_id)
        .where(BehaviorSceneActionEdge.action_node_id == action_node_id)
        .where(BehaviorSceneActionEdge.behavior_experience_path_id == experience_path_id)
    )
    edge = session.exec(statement).first()
    now = datetime.now()
    if edge is None:
        edge = BehaviorSceneActionEdge(
            session_id=session_id,
            scene_node_id=scene_node_id,
            action_node_id=action_node_id,
            behavior_experience_path_id=experience_path_id,
            weight=_clamp(weight, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT),
            count=1,
            update_time=now,
        )
    else:
        edge.count += 1
        edge.weight = _clamp(float(edge.weight or 1.0) + weight * 0.06, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
        edge.update_time = now
    session.add(edge)


def _upsert_action_outcome_edge(
    session: Session,
    *,
    session_id: str,
    experience_path_id: int,
    action_node_id: int,
    outcome_node_id: int,
    weight: float,
) -> None:
    statement = (
        select(BehaviorActionOutcomeEdge)
        .where(BehaviorActionOutcomeEdge.session_id == session_id)
        .where(BehaviorActionOutcomeEdge.action_node_id == action_node_id)
        .where(BehaviorActionOutcomeEdge.outcome_node_id == outcome_node_id)
        .where(BehaviorActionOutcomeEdge.behavior_experience_path_id == experience_path_id)
    )
    edge = session.exec(statement).first()
    now = datetime.now()
    if edge is None:
        edge = BehaviorActionOutcomeEdge(
            session_id=session_id,
            action_node_id=action_node_id,
            outcome_node_id=outcome_node_id,
            behavior_experience_path_id=experience_path_id,
            weight=_clamp(weight, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT),
            count=1,
            update_time=now,
        )
    else:
        edge.count += 1
        edge.weight = _clamp(float(edge.weight or 1.0) + weight * 0.04, MIN_LINK_WEIGHT, MAX_LINK_WEIGHT)
        edge.update_time = now
    session.add(edge)


def _load_scene_node_map(session: Session, node_ids: set[int]) -> dict[int, BehaviorSceneNode]:
    if not node_ids:
        return {}
    nodes = session.exec(select(BehaviorSceneNode).where(BehaviorSceneNode.id.in_(node_ids))).all()  # type: ignore[attr-defined]
    return {int(node.id): node for node in nodes if node.id is not None}


def _score_scene_nodes_by_tag_clusters(
    session: Session,
    *,
    profile: BehaviorScenarioProfile,
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    tag_lookup = _load_tag_cluster_lookup(session)
    distribution = build_scene_cluster_distribution(profile, tag_lookup=tag_lookup)
    tag_entries = _distribution_tag_entries(distribution)
    if not tag_entries:
        return {}

    probability_by_key = {(tag_kind, cluster_key): probability for tag_kind, cluster_key, probability in tag_entries}
    tag_kinds = {tag_kind for tag_kind, _, _ in tag_entries}
    cluster_keys = {cluster_key for _, cluster_key, _ in tag_entries}
    statement = (
        select(BehaviorSceneNodeTag)
        .where(BehaviorSceneNodeTag.tag_kind.in_(tag_kinds))  # type: ignore[attr-defined]
        .where(BehaviorSceneNodeTag.cluster_key.in_(cluster_keys))  # type: ignore[attr-defined]
    )
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorSceneNodeTag, session_ids))

    node_scores: dict[int, float] = {}
    for row in session.exec(statement).all():
        probability = probability_by_key.get((row.tag_kind, row.cluster_key), 0.0)
        if probability <= 0 or row.scene_node_id <= 0:
            continue
        history_bonus = 1.0 + min(float(row.count or 0), 20.0) * 0.02
        score = probability * float(row.weight or 1.0) * history_bonus
        node_scores[row.scene_node_id] = node_scores.get(row.scene_node_id, 0.0) + score

    if not node_scores:
        return {}

    nodes = _load_scene_node_map(session, set(node_scores))
    scored_nodes: list[tuple[int, float]] = []
    for node_id, score in node_scores.items():
        node = nodes.get(node_id)
        if node is None:
            continue
        reinforcement = 1.0 + max(float(node.score or 0.0), -2.0) * 0.04
        scored_nodes.append((node_id, round(score * reinforcement, 4)))

    scored_nodes.sort(key=lambda item: item[1], reverse=True)
    return dict(scored_nodes[:MAX_SCENE_GRAPH_MATCHED_NODES])


def _expand_scene_scores(
    session: Session,
    *,
    active_node_scores: dict[int, float],
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    expanded_scores = dict(active_node_scores)
    if not active_node_scores:
        return expanded_scores

    active_node_ids = set(active_node_scores)
    statement = select(BehaviorSceneEdge).where(
        or_(
            BehaviorSceneEdge.source_scene_id.in_(active_node_ids),  # type: ignore[attr-defined]
            BehaviorSceneEdge.target_scene_id.in_(active_node_ids),  # type: ignore[attr-defined]
        )
    )
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorSceneEdge, session_ids))

    for edge in session.exec(statement).all():
        source_score = active_node_scores.get(edge.source_scene_id, 0.0)
        target_score = active_node_scores.get(edge.target_scene_id, 0.0)
        if source_score > 0:
            spread_score = source_score * float(edge.weight or 1.0) * SCENE_EDGE_SPREAD_FACTOR
            expanded_scores[edge.target_scene_id] = max(expanded_scores.get(edge.target_scene_id, 0.0), spread_score)
        if target_score > 0:
            spread_score = target_score * float(edge.weight or 1.0) * SCENE_EDGE_SPREAD_FACTOR
            expanded_scores[edge.source_scene_id] = max(expanded_scores.get(edge.source_scene_id, 0.0), spread_score)
    return expanded_scores


def _score_scene_clusters(
    session: Session,
    *,
    profile: BehaviorScenarioProfile,
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    tag_lookup = _load_tag_cluster_lookup(session)
    target_distribution = build_scene_cluster_distribution(profile, tag_lookup=tag_lookup)
    if not target_distribution:
        return {}

    statement = select(BehaviorSceneCluster)
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorSceneCluster, session_ids))

    cluster_scores: dict[int, float] = {}
    for cluster in session.exec(statement).all():
        if cluster.id is None:
            continue
        overlap = _cluster_distribution_overlap(
            _load_cluster_distribution(cluster.tag_distribution),
            target_distribution,
            tag_lookup=tag_lookup,
        )
        if overlap < SCENE_CLUSTER_MATCH_THRESHOLD:
            continue
        reinforcement = 1.0 + max(float(cluster.score or 0.0), -2.0) * 0.04
        cluster_scores[cluster.id] = round(overlap * 2.0 * reinforcement, 4)
    return dict(sorted(cluster_scores.items(), key=lambda item: item[1], reverse=True)[:MAX_SCENE_GRAPH_MATCHED_NODES])


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


def _score_behavior_links(
    session: Session,
    *,
    node_scores: dict[int, float],
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    if not node_scores:
        return {}

    statement = select(BehaviorExperienceSceneLink).where(
        BehaviorExperienceSceneLink.scene_node_id.in_(set(node_scores))  # type: ignore[attr-defined]
    )
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorExperienceSceneLink, session_ids))

    behavior_scores: dict[int, float] = {}
    for link in session.exec(statement).all():
        node_score = node_scores.get(link.scene_node_id, 0.0)
        if node_score <= 0:
            continue
        link_weight = float(link.weight or 1.0)
        history_bonus = 1.0 + min(float(link.count or 0), 20.0) * 0.02
        score = node_score * link_weight * history_bonus
        behavior_scores[link.behavior_experience_path_id] = (
            behavior_scores.get(link.behavior_experience_path_id, 0.0) + score
        )
    return behavior_scores


def _score_behavior_paths(
    session: Session,
    *,
    node_scores: dict[int, float],
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    if not node_scores:
        return {}

    scene_action_statement = select(BehaviorSceneActionEdge).where(
        BehaviorSceneActionEdge.scene_node_id.in_(set(node_scores))  # type: ignore[attr-defined]
    )
    if not include_global:
        scene_action_statement = scene_action_statement.where(
            _session_scope_condition(BehaviorSceneActionEdge, session_ids)
        )

    action_scores: dict[int, float] = {}
    behavior_scores: dict[int, float] = {}
    for edge in session.exec(scene_action_statement).all():
        node_score = node_scores.get(edge.scene_node_id, 0.0)
        if node_score <= 0:
            continue
        edge_weight = float(edge.weight or 1.0)
        score = node_score * edge_weight
        action_scores[edge.action_node_id] = max(action_scores.get(edge.action_node_id, 0.0), score)
        behavior_scores[edge.behavior_experience_path_id] = (
            behavior_scores.get(edge.behavior_experience_path_id, 0.0) + score * 0.75
        )

    if not action_scores:
        return behavior_scores

    action_outcome_statement = select(BehaviorActionOutcomeEdge).where(
        BehaviorActionOutcomeEdge.action_node_id.in_(set(action_scores))  # type: ignore[attr-defined]
    )
    if not include_global:
        action_outcome_statement = action_outcome_statement.where(
            _session_scope_condition(BehaviorActionOutcomeEdge, session_ids)
        )

    for edge in session.exec(action_outcome_statement).all():
        action_score = action_scores.get(edge.action_node_id, 0.0)
        if action_score <= 0:
            continue
        edge_weight = float(edge.weight or 1.0)
        score = action_score * edge_weight * 0.35
        behavior_scores[edge.behavior_experience_path_id] = (
            behavior_scores.get(edge.behavior_experience_path_id, 0.0) + score
        )

    return behavior_scores
