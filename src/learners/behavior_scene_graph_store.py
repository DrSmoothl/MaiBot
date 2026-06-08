from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import or_
from sqlmodel import Session, select

import difflib
import json
import re

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
)
from src.common.logger import get_logger

from .behavior_scenario import BehaviorScenarioProfile

logger = get_logger("behavior_scene_graph")

MAX_SCENE_GRAPH_NODES_PER_PROFILE = 12
MAX_SCENE_GRAPH_MATCHED_NODES = 24
MAX_SCENE_GRAPH_BEHAVIOR_IDS = 48
SCENE_NODE_MATCH_THRESHOLD = 0.18
SCENE_EDGE_SPREAD_FACTOR = 0.35
MIN_LINK_WEIGHT = 0.1
MAX_LINK_WEIGHT = 8.0
MIN_NODE_SCORE = -4.0
MAX_NODE_SCORE = 6.0
SCENE_CLUSTER_MATCH_THRESHOLD = 0.58
SCENE_CLUSTER_REUSE_THRESHOLD = 0.72


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


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, value))


def _extract_terms(text: str) -> set[str]:
    normalized_text = _normalize_name(text, max_length=400)
    if not normalized_text:
        return set()

    terms: set[str] = set(re.findall(r"[a-z0-9_./:-]{2,}", normalized_text))
    for segment in re.findall(r"[\u4e00-\u9fff]+", normalized_text):
        if len(segment) == 1:
            terms.add(segment)
            continue
        for ngram_length in (2, 3, 4):
            if len(segment) < ngram_length:
                continue
            for index in range(len(segment) - ngram_length + 1):
                terms.add(segment[index : index + ngram_length])
    return terms


def _text_similarity(left: str, right: str) -> float:
    normalized_left = _normalize_name(left)
    normalized_right = _normalize_name(right)
    if not normalized_left or not normalized_right:
        return 0.0
    if normalized_left == normalized_right:
        return 1.0

    left_terms = _extract_terms(normalized_left)
    right_terms = _extract_terms(normalized_right)
    term_score = 0.0
    if left_terms and right_terms:
        overlap_count = len(left_terms & right_terms)
        if overlap_count > 0:
            term_score = overlap_count / (len(left_terms) ** 0.5 * len(right_terms) ** 0.5)

    sequence_score = difflib.SequenceMatcher(None, normalized_left, normalized_right).ratio()
    return max(term_score, sequence_score * 0.65)


def _normalize_tag_name(tag_kind: str, value: str) -> str:
    normalized_value = _normalize_display_text(value, max_length=48)
    normalized_key = _normalize_name(normalized_value, max_length=48)
    if not normalized_key:
        return ""
    return f"{tag_kind}:{normalized_key}"


def _build_cluster_tag_weights(profile: BehaviorScenarioProfile) -> dict[str, float]:
    tag_weights: dict[str, float] = {}

    def add(tag_kind: str, value: str, weight: float) -> None:
        tag_name = _normalize_tag_name(tag_kind, value)
        if not tag_name:
            return
        tag_weights[tag_name] = max(tag_weights.get(tag_name, 0.0), weight)

    add("phase", profile.conversation_phase, 1.4)
    for tag in profile.domain_tags[:4]:
        add("domain", tag, 1.0)
    for need in profile.behavior_needs[:4]:
        add("need", need, 1.1)
    for risk in profile.risk_flags[:3]:
        add("risk", risk, 0.8)
    return tag_weights


def build_scene_cluster_distribution(profile: BehaviorScenarioProfile) -> list[dict[str, float | str]]:
    """将场景画像转成 tag 概率分布，用于匹配稳定场景簇。"""

    tag_weights = _build_cluster_tag_weights(profile)
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


def _distribution_to_mapping(distribution: Sequence[dict[str, Any]]) -> dict[str, float]:
    tag_probs: dict[str, float] = {}
    for item in distribution:
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag") or "").strip()
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


def _cluster_normalized_tags(distribution: Sequence[dict[str, Any]]) -> str:
    return "|".join(sorted(_distribution_to_mapping(distribution)))


def _cluster_name_from_distribution(distribution: Sequence[dict[str, Any]]) -> str:
    tag_probs = _distribution_to_mapping(distribution)
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
) -> float:
    left_probs = _distribution_to_mapping(left_distribution)
    right_probs = _distribution_to_mapping(right_distribution)
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

    add("scene", scene_start or profile.to_learning_start_text(), weight=1.25)
    add("scene", profile.summary, weight=1.0)
    add("intent", profile.user_intent, weight=0.85)
    add("phase", profile.conversation_phase, weight=0.8)

    for tag in profile.domain_tags[:4]:
        add("domain", tag, weight=0.65)
    for need in profile.behavior_needs[:4]:
        add("need", need, weight=0.75)
    for risk in profile.risk_flags[:2]:
        add("risk", risk, weight=0.45)
    for avoid_behavior in profile.avoid_behaviors[:2]:
        add("avoid", avoid_behavior, weight=0.35)

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

    descriptors = build_scene_descriptors(profile, scene_start=profile.to_learning_start_text())
    if not descriptors:
        return {}

    try:
        with get_db_session(auto_commit=False) as session:
            nodes = _load_scoped_scene_nodes(session, session_ids=session_ids, include_global=include_global)
            active_node_scores = _score_scene_nodes(nodes, descriptors)
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

    descriptors = build_scene_descriptors(profile, scene_start=profile.to_learning_start_text())
    if not descriptors:
        return {
            "descriptors": [],
            "matched_clusters": [],
            "matched_nodes": [],
            "expanded_nodes": [],
            "candidate_scores": [],
        }

    try:
        with get_db_session(auto_commit=False) as session:
            nodes = _load_scoped_scene_nodes(session, session_ids=session_ids, include_global=include_global)
            node_map = {node.id: node for node in nodes if node.id is not None}
            active_node_scores = _score_scene_nodes(nodes, descriptors)
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
    distribution = build_scene_cluster_distribution(profile)
    if not distribution:
        return None
    normalized_tags = _cluster_normalized_tags(distribution)
    if not normalized_tags:
        return None

    statement = (
        select(BehaviorSceneCluster)
        .where(BehaviorSceneCluster.session_id == session_id)
        .where(BehaviorSceneCluster.normalized_tags == normalized_tags)
    )
    cluster = session.exec(statement).first()
    is_exact_match = cluster is not None
    if cluster is None:
        cluster_candidates = session.exec(
            select(BehaviorSceneCluster).where(BehaviorSceneCluster.session_id == session_id)
        ).all()
        best_cluster: Optional[BehaviorSceneCluster] = None
        best_overlap = 0.0
        for candidate in cluster_candidates:
            overlap = _cluster_distribution_overlap(
                _load_cluster_distribution(candidate.tag_distribution),
                distribution,
            )
            if overlap > best_overlap:
                best_cluster = candidate
                best_overlap = overlap
        if best_cluster is not None and best_overlap >= SCENE_CLUSTER_REUSE_THRESHOLD:
            cluster = best_cluster

    now = datetime.now()
    if cluster is None:
        cluster = BehaviorSceneCluster(
            session_id=session_id,
            name=_cluster_name_from_distribution(distribution),
            normalized_tags=normalized_tags,
            tag_distribution=_dump_cluster_distribution(distribution),
            source_count=1,
            update_time=now,
        )
    else:
        if is_exact_match:
            cluster.name = _cluster_name_from_distribution(distribution)
            cluster.normalized_tags = normalized_tags
            cluster.tag_distribution = _dump_cluster_distribution(distribution)
        else:
            cluster.tag_distribution = _merge_cluster_distributions(
                _load_cluster_distribution(cluster.tag_distribution),
                distribution,
                existing_weight=max(int(cluster.source_count or 0), 1),
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
) -> str:
    existing_probs = _distribution_to_mapping(existing_distribution)
    new_probs = _distribution_to_mapping(new_distribution)
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
    normalized_name = _normalize_name(descriptor.name)
    statement = (
        select(BehaviorSceneNode)
        .where(BehaviorSceneNode.session_id == session_id)
        .where(BehaviorSceneNode.node_kind == descriptor.node_kind)
        .where(BehaviorSceneNode.normalized_name == normalized_name)
    )
    node = session.exec(statement).first()
    now = datetime.now()
    if node is None:
        node = BehaviorSceneNode(
            session_id=session_id,
            node_kind=descriptor.node_kind,
            name=descriptor.name,
            normalized_name=normalized_name,
            source_count=1,
            update_time=now,
        )
    else:
        node.name = descriptor.name
        node.normalized_name = normalized_name
        node.source_count += 1
        node.update_time = now
    session.add(node)
    session.flush()
    return node


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


def _load_scoped_scene_nodes(
    session: Session,
    *,
    session_ids: set[str],
    include_global: bool,
) -> list[BehaviorSceneNode]:
    statement = select(BehaviorSceneNode)
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorSceneNode, session_ids))
    statement = statement.order_by(BehaviorSceneNode.update_time.desc())  # type: ignore[attr-defined]
    return list(session.exec(statement).all())


def _score_scene_nodes(
    nodes: Sequence[BehaviorSceneNode],
    descriptors: Sequence[SceneDescriptor],
) -> dict[int, float]:
    scored_nodes: list[tuple[int, float]] = []
    for node in nodes:
        if node.id is None:
            continue
        best_score = 0.0
        for descriptor in descriptors:
            kind_multiplier = 1.0 if node.node_kind == descriptor.node_kind else 0.72
            similarity = _text_similarity(node.name, descriptor.name)
            score = similarity * descriptor.weight * kind_multiplier
            if node.normalized_name == _normalize_name(descriptor.name) and node.node_kind == descriptor.node_kind:
                score = max(score, descriptor.weight + 0.35)
            best_score = max(best_score, score)
        if best_score < SCENE_NODE_MATCH_THRESHOLD:
            continue
        reinforcement = 1.0 + max(float(node.score or 0.0), -2.0) * 0.04
        scored_nodes.append((node.id, round(best_score * reinforcement, 4)))

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
    target_distribution = build_scene_cluster_distribution(profile)
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
