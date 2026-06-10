"""行为学习图谱浏览 API。"""

from datetime import datetime
from typing import Annotated, Any, Optional

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlmodel import col, func, select

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
    ChatSession,
)
from src.learners.behavior_scenario import BehaviorScenarioProfile, parse_behavior_scenario_response
from src.learners.behavior_scene_graph_store import debug_retrieve_behavior_scores_from_scene_graph
from src.webui.dependencies import require_auth

router = APIRouter(prefix="/behavior", tags=["Behavior"], dependencies=[Depends(require_auth)])


class BehaviorChatInfo(BaseModel):
    session_id: str
    display_name: str
    platform: str = ""
    chat_type: str = ""
    path_count: int = 0
    cluster_count: int = 0
    scene_count: int = 0
    last_active_time: Optional[str] = None


class BehaviorClusterTag(BaseModel):
    tag: str
    probability: float = 0.0


class BehaviorSceneClusterPayload(BaseModel):
    id: Optional[int] = None
    name: str = ""
    tags: list[BehaviorClusterTag] = Field(default_factory=list)
    source_count: int = 0
    score: float = 0.0
    update_time: Optional[str] = None


class BehaviorPathItem(BaseModel):
    id: int
    session_id: Optional[str] = None
    chat_name: str = ""
    trigger: str = ""
    scene_cluster_id: Optional[int] = None
    scene_cluster_name: str = ""
    scene_cluster_tags: list[BehaviorClusterTag] = Field(default_factory=list)
    scene_cluster_source_count: int = 0
    scene_cluster_score: float = 0.0
    actor_type: str = "other_user"
    learning_type: str = "observed_behavior"
    action: str = ""
    outcome: str = ""
    count: int = 0
    activation_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    score: float = 0.0
    enabled: bool = True
    last_active_time: Optional[str] = None
    last_feedback_time: Optional[str] = None
    update_time: Optional[str] = None


class BehaviorPathListResponse(BaseModel):
    success: bool = True
    total: int
    page: int
    page_size: int
    data: list[BehaviorPathItem]


class BehaviorClusterItem(BehaviorSceneClusterPayload):
    session_id: Optional[str] = None
    chat_name: str = ""
    path_count: int = 0
    enabled_path_count: int = 0
    activation_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    observed_path_count: int = 0
    self_reflection_path_count: int = 0
    last_active_time: Optional[str] = None


class BehaviorClusterListResponse(BaseModel):
    success: bool = True
    total: int
    page: int
    page_size: int
    data: list[BehaviorClusterItem]


class BehaviorNodePayload(BaseModel):
    id: int
    kind: str
    label: str
    score: float = 0.0
    source_count: int = 0


class BehaviorEdgePayload(BaseModel):
    id: str
    source: str
    target: str
    kind: str
    weight: float = 1.0
    count: int = 0


class BehaviorPathDetailResponse(BaseModel):
    success: bool = True
    data: dict[str, Any]


class BehaviorScenarioDebugRequest(BaseModel):
    session_id: Optional[str] = Field(default=None)
    include_global: bool = Field(default=True)
    retrieval_mode: str = Field(default="tag_expand_scene_cluster")
    summary: str = Field(default="")
    tag_clusters: list[dict[str, Any]] = Field(default_factory=list)
    need: dict[str, Any] = Field(default_factory=dict)
    other_traits: list[dict[str, Any]] = Field(default_factory=list)
    max_count: int = Field(default=20, ge=1, le=80)


def _isoformat(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    if value:
        return str(value)
    return None


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


def _cluster_tag_payloads(raw_value: Any) -> list[BehaviorClusterTag]:
    tags: list[BehaviorClusterTag] = []
    for item in _load_json_list(raw_value):
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag") or "").strip()
        if not tag:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            probability = 0.0
        tags.append(BehaviorClusterTag(tag=tag, probability=max(probability, 0.0)))
    return sorted(tags, key=lambda item: item.probability, reverse=True)


def _cluster_payload(cluster: Optional[BehaviorSceneCluster]) -> BehaviorSceneClusterPayload:
    if cluster is None:
        return BehaviorSceneClusterPayload()
    return BehaviorSceneClusterPayload(
        id=cluster.id,
        name=str(cluster.name or ""),
        tags=_cluster_tag_payloads(cluster.tag_distribution),
        source_count=int(cluster.source_count or 0),
        score=float(cluster.score or 0.0),
        update_time=_isoformat(cluster.update_time),
    )


def _chat_type_of(session: Optional[ChatSession]) -> str:
    if session is None:
        return ""
    return "group" if session.group_id else "private"


def _chat_display_name(session: Optional[ChatSession], session_id: Optional[str]) -> str:
    if session is None:
        return "全局行为" if not session_id else session_id
    if session.group_name:
        return session.group_name
    if session.user_nickname:
        return f"{session.user_nickname} 的私聊"
    return session.session_id


def _session_scope(session_id: Optional[str]) -> set[str]:
    normalized_session_id = str(session_id or "").strip()
    return {normalized_session_id} if normalized_session_id else set()


@router.get("/chats")
async def list_behavior_chats() -> dict[str, Any]:
    """列出存在行为经验路径的聊天流。"""

    with get_db_session(auto_commit=False) as session:
        path_rows = session.exec(
            select(
                BehaviorExperiencePath.session_id,
                func.count(BehaviorExperiencePath.id),
                func.max(BehaviorExperiencePath.last_active_time),
            )
            .group_by(BehaviorExperiencePath.session_id)
            .order_by(func.max(BehaviorExperiencePath.last_active_time).desc())
        ).all()
        scene_rows = session.exec(
            select(BehaviorSceneNode.session_id, func.count(BehaviorSceneNode.id)).group_by(BehaviorSceneNode.session_id)
        ).all()
        scene_count_by_session = {row[0]: int(row[1] or 0) for row in scene_rows}
        cluster_rows = session.exec(
            select(BehaviorSceneCluster.session_id, func.count(BehaviorSceneCluster.id)).group_by(
                BehaviorSceneCluster.session_id
            )
        ).all()
        cluster_count_by_session = {row[0]: int(row[1] or 0) for row in cluster_rows}
        session_ids = [row[0] for row in path_rows if row[0]]
        chat_sessions = {}
        if session_ids:
            chat_sessions = {
                chat.session_id: chat
                for chat in session.exec(select(ChatSession).where(col(ChatSession.session_id).in_(session_ids))).all()
            }

    chats = [
        BehaviorChatInfo(
            session_id=row[0] or "",
            display_name=_chat_display_name(chat_sessions.get(row[0]), row[0]),
            platform=str(chat_sessions[row[0]].platform) if row[0] in chat_sessions else "",
            chat_type=_chat_type_of(chat_sessions.get(row[0])),
            path_count=int(row[1] or 0),
            cluster_count=cluster_count_by_session.get(row[0], 0),
            scene_count=scene_count_by_session.get(row[0], 0),
            last_active_time=_isoformat(row[2]),
        ).model_dump()
        for row in path_rows
    ]
    return {"success": True, "data": chats}


@router.get("/paths", response_model=BehaviorPathListResponse)
async def list_behavior_paths(
    session_id: Annotated[Optional[str], Query()] = None,
    search: Annotated[str, Query()] = "",
    enabled: Annotated[str, Query()] = "all",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> BehaviorPathListResponse:
    """分页列出行为经验路径。"""

    normalized_search = " ".join(str(search or "").split()).strip().lower()
    with get_db_session(auto_commit=False) as session:
        statement = select(BehaviorExperiencePath)
        if session_id is not None and session_id != "all":
            if session_id == "__global__":
                statement = statement.where(BehaviorExperiencePath.session_id.is_(None))  # type: ignore[attr-defined]
            else:
                statement = statement.where(BehaviorExperiencePath.session_id == session_id)
        if enabled == "true":
            statement = statement.where(BehaviorExperiencePath.enabled.is_(True))  # type: ignore[attr-defined]
        elif enabled == "false":
            statement = statement.where(BehaviorExperiencePath.enabled.is_(False))  # type: ignore[attr-defined]

        paths = list(session.exec(statement.order_by(BehaviorExperiencePath.update_time.desc())).all())  # type: ignore[attr-defined]
        path_items = _build_path_items(session, paths)
        if normalized_search:
            path_items = [
                item
                for item in path_items
                if normalized_search
                in (
                    f"{item.scene_cluster_name}\n{item.trigger}\n{item.action}\n{item.outcome}\n"
                    f"{item.actor_type}\n{item.learning_type}\n{item.chat_name}\n"
                    + "\n".join(tag.tag for tag in item.scene_cluster_tags)
                ).lower()
            ]
        total = len(path_items)
        start = (page - 1) * page_size
        data = path_items[start : start + page_size]
    return BehaviorPathListResponse(total=total, page=page, page_size=page_size, data=data)


@router.get("/clusters", response_model=BehaviorClusterListResponse)
async def list_behavior_clusters(
    session_id: Annotated[Optional[str], Query()] = None,
    search: Annotated[str, Query()] = "",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=5000)] = 20,
) -> BehaviorClusterListResponse:
    """分页列出行为场景簇，便于浏览 tag 概率分布。"""

    normalized_search = " ".join(str(search or "").split()).strip().lower()
    with get_db_session(auto_commit=False) as session:
        statement = select(BehaviorSceneCluster)
        if session_id is not None and session_id != "all":
            if session_id == "__global__":
                statement = statement.where(BehaviorSceneCluster.session_id.is_(None))  # type: ignore[attr-defined]
            else:
                statement = statement.where(BehaviorSceneCluster.session_id == session_id)

        clusters = list(session.exec(statement.order_by(BehaviorSceneCluster.update_time.desc())).all())  # type: ignore[attr-defined]
        cluster_items = _build_cluster_items(session, clusters)
        if normalized_search:
            cluster_items = [
                item
                for item in cluster_items
                if normalized_search
                in (
                    f"{item.name}\n{item.chat_name}\n"
                    + "\n".join(tag.tag for tag in item.tags)
                ).lower()
            ]
        total = len(cluster_items)
        start = (page - 1) * page_size
        data = cluster_items[start : start + page_size]
    return BehaviorClusterListResponse(total=total, page=page, page_size=page_size, data=data)


@router.get("/paths/{path_id}", response_model=BehaviorPathDetailResponse)
async def get_behavior_path_detail(path_id: int) -> BehaviorPathDetailResponse:
    """读取一条行为经验路径及其局部图谱。"""

    with get_db_session(auto_commit=False) as session:
        path = session.get(BehaviorExperiencePath, path_id)
        if path is None:
            raise HTTPException(status_code=404, detail="行为经验路径不存在")
        item = _build_path_items(session, [path])[0]
        scene_links = session.exec(
            select(BehaviorExperienceSceneLink).where(
                BehaviorExperienceSceneLink.behavior_experience_path_id == path_id
            )
        ).all()
        scene_node_ids = {link.scene_node_id for link in scene_links}
        scene_nodes = _load_scene_nodes(session, scene_node_ids)
        scene_edges = session.exec(
            select(BehaviorSceneEdge).where(
                (col(BehaviorSceneEdge.source_scene_id).in_(scene_node_ids))
                | (col(BehaviorSceneEdge.target_scene_id).in_(scene_node_ids))
            )
        ).all()
        scene_action_edges = session.exec(
            select(BehaviorSceneActionEdge).where(BehaviorSceneActionEdge.behavior_experience_path_id == path_id)
        ).all()
        action_outcome_edges = session.exec(
            select(BehaviorActionOutcomeEdge).where(BehaviorActionOutcomeEdge.behavior_experience_path_id == path_id)
        ).all()

    nodes: list[BehaviorNodePayload] = [
        BehaviorNodePayload(
            id=node.id or 0,
            kind=node.node_kind,
            label=node.name,
            score=float(node.score or 0.0),
            source_count=int(node.source_count or 0),
        )
        for node in scene_nodes.values()
    ]
    nodes.extend(
        [
            BehaviorNodePayload(id=path.action_node_id, kind="action", label=item.action),
            BehaviorNodePayload(id=path.outcome_node_id, kind="outcome", label=item.outcome),
        ]
    )
    edges = _build_detail_edges(scene_edges, scene_links, scene_action_edges, action_outcome_edges)
    return BehaviorPathDetailResponse(
        data={
            "path": item.model_dump(),
            "scene_cluster": _cluster_payload(session.get(BehaviorSceneCluster, path.scene_cluster_id)).model_dump(),
            "evidence": _load_json_list(path.evidence_list),
            "feedback": _load_json_list(path.feedback_list),
            "nodes": [node.model_dump() for node in nodes],
            "edges": [edge.model_dump() for edge in edges],
        }
    )


@router.post("/retrieval-debug")
async def debug_behavior_retrieval(request: BehaviorScenarioDebugRequest) -> dict[str, Any]:
    """按输入场景模拟一次本地场景图检索。"""

    profile = BehaviorScenarioProfile(
        summary=" ".join(request.summary.split()).strip(),
        tag_clusters=parse_behavior_scenario_response(
            json.dumps(
                {
                    "tag_clusters": request.tag_clusters,
                    "need": request.need,
                    "other_traits": request.other_traits,
                },
                ensure_ascii=False,
            )
        ).tag_clusters,
        confidence=1.0 if request.tag_clusters or request.need or request.other_traits else 0.0,
    )
    debug_payload = debug_retrieve_behavior_scores_from_scene_graph(
        session_ids=_session_scope(request.session_id),
        include_global=request.include_global,
        profile=profile,
        max_count=request.max_count,
        retrieval_mode=request.retrieval_mode,
    )
    behavior_ids = [item["behavior_id"] for item in debug_payload.get("candidate_scores", [])]
    with get_db_session(auto_commit=False) as session:
        paths = []
        if behavior_ids:
            paths = session.exec(select(BehaviorExperiencePath).where(col(BehaviorExperiencePath.id).in_(behavior_ids))).all()
        path_items = {item.id: item for item in _build_path_items(session, list(paths))}
        matched_clusters = _enrich_debug_clusters(session, debug_payload.get("matched_clusters", []))
    return {
        "success": True,
        "data": {
            **debug_payload,
            "matched_clusters": matched_clusters,
            "candidates": [
                {
                    **score_item,
                    "path": path_items.get(score_item["behavior_id"]).model_dump()
                    if score_item["behavior_id"] in path_items
                    else None,
                }
                for score_item in debug_payload.get("candidate_scores", [])
            ],
        },
    }


def _build_path_items(session: Any, paths: list[BehaviorExperiencePath]) -> list[BehaviorPathItem]:
    cluster_ids = {path.scene_cluster_id for path in paths}
    action_ids = {path.action_node_id for path in paths}
    outcome_ids = {path.outcome_node_id for path in paths}
    session_ids = {path.session_id for path in paths if path.session_id}
    scene_clusters = _load_scene_clusters(session, cluster_ids)
    action_nodes = {
        node.id: node
        for node in session.exec(select(BehaviorActionNode).where(col(BehaviorActionNode.id).in_(action_ids))).all()
    }
    outcome_nodes = {
        node.id: node
        for node in session.exec(select(BehaviorOutcomeNode).where(col(BehaviorOutcomeNode.id).in_(outcome_ids))).all()
    }
    chat_sessions = {
        chat.session_id: chat
        for chat in session.exec(select(ChatSession).where(col(ChatSession.session_id).in_(session_ids))).all()
    }
    items: list[BehaviorPathItem] = []
    for path in paths:
        scene_cluster = scene_clusters.get(path.scene_cluster_id)
        cluster_payload = _cluster_payload(scene_cluster)
        cluster_name = cluster_payload.name
        items.append(
            BehaviorPathItem(
                id=path.id or 0,
                session_id=path.session_id,
                chat_name=_chat_display_name(chat_sessions.get(path.session_id), path.session_id),
                trigger=cluster_name,
                scene_cluster_id=cluster_payload.id,
                scene_cluster_name=cluster_name,
                scene_cluster_tags=cluster_payload.tags,
                scene_cluster_source_count=cluster_payload.source_count,
                scene_cluster_score=cluster_payload.score,
                actor_type=str(path.actor_type or "other_user"),
                learning_type=str(path.learning_type or "observed_behavior"),
                action=action_nodes[path.action_node_id].action if path.action_node_id in action_nodes else "",
                outcome=outcome_nodes[path.outcome_node_id].outcome if path.outcome_node_id in outcome_nodes else "",
                count=int(path.count or 0),
                activation_count=int(path.activation_count or 0),
                success_count=int(path.success_count or 0),
                failure_count=int(path.failure_count or 0),
                score=float(path.score or 0.0),
                enabled=bool(path.enabled),
                last_active_time=_isoformat(path.last_active_time),
                last_feedback_time=_isoformat(path.last_feedback_time),
                update_time=_isoformat(path.update_time),
            )
        )
    return items


def _enrich_debug_clusters(session: Any, matched_clusters: Any) -> list[dict[str, Any]]:
    if not isinstance(matched_clusters, list):
        return []
    cluster_ids = {
        int(item["cluster_id"])
        for item in matched_clusters
        if isinstance(item, dict) and isinstance(item.get("cluster_id"), int)
    }
    scene_clusters = _load_scene_clusters(session, cluster_ids)
    enriched_clusters: list[dict[str, Any]] = []
    for item in matched_clusters:
        if not isinstance(item, dict):
            continue
        cluster_id = item.get("cluster_id")
        cluster = scene_clusters.get(cluster_id) if isinstance(cluster_id, int) else None
        cluster_payload = _cluster_payload(cluster)
        enriched_clusters.append(
            {
                **item,
                "name": cluster_payload.name or str(item.get("name") or ""),
                "tags": [tag.model_dump() for tag in cluster_payload.tags],
                "source_count": cluster_payload.source_count,
                "cluster_score": cluster_payload.score,
            }
        )
    return enriched_clusters


def _build_cluster_items(session: Any, clusters: list[BehaviorSceneCluster]) -> list[BehaviorClusterItem]:
    cluster_ids = {cluster.id for cluster in clusters if cluster.id is not None}
    session_ids = {cluster.session_id for cluster in clusters if cluster.session_id}
    paths_by_cluster_id: dict[int, list[BehaviorExperiencePath]] = {cluster_id: [] for cluster_id in cluster_ids}
    if cluster_ids:
        paths = session.exec(
            select(BehaviorExperiencePath).where(col(BehaviorExperiencePath.scene_cluster_id).in_(cluster_ids))
        ).all()
        for path in paths:
            paths_by_cluster_id.setdefault(path.scene_cluster_id, []).append(path)
    chat_sessions = {
        chat.session_id: chat
        for chat in session.exec(select(ChatSession).where(col(ChatSession.session_id).in_(session_ids))).all()
    }

    items: list[BehaviorClusterItem] = []
    for cluster in clusters:
        cluster_payload = _cluster_payload(cluster)
        cluster_paths = paths_by_cluster_id.get(cluster.id or 0, [])
        last_active_time = max((path.last_active_time for path in cluster_paths if path.last_active_time), default=None)
        items.append(
            BehaviorClusterItem(
                **cluster_payload.model_dump(),
                session_id=cluster.session_id,
                chat_name=_chat_display_name(chat_sessions.get(cluster.session_id), cluster.session_id),
                path_count=len(cluster_paths),
                enabled_path_count=sum(1 for path in cluster_paths if path.enabled),
                activation_count=sum(int(path.activation_count or 0) for path in cluster_paths),
                success_count=sum(int(path.success_count or 0) for path in cluster_paths),
                failure_count=sum(int(path.failure_count or 0) for path in cluster_paths),
                observed_path_count=sum(1 for path in cluster_paths if path.learning_type == "observed_behavior"),
                self_reflection_path_count=sum(1 for path in cluster_paths if path.learning_type == "self_reflection"),
                last_active_time=_isoformat(last_active_time),
            )
        )
    return items


def _load_scene_clusters(session: Any, scene_cluster_ids: set[int]) -> dict[int, BehaviorSceneCluster]:
    if not scene_cluster_ids:
        return {}
    return {
        cluster.id: cluster
        for cluster in session.exec(select(BehaviorSceneCluster).where(col(BehaviorSceneCluster.id).in_(scene_cluster_ids))).all()
        if cluster.id is not None
    }


def _load_scene_nodes(session: Any, scene_node_ids: set[int]) -> dict[int, BehaviorSceneNode]:
    if not scene_node_ids:
        return {}
    return {
        node.id: node
        for node in session.exec(select(BehaviorSceneNode).where(col(BehaviorSceneNode.id).in_(scene_node_ids))).all()
        if node.id is not None
    }


def _build_detail_edges(
    scene_edges: list[BehaviorSceneEdge],
    scene_links: list[BehaviorExperienceSceneLink],
    scene_action_edges: list[BehaviorSceneActionEdge],
    action_outcome_edges: list[BehaviorActionOutcomeEdge],
) -> list[BehaviorEdgePayload]:
    edges: list[BehaviorEdgePayload] = []
    for edge in scene_edges:
        edges.append(
            BehaviorEdgePayload(
                id=f"scene-{edge.id}",
                source=f"scene:{edge.source_scene_id}",
                target=f"scene:{edge.target_scene_id}",
                kind=edge.edge_type,
                weight=float(edge.weight or 1.0),
                count=int(edge.count or 0),
            )
        )
    for link in scene_links:
        edges.append(
            BehaviorEdgePayload(
                id=f"link-{link.id}",
                source=f"scene:{link.scene_node_id}",
                target=f"path:{link.behavior_experience_path_id}",
                kind=link.link_role,
                weight=float(link.weight or 1.0),
                count=int(link.count or 0),
            )
        )
    for edge in scene_action_edges:
        edges.append(
            BehaviorEdgePayload(
                id=f"scene-action-{edge.id}",
                source=f"scene:{edge.scene_node_id}",
                target=f"action:{edge.action_node_id}",
                kind="scene_action",
                weight=float(edge.weight or 1.0),
                count=int(edge.count or 0),
            )
        )
    for edge in action_outcome_edges:
        edges.append(
            BehaviorEdgePayload(
                id=f"action-outcome-{edge.id}",
                source=f"action:{edge.action_node_id}",
                target=f"outcome:{edge.outcome_node_id}",
                kind="action_outcome",
                weight=float(edge.weight or 1.0),
                count=int(edge.count or 0),
            )
        )
    return edges
