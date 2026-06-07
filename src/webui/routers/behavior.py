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
    BehaviorSceneEdge,
    BehaviorSceneNode,
    ChatSession,
)
from src.learners.behavior_scenario import BehaviorScenarioProfile
from src.learners.behavior_scene_graph_store import debug_retrieve_behavior_scores_from_scene_graph
from src.webui.dependencies import require_auth

router = APIRouter(prefix="/behavior", tags=["Behavior"], dependencies=[Depends(require_auth)])


class BehaviorChatInfo(BaseModel):
    session_id: str
    display_name: str
    platform: str = ""
    chat_type: str = ""
    path_count: int = 0
    scene_count: int = 0
    last_active_time: Optional[str] = None


class BehaviorPathItem(BaseModel):
    id: int
    session_id: Optional[str] = None
    chat_name: str = ""
    trigger: str = ""
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
    summary: str = Field(default="")
    user_intent: str = Field(default="")
    conversation_phase: str = Field(default="")
    domain_tags: list[str] = Field(default_factory=list)
    behavior_needs: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    avoid_behaviors: list[str] = Field(default_factory=list)
    retrieval_query: str = Field(default="")
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
                if normalized_search in f"{item.trigger}\n{item.action}\n{item.outcome}\n{item.chat_name}".lower()
            ]
        total = len(path_items)
        start = (page - 1) * page_size
        data = path_items[start : start + page_size]
    return BehaviorPathListResponse(total=total, page=page, page_size=page_size, data=data)


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
        summary=request.summary,
        user_intent=request.user_intent,
        conversation_phase=request.conversation_phase,
        domain_tags=request.domain_tags,
        behavior_needs=request.behavior_needs,
        risk_flags=request.risk_flags,
        avoid_behaviors=request.avoid_behaviors,
        retrieval_query=request.retrieval_query,
        confidence=1.0,
    )
    debug_payload = debug_retrieve_behavior_scores_from_scene_graph(
        session_ids=_session_scope(request.session_id),
        include_global=request.include_global,
        profile=profile,
        max_count=request.max_count,
    )
    behavior_ids = [item["behavior_id"] for item in debug_payload.get("candidate_scores", [])]
    with get_db_session(auto_commit=False) as session:
        paths = []
        if behavior_ids:
            paths = session.exec(select(BehaviorExperiencePath).where(col(BehaviorExperiencePath.id).in_(behavior_ids))).all()
        path_items = {item.id: item for item in _build_path_items(session, list(paths))}
    return {
        "success": True,
        "data": {
            **debug_payload,
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
    scene_ids = {path.start_scene_node_id for path in paths}
    action_ids = {path.action_node_id for path in paths}
    outcome_ids = {path.outcome_node_id for path in paths}
    session_ids = {path.session_id for path in paths if path.session_id}
    scene_nodes = _load_scene_nodes(session, scene_ids)
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
    return [
        BehaviorPathItem(
            id=path.id or 0,
            session_id=path.session_id,
            chat_name=_chat_display_name(chat_sessions.get(path.session_id), path.session_id),
            trigger=scene_nodes[path.start_scene_node_id].name if path.start_scene_node_id in scene_nodes else "",
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
        for path in paths
    ]


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
