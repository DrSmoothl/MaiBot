from contextlib import contextmanager
from typing import Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import pytest

import src.learners.behavior_scene_graph_store as graph_store
import src.learners.behavior_selector as selector_module
from src.common.database.database_model import (
    BehaviorActionNode,
    BehaviorActionOutcomeEdge,
    BehaviorExperiencePath,
    BehaviorExperienceSceneLink,
    BehaviorOutcomeNode,
    BehaviorSceneActionEdge,
    BehaviorSceneNode,
)
from src.learners.behavior_scenario import BehaviorScenarioProfile
from src.learners.behavior_selector import BehaviorPatternSelector


@pytest.fixture(name="behavior_graph_engine")
def behavior_graph_engine_fixture() -> Generator:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    yield engine


def _patch_graph_session(monkeypatch: pytest.MonkeyPatch, engine) -> None:
    @contextmanager
    def fake_get_db_session(auto_commit: bool = True):
        with Session(engine) as session:
            yield session
            if auto_commit:
                session.commit()

    monkeypatch.setattr(graph_store, "get_db_session", fake_get_db_session)


def _insert_behavior_experience_path(
    engine,
    *,
    scene_start: str,
    action: str,
    outcome: str,
    session_id: str,
    profile: BehaviorScenarioProfile,
) -> int:
    with Session(engine) as session:
        refs = graph_store.upsert_behavior_graph_refs(
            session=session,
            session_id=session_id,
            profile=profile,
            scene_start=scene_start,
            action=action,
            outcome=outcome,
        )
        assert refs is not None
        path = BehaviorExperiencePath(
            count=1,
            session_id=session_id,
            start_scene_node_id=refs.start_scene_node_id,
            action_node_id=refs.action_node_id,
            outcome_node_id=refs.outcome_node_id,
        )
        session.add(path)
        session.flush()
        session.refresh(path)
        assert path.id is not None
        graph_store.link_behavior_experience_to_scene_graph(
            session=session,
            experience_path_id=path.id,
            session_id=session_id,
            refs=refs,
        )
        session.commit()
        return path.id


def _technical_config_profile() -> BehaviorScenarioProfile:
    return BehaviorScenarioProfile(
        summary="有人反馈模型配置已按建议调整但问题依旧",
        user_intent="继续排查配置为什么没有生效",
        conversation_phase="已尝试无效",
        domain_tags=["技术配置", "模型选择"],
        behavior_needs=["追问关键细节", "给出具体检查点"],
        retrieval_query="配置不生效 继续排查 追问日志和配置项",
        confidence=0.86,
    )


def test_link_behavior_experience_to_scene_graph_and_retrieve(
    monkeypatch: pytest.MonkeyPatch,
    behavior_graph_engine,
) -> None:
    _patch_graph_session(monkeypatch, behavior_graph_engine)
    profile = _technical_config_profile()
    path_id = _insert_behavior_experience_path(
        behavior_graph_engine,
        scene_start=profile.to_learning_start_text(),
        action="追问更底层配置并给出检查方向",
        outcome="对方继续补充配置位置",
        session_id="session-a",
        profile=profile,
    )
    scores = graph_store.retrieve_behavior_scores_from_scene_graph(
        session_ids={"session-a"},
        include_global=False,
        profile=profile,
    )

    assert path_id in scores
    assert scores[path_id] > 0
    with Session(behavior_graph_engine) as session:
        nodes = session.exec(select(BehaviorSceneNode)).all()
        action_nodes = session.exec(select(BehaviorActionNode)).all()
        outcome_nodes = session.exec(select(BehaviorOutcomeNode)).all()
        links = session.exec(select(BehaviorExperienceSceneLink)).all()
        scene_action_edges = session.exec(select(BehaviorSceneActionEdge)).all()
        action_outcome_edges = session.exec(select(BehaviorActionOutcomeEdge)).all()
    assert nodes
    assert action_nodes
    assert outcome_nodes
    assert links
    assert scene_action_edges
    assert action_outcome_edges


def test_scene_graph_selection_and_feedback_update_links(
    monkeypatch: pytest.MonkeyPatch,
    behavior_graph_engine,
) -> None:
    _patch_graph_session(monkeypatch, behavior_graph_engine)
    profile = _technical_config_profile()
    path_id = _insert_behavior_experience_path(
        behavior_graph_engine,
        scene_start=profile.to_learning_start_text(),
        action="追问更底层配置并给出检查方向",
        outcome="对方继续补充配置位置",
        session_id="session-a",
        profile=profile,
    )

    graph_store.mark_behavior_scene_links_selected(path_id)
    graph_store.apply_behavior_scene_feedback(experience_path_id=path_id, score_delta=1.5, status="success")

    with Session(behavior_graph_engine) as session:
        links = session.exec(
            select(BehaviorExperienceSceneLink).where(
                BehaviorExperienceSceneLink.behavior_experience_path_id == path_id
            )
        ).all()
        scene_action_edges = session.exec(
            select(BehaviorSceneActionEdge).where(BehaviorSceneActionEdge.behavior_experience_path_id == path_id)
        ).all()
        action_outcome_edges = session.exec(
            select(BehaviorActionOutcomeEdge).where(BehaviorActionOutcomeEdge.behavior_experience_path_id == path_id)
        ).all()

    assert links
    assert max(link.weight for link in links) > 1.0
    assert scene_action_edges
    assert max(edge.weight for edge in scene_action_edges) > 1.0
    assert action_outcome_edges
    assert max(edge.weight for edge in action_outcome_edges) > 1.0


def test_behavior_selector_prefers_scene_graph_scores(monkeypatch: pytest.MonkeyPatch) -> None:
    selector = BehaviorPatternSelector()
    profile = _technical_config_profile()
    patterns = [
        BehaviorExperiencePath(
            id=1,
            start_scene_node_id=1,
            action_node_id=1,
            outcome_node_id=1,
            count=3,
            session_id="session-a",
        ),
        BehaviorExperiencePath(
            id=2,
            start_scene_node_id=2,
            action_node_id=2,
            outcome_node_id=2,
            count=1,
            session_id="session-a",
        ),
    ]
    pattern_payloads = {
        1: {
            "id": 1,
            "trigger": "有人开玩笑",
            "action": "轻松接梗",
            "outcome": "群聊继续闲聊",
            "count": 3,
            "score": 0,
        },
        2: {
            "id": 2,
            "trigger": profile.to_learning_start_text(),
            "action": "追问更底层配置并给出检查方向",
            "outcome": "对方继续补充配置位置",
            "count": 1,
            "score": 0,
        },
    }

    monkeypatch.setattr(selector, "_resolve_behavior_group_scope", lambda session_id: ({"session-a"}, False))
    monkeypatch.setattr(selector_module.behavior_pattern_maintenance, "maybe_maintain_session", lambda **kwargs: None)
    monkeypatch.setattr(selector_module, "list_behavior_patterns_for_sessions", lambda **kwargs: patterns)
    monkeypatch.setattr(selector_module, "behavior_pattern_to_dict", lambda pattern: pattern_payloads[int(pattern.id)])
    monkeypatch.setattr(selector_module, "retrieve_behavior_scores_from_scene_graph", lambda **kwargs: {2: 4.2})

    candidates = selector._load_behavior_candidates(
        "session-a",
        context_text="配置不生效，想继续排查",
        scenario_profile=profile,
    )

    assert candidates[0]["id"] == 2
    assert candidates[0]["scene_graph_score"] == 4.2
