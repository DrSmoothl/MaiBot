from contextlib import contextmanager
from typing import Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import pytest

import src.learners.behavior_scene_graph_store as graph_store
import src.learners.behavior_pattern_store as pattern_store
import src.learners.behavior_selector as selector_module
from src.common.database.database_model import (
    BehaviorActionNode,
    BehaviorActionOutcomeEdge,
    BehaviorExperiencePath,
    BehaviorExperienceSceneLink,
    BehaviorOutcomeNode,
    BehaviorSceneActionEdge,
    BehaviorSceneCluster,
    BehaviorSceneNode,
)
from src.learners.behavior_scenario import BehaviorScenarioProfile, BehaviorScenarioSegment
from src.learners.behavior_selector import BehaviorPatternSelector
from src.learners.behavior_learner import parse_behavior_response_with_diagnostics


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
    monkeypatch.setattr(pattern_store, "get_db_session", fake_get_db_session)


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
            scene_cluster_id=refs.scene_cluster_id,
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
        confidence=0.86,
    )


def test_scene_cluster_distribution_uses_structured_tags() -> None:
    profile = _technical_config_profile()

    distribution = graph_store.build_scene_cluster_distribution(profile)
    tags = {str(item["tag"]) for item in distribution}
    total_probability = sum(float(item["probability"]) for item in distribution)

    assert "phase:已尝试无效" in tags
    assert "domain:技术配置" in tags
    assert "need:追问关键细节" in tags
    assert 0.999 <= total_probability <= 1.001


def test_behavior_scene_segment_prompt_payload_hides_scene_start() -> None:
    segment = BehaviorScenarioSegment(segment_id="s1", title="测试场景", profile=_technical_config_profile())

    payload = segment.to_prompt_payload()

    assert "scene_start" not in payload
    assert payload["profile"]["summary"]


def test_behavior_learning_parser_keeps_actor_and_learning_type() -> None:
    response = """
    [
      {
        "segment_id": "s1",
        "actor_type": "other_user",
        "learning_type": "observed_behavior",
        "action": "先表达共情，再提出一个可执行建议",
        "outcome": "对方继续补充细节",
        "source_ids": ["1", "2"]
      },
      {
        "segment_id": "s1",
        "actor_type": "maibot_self",
        "learning_type": "self_reflection",
        "action": "承认信息不足后追问关键配置",
        "outcome": "对方补充配置，排查方向更明确",
        "source_ids": ["3"]
      }
    ]
    """

    result = parse_behavior_response_with_diagnostics(response, scene_start="测试场景")

    assert result.diagnostics.accepted_item_count == 2
    assert result.candidates[0].actor_type == "other_user"
    assert result.candidates[0].learning_type == "observed_behavior"
    assert result.candidates[1].actor_type == "maibot_self"
    assert result.candidates[1].learning_type == "self_reflection"


def test_behavior_learning_parser_discards_items_missing_path_type() -> None:
    response = """
    [
      {
        "segment_id": "s1",
        "action": "先表达共情，再提出一个可执行建议",
        "outcome": "对方继续补充细节",
        "source_ids": ["1", "2"]
      },
      {
        "segment_id": "s1",
        "actor_type": "maibot_self",
        "action": "承认信息不足后追问关键配置",
        "outcome": "对方补充配置，排查方向更明确",
        "source_ids": ["3"]
      }
    ]
    """

    result = parse_behavior_response_with_diagnostics(response, scene_start="测试场景")

    assert result.diagnostics.parsed_item_count == 2
    assert result.diagnostics.accepted_item_count == 0
    assert result.diagnostics.invalid_item_count == 2
    assert result.candidates == []


def test_behavior_store_keeps_observed_and_self_paths_separate(
    monkeypatch: pytest.MonkeyPatch,
    behavior_graph_engine,
) -> None:
    _patch_graph_session(monkeypatch, behavior_graph_engine)
    profile = _technical_config_profile()
    common_kwargs = {
        "trigger": profile.to_learning_start_text(),
        "action": "承认信息不足后追问关键配置",
        "outcome": "对方补充配置，排查方向更明确",
        "source_ids": ["1"],
        "session_id": "session-a",
        "scenario_profile": profile,
        "scene_start": profile.to_learning_start_text(),
    }

    observed_path = pattern_store.upsert_behavior_experience(
        **common_kwargs,
        actor_type=pattern_store.ACTOR_OTHER_USER,
        learning_type=pattern_store.LEARNING_OBSERVED,
    )
    self_path = pattern_store.upsert_behavior_experience(
        **common_kwargs,
        actor_type=pattern_store.ACTOR_MAIBOT_SELF,
        learning_type=pattern_store.LEARNING_SELF_REFLECTION,
    )

    assert observed_path is not None
    assert self_path is not None
    assert observed_path.id != self_path.id
    assert pattern_store.apply_behavior_feedback(
        pattern_id=observed_path.id or 0,
        score_delta=1.0,
        status="success",
        reason="观察路径不接受自身反馈",
        outcome="",
        session_id="session-a",
    ) is None
    feedback_path = pattern_store.apply_behavior_feedback(
        pattern_id=self_path.id or 0,
        score_delta=1.0,
        status="success",
        reason="自身路径可以反馈",
        outcome="对话推进",
        session_id="session-a",
    )
    assert feedback_path is not None
    assert feedback_path.success_count == 1


def test_behavior_paths_share_stable_cluster_start_scene(
    monkeypatch: pytest.MonkeyPatch,
    behavior_graph_engine,
) -> None:
    _patch_graph_session(monkeypatch, behavior_graph_engine)
    first_profile = _technical_config_profile()
    second_profile = BehaviorScenarioProfile(
        summary="配置排查继续推进，用户换了一种说法描述问题仍然存在",
        user_intent="继续确认模型配置为什么没有生效",
        conversation_phase="已尝试无效",
        domain_tags=["模型选择", "技术配置"],
        behavior_needs=["给出具体检查点", "追问关键细节"],
        confidence=0.82,
    )
    first_path_id = _insert_behavior_experience_path(
        behavior_graph_engine,
        scene_start=first_profile.to_learning_start_text(),
        action="追问更底层配置并给出检查方向",
        outcome="对方继续补充配置位置",
        session_id="session-a",
        profile=first_profile,
    )
    second_path_id = _insert_behavior_experience_path(
        behavior_graph_engine,
        scene_start=second_profile.to_learning_start_text(),
        action="直接给出最小检查清单",
        outcome="对方可以按清单逐项验证配置",
        session_id="session-a",
        profile=second_profile,
    )

    with Session(behavior_graph_engine) as session:
        first_path = session.get(BehaviorExperiencePath, first_path_id)
        second_path = session.get(BehaviorExperiencePath, second_path_id)
        assert first_path is not None
        assert second_path is not None
        assert first_path.scene_cluster_id == second_path.scene_cluster_id
        scene_cluster = session.get(BehaviorSceneCluster, first_path.scene_cluster_id)
        assert scene_cluster is not None
        assert "phase:已尝试无效" in scene_cluster.normalized_tags


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
        clusters = session.exec(select(BehaviorSceneCluster)).all()
        action_nodes = session.exec(select(BehaviorActionNode)).all()
        outcome_nodes = session.exec(select(BehaviorOutcomeNode)).all()
        links = session.exec(select(BehaviorExperienceSceneLink)).all()
        scene_action_edges = session.exec(select(BehaviorSceneActionEdge)).all()
        action_outcome_edges = session.exec(select(BehaviorActionOutcomeEdge)).all()
    assert nodes
    assert clusters
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
            scene_cluster_id=1,
            action_node_id=1,
            outcome_node_id=1,
            count=3,
            session_id="session-a",
        ),
        BehaviorExperiencePath(
            id=2,
            scene_cluster_id=2,
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


def test_behavior_reference_text_hides_internal_metadata() -> None:
    profile = BehaviorScenarioProfile(
        summary="用户想替换当前模型服务商",
        user_intent="寻找便宜的识图模型 API",
        conversation_phase="配置咨询",
        domain_tags=["模型配置", "识图模型"],
        behavior_needs=["给出替换建议", "管理小模型预期"],
        risk_flags=["需确认模型支持识图"],
        confidence=0.85,
    )
    behaviors = [
        {
            "id": 1,
            "trigger": "用户询问可替换模型",
            "action": "给出具体供应商和模型建议",
            "outcome": "用户获得切换方向",
            "actor_type": "maibot_self",
            "learning_type": "self_reflection",
            "scene_graph_score": 1.6,
            "context_match_score": 0.4,
        },
        {
            "id": 2,
            "trigger": "用户继续询问价格",
            "action": "补充价格和能力边界",
            "outcome": "用户理解取舍",
            "actor_type": "other_user",
            "learning_type": "observed_behavior",
            "scene_graph_score": 1.2,
        },
    ]

    reference_text = BehaviorPatternSelector._build_group_reference_text(
        behaviors=behaviors,
        scenario_profile=profile,
    )

    assert "优先级：高" in reference_text
    assert "优先级：中" in reference_text
    assert "场景摘要：用户想替换当前模型服务商" in reference_text
    assert "当前需要：给出替换建议；管理小模型预期" in reference_text
    assert "路径类型" not in reference_text
    assert "actor_type" not in reference_text
    assert "learning_type" not in reference_text
    assert "匹配分数" not in reference_text
    assert "scene_graph_score" not in reference_text
    assert "context_match_score" not in reference_text
    assert "domain_tags" not in reference_text
    assert "confidence" not in reference_text
