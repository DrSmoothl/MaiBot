from contextlib import contextmanager
from typing import Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

import pytest

import src.learners.behavior_pattern_store as pattern_store
from src.common.database.database_model import BehaviorExperiencePath
from src.learners.behavior_learner import parse_behavior_feedback_response


@pytest.fixture(name="behavior_feedback_engine")
def behavior_feedback_engine_fixture() -> Generator:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    yield engine


def _patch_pattern_store_session(monkeypatch: pytest.MonkeyPatch, engine) -> None:
    @contextmanager
    def fake_get_db_session(auto_commit: bool = True):
        with Session(engine) as session:
            yield session
            if auto_commit:
                session.commit()

    monkeypatch.setattr(pattern_store, "get_db_session", fake_get_db_session)


def test_parse_behavior_feedback_response_keeps_adopted_effective_items() -> None:
    response = """
    {
      "feedback": [
        {
          "behavior_id": 12,
          "adopted": true,
          "status": "success",
          "score_delta": 0.7,
          "reason": "麦麦采用了追问配置的策略，用户随后补充了配置路径。",
          "outcome": "用户补充配置路径并继续排查。",
          "source_ids": ["m2", "m3"]
        },
        {
          "behavior_id": 13,
          "adopted": false,
          "status": "success",
          "score_delta": 0.5,
          "reason": "只是展示过，没有采用。",
          "outcome": "无",
          "source_ids": ["m4"]
        },
        {
          "behavior_id": 14,
          "adopted": true,
          "status": "neutral",
          "score_delta": 0,
          "reason": "后续结果不明确。",
          "outcome": "无",
          "source_ids": ["m5"]
        }
      ]
    }
    """

    feedback_items = parse_behavior_feedback_response(response)

    assert len(feedback_items) == 1
    assert feedback_items[0].behavior_id == 12
    assert feedback_items[0].status == "success"
    assert feedback_items[0].score_delta == 0.7
    assert feedback_items[0].source_ids == ["m2", "m3"]


def test_parse_behavior_feedback_response_accepts_observed_behavior_ids() -> None:
    response = """
    [
      {
        "behavior_id": 21,
        "adopted": true,
        "status": "failed",
        "score_delta": -0.6,
        "reason": "麦麦采用了该行为但用户没有继续配合。",
        "outcome": "对话没有推进。",
        "source_ids": "m2,m3"
      }
    ]
    """

    feedback_items = parse_behavior_feedback_response(response)

    assert len(feedback_items) == 1
    assert feedback_items[0].behavior_id == 21
    assert feedback_items[0].status == "failed"
    assert feedback_items[0].score_delta == -0.6
    assert feedback_items[0].source_ids == ["m2", "m3"]


def test_apply_behavior_feedback_accepts_observed_behavior_path(
    monkeypatch: pytest.MonkeyPatch,
    behavior_feedback_engine,
) -> None:
    _patch_pattern_store_session(monkeypatch, behavior_feedback_engine)

    with Session(behavior_feedback_engine) as session:
        path = BehaviorExperiencePath(
            session_id="session-a",
            scene_cluster_id=1,
            action_node_id=1,
            outcome_node_id=1,
            actor_type=pattern_store.ACTOR_OTHER_USER,
            learning_type=pattern_store.LEARNING_OBSERVED,
            score=0.0,
            success_count=0,
            failure_count=0,
            feedback_list="[]",
        )
        session.add(path)
        session.commit()
        session.refresh(path)
        path_id = path.id

    assert path_id is not None
    feedback_path = pattern_store.apply_behavior_feedback(
        pattern_id=path_id,
        score_delta=0.7,
        status="success",
        reason="麦麦采用了观察路径中的行为，后续对话推进。",
        outcome="用户继续补充信息。",
        session_id="session-a",
    )

    assert feedback_path is not None
    assert feedback_path.score == 0.7
    assert feedback_path.success_count == 1
