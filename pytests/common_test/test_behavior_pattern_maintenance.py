from contextlib import contextmanager
from datetime import datetime
from typing import Generator

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import json
import pytest

import src.learners.behavior_pattern_maintenance as maintenance_module
import src.learners.behavior_pattern_store as pattern_store
from src.common.database.database_model import (
    BehaviorActionNode,
    BehaviorExperiencePath,
    BehaviorOutcomeNode,
    BehaviorSceneCluster,
)
from src.learners.behavior_pattern_maintenance import BehaviorPatternMergeGroup, BehaviorPatternMaintenanceService


@pytest.fixture(name="behavior_maintenance_engine")
def behavior_maintenance_engine_fixture() -> Generator:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    yield engine


def _patch_maintenance_session(monkeypatch: pytest.MonkeyPatch, engine) -> None:
    @contextmanager
    def fake_get_db_session(auto_commit: bool = True):
        with Session(engine) as session:
            yield session
            if auto_commit:
                session.commit()

    monkeypatch.setattr(maintenance_module, "get_db_session", fake_get_db_session)
    monkeypatch.setattr(pattern_store, "get_db_session", fake_get_db_session)


def _insert_path(
    engine,
    *,
    session_id: str = "session-a",
    cluster_tags: tuple[str, ...] = (
        "phase:技术排查",
        "domain:技术配置",
        "domain:报错排查",
        "need:给出检查点",
        "need:追问关键细节",
        "risk:信息不足",
    ),
    action: str = "追问配置路径并给出检查清单",
    outcome: str = "用户补充配置位置并继续排查",
) -> int:
    now = datetime.now()
    with Session(engine) as session:
        normalized_tags = "|".join(sorted(cluster_tags))
        probability = 1 / len(cluster_tags)
        tag_distribution = json.dumps(
            [{"tag": tag, "probability": probability} for tag in sorted(cluster_tags)],
            ensure_ascii=False,
        )
        scene_cluster = session.exec(
            select(BehaviorSceneCluster)
            .where(BehaviorSceneCluster.session_id == session_id)
            .where(BehaviorSceneCluster.normalized_tags == normalized_tags)
        ).first()
        if scene_cluster is None:
            scene_cluster = BehaviorSceneCluster(
                session_id=session_id,
                name=normalized_tags,
                normalized_tags=normalized_tags,
                tag_distribution=tag_distribution,
                source_count=1,
                update_time=now,
            )
            session.add(scene_cluster)

        action_node = session.exec(
            select(BehaviorActionNode)
            .where(BehaviorActionNode.session_id == session_id)
            .where(BehaviorActionNode.action == action)
        ).first()
        if action_node is None:
            action_node = BehaviorActionNode(
                session_id=session_id,
                action=action,
                source_count=1,
                update_time=now,
            )
            session.add(action_node)

        outcome_node = session.exec(
            select(BehaviorOutcomeNode)
            .where(BehaviorOutcomeNode.session_id == session_id)
            .where(BehaviorOutcomeNode.outcome == outcome)
        ).first()
        if outcome_node is None:
            outcome_node = BehaviorOutcomeNode(
                session_id=session_id,
                outcome=outcome,
                source_count=1,
                update_time=now,
            )
            session.add(outcome_node)
        session.flush()
        if scene_cluster.id is None or action_node.id is None or outcome_node.id is None:
            raise AssertionError("行为维护测试节点写入失败")

        path = BehaviorExperiencePath(
            session_id=session_id,
            scene_cluster_id=scene_cluster.id,
            action_node_id=action_node.id,
            outcome_node_id=outcome_node.id,
            evidence_list="[]",
            feedback_list="[]",
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
        session.add(path)
        session.commit()
        assert path.id is not None
        return path.id


def test_different_scene_cluster_distribution_does_not_merge(
    monkeypatch: pytest.MonkeyPatch,
    behavior_maintenance_engine,
) -> None:
    _patch_maintenance_session(monkeypatch, behavior_maintenance_engine)
    technical_id = _insert_path(behavior_maintenance_engine)
    casual_id = _insert_path(
        behavior_maintenance_engine,
        cluster_tags=(
            "phase:玩笑互动",
            "domain:日常闲聊",
            "domain:群聊互动",
            "need:接梗",
            "need:保持轻松",
            "risk:避免过度严肃",
        ),
        action="追问配置路径并给出检查清单",
        outcome="用户补充配置位置并继续排查",
    )
    service = BehaviorPatternMaintenanceService()

    result = service.apply_merge_groups(
        session_id="session-a",
        merge_groups=[BehaviorPatternMergeGroup(keeper_id=technical_id, merge_ids=[casual_id])],
    )

    assert result.merged_count == 0
    with Session(behavior_maintenance_engine) as session:
        casual_path = session.get(BehaviorExperiencePath, casual_id)
        assert casual_path is not None
        assert casual_path.enabled


def test_different_branches_in_same_cluster_are_preserved(
    monkeypatch: pytest.MonkeyPatch,
    behavior_maintenance_engine,
) -> None:
    _patch_maintenance_session(monkeypatch, behavior_maintenance_engine)
    first_id = _insert_path(
        behavior_maintenance_engine,
        action="追问配置路径并给出检查清单",
        outcome="用户补充配置位置并继续排查",
    )
    second_id = _insert_path(
        behavior_maintenance_engine,
        action="先承认信息不足，再要求用户贴出完整日志",
        outcome="用户转而补充日志截图，问题进入证据收集阶段",
    )
    service = BehaviorPatternMaintenanceService()

    result = service.apply_merge_groups(
        session_id="session-a",
        merge_groups=[BehaviorPatternMergeGroup(keeper_id=first_id, merge_ids=[second_id])],
    )

    assert result.merged_count == 0
    with Session(behavior_maintenance_engine) as session:
        second_path = session.get(BehaviorExperiencePath, second_id)
        assert second_path is not None
        assert second_path.enabled


def test_duplicate_branches_in_same_cluster_can_merge(
    monkeypatch: pytest.MonkeyPatch,
    behavior_maintenance_engine,
) -> None:
    _patch_maintenance_session(monkeypatch, behavior_maintenance_engine)
    first_id = _insert_path(
        behavior_maintenance_engine,
        action="追问配置路径并给出检查清单",
        outcome="用户补充配置位置并继续排查",
    )
    second_id = _insert_path(
        behavior_maintenance_engine,
        action="追问配置路径，并给出检查清单",
        outcome="用户补充配置位置，继续排查",
    )
    service = BehaviorPatternMaintenanceService()

    result = service.apply_merge_groups(
        session_id="session-a",
        merge_groups=[BehaviorPatternMergeGroup(keeper_id=first_id, merge_ids=[second_id])],
    )

    assert result.merged_count == 1
    with Session(behavior_maintenance_engine) as session:
        first_path = session.get(BehaviorExperiencePath, first_id)
        second_path = session.get(BehaviorExperiencePath, second_id)
        assert first_path is not None
        assert second_path is not None
        assert first_path.enabled
        assert not second_path.enabled
