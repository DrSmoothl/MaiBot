from argparse import ArgumentParser, Namespace
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from random import Random
from typing import Any, Iterable, Optional, Sequence

import asyncio
import json
import shutil
import sqlite3
import sys
import time

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, create_engine, select


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.evaluate_offline_behavior_retrieval import (  # noqa: E402
    EvalWindow,
    _brief_candidate,
    _build_eval_windows,
    _discover_learned_session_ids,
    _load_trained_message_ids,
    _profile_payload,
    _scene_cluster_tag_summary,
)
from scripts.offline_behavior_learning import (  # noqa: E402
    MIN_RANDOM_CHAT_MESSAGES,
    _build_engine,
    _build_session_provider,
    _discover_session_ids,
    _initialize_target_database,
    _patch_behavior_model_name,
    _patch_behavior_storage,
    _resolve_path,
    run_learning,
)
from src.chat.message_receive.message import SessionMessage  # noqa: E402
from src.common.database.database_model import (  # noqa: E402
    BehaviorExperiencePath,
    BehaviorSceneCluster,
    BehaviorSceneTagCluster,
)
from src.learners.behavior_learner import BehaviorLearner  # noqa: E402
from src.learners.behavior_pattern_store import behavior_pattern_to_dict  # noqa: E402
from src.learners.behavior_scene_graph_store import (  # noqa: E402
    _distribution_to_mapping,
    _load_cluster_distribution,
    _load_tag_cluster_lookup,
    _normalize_tag_kind,
    _score_behavior_clusters,
    _score_scene_clusters_by_expanded_tags,
    _session_scope_condition,
    build_scene_cluster_distribution,
    retrieve_behavior_scores_from_scene_graph,
)

SIDECHANNEL_TABLE = "behavior_experience_tag_links"
SIDECHANNEL_KINDS = {"need", "attitude"}
SIDECHANNEL_BONUS_FACTOR = 0.55
SIDECHANNEL_BONUS_OVER_CLUSTER_CAP = 0.35
SIDECHANNEL_TOPK = 64


def _round_ms(value: float) -> float:
    return round(value, 3)


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - lower_index
    return ordered[lower_index] * (1 - fraction) + ordered[upper_index] * fraction


def _duration_summary(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "avg_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": len(values),
        "avg_ms": _round_ms(sum(values) / len(values)),
        "p50_ms": _round_ms(_percentile(values, 0.50)),
        "p95_ms": _round_ms(_percentile(values, 0.95)),
        "max_ms": _round_ms(max(values)),
    }


def _llm_usage_summary(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {
            "available": False,
            "reason": "数据库不存在",
        }
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'llm_usage'"
        ).fetchone()
        if table_exists is None:
            return {
                "available": False,
                "reason": "llm_usage 表不存在",
            }
        rows = connection.execute(
            """
            SELECT
                request_type,
                model_name,
                model_assign_name,
                task_name,
                time_cost,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                prompt_cache_hit_tokens,
                prompt_cache_miss_tokens,
                cost
            FROM llm_usage
            """
        ).fetchall()
    summaries: dict[str, dict[str, Any]] = {}
    for row in rows:
        request_type = str(row["request_type"] or "unknown")
        summary = summaries.setdefault(
            request_type,
            {
                "count": 0,
                "time_cost_seconds": [],
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "prompt_cache_hit_tokens": 0,
                "prompt_cache_miss_tokens": 0,
                "cost_yuan": 0.0,
                "models": Counter(),
                "assign_names": Counter(),
                "task_names": Counter(),
            },
        )
        summary["count"] += 1
        summary["time_cost_seconds"].append(float(row["time_cost"] or 0.0))
        summary["prompt_tokens"] += int(row["prompt_tokens"] or 0)
        summary["completion_tokens"] += int(row["completion_tokens"] or 0)
        summary["total_tokens"] += int(row["total_tokens"] or 0)
        summary["prompt_cache_hit_tokens"] += int(row["prompt_cache_hit_tokens"] or 0)
        summary["prompt_cache_miss_tokens"] += int(row["prompt_cache_miss_tokens"] or 0)
        summary["cost_yuan"] += float(row["cost"] or 0.0)
        summary["models"].update([str(row["model_name"] or "")])
        if row["model_assign_name"]:
            summary["assign_names"].update([str(row["model_assign_name"])])
        if row["task_name"]:
            summary["task_names"].update([str(row["task_name"])])

    by_request_type: dict[str, dict[str, Any]] = {}
    all_time_costs: list[float] = []
    totals = {
        "count": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "prompt_cache_hit_tokens": 0,
        "prompt_cache_miss_tokens": 0,
        "cost_yuan": 0.0,
    }
    for request_type, summary in sorted(summaries.items()):
        time_costs = summary["time_cost_seconds"]
        all_time_costs.extend(time_costs)
        totals["count"] += int(summary["count"])
        totals["prompt_tokens"] += int(summary["prompt_tokens"])
        totals["completion_tokens"] += int(summary["completion_tokens"])
        totals["total_tokens"] += int(summary["total_tokens"])
        totals["prompt_cache_hit_tokens"] += int(summary["prompt_cache_hit_tokens"])
        totals["prompt_cache_miss_tokens"] += int(summary["prompt_cache_miss_tokens"])
        totals["cost_yuan"] += float(summary["cost_yuan"])
        by_request_type[request_type] = {
            "count": summary["count"],
            "total_time_cost_seconds": round(sum(time_costs), 3),
            "avg_time_cost_seconds": round(sum(time_costs) / len(time_costs), 3) if time_costs else 0.0,
            "p50_time_cost_seconds": round(_percentile(time_costs, 0.50), 3),
            "p95_time_cost_seconds": round(_percentile(time_costs, 0.95), 3),
            "max_time_cost_seconds": round(max(time_costs), 3) if time_costs else 0.0,
            "prompt_tokens": summary["prompt_tokens"],
            "completion_tokens": summary["completion_tokens"],
            "total_tokens": summary["total_tokens"],
            "prompt_cache_hit_tokens": summary["prompt_cache_hit_tokens"],
            "prompt_cache_miss_tokens": summary["prompt_cache_miss_tokens"],
            "cost_yuan": round(summary["cost_yuan"], 6),
            "models": dict(summary["models"].most_common()),
            "assign_names": dict(summary["assign_names"].most_common()),
            "task_names": dict(summary["task_names"].most_common()),
        }

    return {
        "available": True,
        "first_token_latency_available": False,
        "output_time_available": False,
        "note": "当前 llm_usage 只记录 LLM 请求总耗时 time_cost，未记录首字延迟和流式输出耗时。",
        "total": {
            **totals,
            "cost_yuan": round(float(totals["cost_yuan"]), 6),
            "total_time_cost_seconds": round(sum(all_time_costs), 3),
            "avg_time_cost_seconds": round(sum(all_time_costs) / len(all_time_costs), 3) if all_time_costs else 0.0,
            "p50_time_cost_seconds": round(_percentile(all_time_costs, 0.50), 3),
            "p95_time_cost_seconds": round(_percentile(all_time_costs, 0.95), 3),
            "max_time_cost_seconds": round(max(all_time_costs), 3) if all_time_costs else 0.0,
        },
        "by_request_type": by_request_type,
    }


@dataclass(frozen=True)
class ModeResult:
    candidates: list[dict[str, Any]]
    elapsed_ms: float


@dataclass(frozen=True)
class SidechannelLinkEvent:
    session_id: Optional[str]
    behavior_experience_path_id: int
    tag_kind: str
    cluster_key: str
    link_role: str
    weight: float
    count: int


def _remove_file(path: Path) -> None:
    if path.exists():
        path.unlink()
    wal_path = Path(f"{path}-wal")
    shm_path = Path(f"{path}-shm")
    if wal_path.exists():
        wal_path.unlink()
    if shm_path.exists():
        shm_path.unlink()


def _copy_sqlite_database(source: Path, target: Path) -> None:
    _remove_file(target)
    with sqlite3.connect(source) as source_connection:
        source_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        with sqlite3.connect(target) as target_connection:
            source_connection.backup(target_connection)


def _learning_args(args: Namespace, *, target_db: Path, progress_jsonl: Path) -> Namespace:
    return Namespace(
        chat_id=list(args.chat_id or []),
        session_id=list(args.session_id or []),
        source_db=str(args.source_db),
        target_db=str(target_db),
        progress_jsonl=str(progress_jsonl),
        since=args.since,
        until=args.until,
        limit=args.limit,
        window_size=args.window_size,
        step=args.window_size,
        max_batches=0,
        random_windows=args.train_windows,
        seed=args.seed,
        model_name=args.model_name,
        uniform_by_chat=False,
        balanced_by_chat=True,
        min_text_length=args.min_text_length,
        min_messages_for_extraction=args.min_messages_for_extraction,
        dry_run=False,
    )


async def _run_or_reuse_learning(
    args: Namespace,
    *,
    mode_name: str,
    target_db: Path,
    progress_jsonl: Path,
    write_sidechannel_during_training: bool = False,
) -> None:
    if args.reuse and target_db.exists() and progress_jsonl.exists():
        print(f"[{mode_name}] 复用已有学习库: {target_db}")
        return
    _remove_file(target_db)
    _remove_file(progress_jsonl)
    print(f"\n[{mode_name}] 开始离线学习 {args.train_windows} 个窗口")
    restore_patch = None
    if write_sidechannel_during_training:
        target_engine = _build_engine(target_db)
        _initialize_target_database(target_engine)
        target_session_local = sessionmaker(autocommit=False, autoflush=False, bind=target_engine, class_=Session)
        with target_session_local() as target_session:
            _create_sidechannel_table(target_session)
            target_session.commit()
        restore_patch = _install_training_sidechannel_patch(_build_session_provider(target_session_local))
    try:
        await run_learning(_learning_args(args, target_db=target_db, progress_jsonl=progress_jsonl))
    finally:
        if restore_patch is not None:
            restore_patch()


def _create_sidechannel_table(session: Session) -> None:
    session.exec(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SIDECHANNEL_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id VARCHAR(255),
                behavior_experience_path_id INTEGER NOT NULL,
                tag_kind VARCHAR(40) NOT NULL,
                cluster_key TEXT NOT NULL,
                link_role VARCHAR(40) NOT NULL,
                weight FLOAT NOT NULL DEFAULT 1,
                count INTEGER NOT NULL DEFAULT 0,
                update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (behavior_experience_path_id, tag_kind, cluster_key, link_role)
            )
            """
        )
    )
    session.exec(text(f"CREATE INDEX IF NOT EXISTS ix_{SIDECHANNEL_TABLE}_scope_tag ON {SIDECHANNEL_TABLE} (session_id, tag_kind, cluster_key)"))
    session.exec(text(f"CREATE INDEX IF NOT EXISTS ix_{SIDECHANNEL_TABLE}_path ON {SIDECHANNEL_TABLE} (behavior_experience_path_id)"))


def _write_profile_sidechannel_links(
    session: Session,
    *,
    path: BehaviorExperiencePath,
    profile: Any,
) -> int:
    if path.id is None:
        return 0
    _create_sidechannel_table(session)
    tag_lookup = _load_tag_cluster_lookup(session)
    tag_probs = _distribution_to_mapping(
        build_scene_cluster_distribution(profile, tag_lookup=tag_lookup),
        tag_lookup=tag_lookup,
    )
    inserted = 0
    for tag_name, probability in tag_probs.items():
        if ":" not in tag_name:
            continue
        tag_kind, cluster_key = tag_name.split(":", 1)
        tag_kind = _normalize_tag_kind(tag_kind)
        if tag_kind not in SIDECHANNEL_KINDS or not cluster_key:
            continue
        session.exec(
            text(
                f"""
                INSERT INTO {SIDECHANNEL_TABLE} (
                    session_id,
                    behavior_experience_path_id,
                    tag_kind,
                    cluster_key,
                    link_role,
                    weight,
                    count,
                    update_time
                )
                VALUES (
                    :session_id,
                    :path_id,
                    :tag_kind,
                    :cluster_key,
                    :link_role,
                    :weight,
                    :count,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (behavior_experience_path_id, tag_kind, cluster_key, link_role)
                DO UPDATE SET
                    weight = excluded.weight,
                    count = {SIDECHANNEL_TABLE}.count + 1,
                    update_time = CURRENT_TIMESTAMP
                """
            ),
            params={
                "session_id": path.session_id,
                "path_id": path.id,
                "tag_kind": tag_kind,
                "cluster_key": cluster_key,
                "link_role": "need" if tag_kind == "need" else "other_trait",
                "weight": float(probability or 0.0),
                "count": max(int(path.count or 0), 1),
            },
        )
        inserted += 1
    return inserted


def _collect_profile_sidechannel_link_events(
    session: Session,
    *,
    path: BehaviorExperiencePath,
    profile: Any,
) -> list[SidechannelLinkEvent]:
    if path.id is None:
        return []
    tag_lookup = _load_tag_cluster_lookup(session)
    tag_probs = _distribution_to_mapping(
        build_scene_cluster_distribution(profile, tag_lookup=tag_lookup),
        tag_lookup=tag_lookup,
    )
    events: list[SidechannelLinkEvent] = []
    for tag_name, probability in tag_probs.items():
        if ":" not in tag_name:
            continue
        tag_kind, cluster_key = tag_name.split(":", 1)
        tag_kind = _normalize_tag_kind(tag_kind)
        if tag_kind not in SIDECHANNEL_KINDS or not cluster_key:
            continue
        events.append(
            SidechannelLinkEvent(
                session_id=path.session_id,
                behavior_experience_path_id=int(path.id),
                tag_kind=tag_kind,
                cluster_key=cluster_key,
                link_role="need" if tag_kind == "need" else "other_trait",
                weight=float(probability or 0.0),
                count=max(int(path.count or 0), 1),
            )
        )
    return events


def _write_sidechannel_link_events(session: Session, events: Iterable[SidechannelLinkEvent]) -> int:
    _create_sidechannel_table(session)
    inserted = 0
    for event in events:
        session.exec(
            text(
                f"""
                INSERT INTO {SIDECHANNEL_TABLE} (
                    session_id,
                    behavior_experience_path_id,
                    tag_kind,
                    cluster_key,
                    link_role,
                    weight,
                    count,
                    update_time
                )
                VALUES (
                    :session_id,
                    :path_id,
                    :tag_kind,
                    :cluster_key,
                    :link_role,
                    :weight,
                    :count,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (behavior_experience_path_id, tag_kind, cluster_key, link_role)
                DO UPDATE SET
                    weight = excluded.weight,
                    count = excluded.count,
                    update_time = CURRENT_TIMESTAMP
                """
            ),
            params={
                "session_id": event.session_id,
                "path_id": event.behavior_experience_path_id,
                "tag_kind": event.tag_kind,
                "cluster_key": event.cluster_key,
                "link_role": event.link_role,
                "weight": event.weight,
                "count": event.count,
            },
        )
        inserted += 1
    session.commit()
    return inserted


def _read_sidechannel_link_events(session: Session) -> list[SidechannelLinkEvent]:
    _create_sidechannel_table(session)
    rows = session.exec(
        text(
            f"""
            SELECT
                session_id,
                behavior_experience_path_id,
                tag_kind,
                cluster_key,
                link_role,
                weight,
                count
            FROM {SIDECHANNEL_TABLE}
            """
        )
    ).all()
    return [
        SidechannelLinkEvent(
            session_id=str(row[0]) if row[0] is not None else None,
            behavior_experience_path_id=int(row[1]),
            tag_kind=str(row[2]),
            cluster_key=str(row[3]),
            link_role=str(row[4]),
            weight=float(row[5] or 0.0),
            count=int(row[6] or 0),
        )
        for row in rows
    ]


async def _learn_with_sidechannel_capture(
    args: Namespace,
    *,
    target_db: Path,
    progress_jsonl: Path,
    sidechannel_events: list[SidechannelLinkEvent],
    train_windows: int,
    title: str,
) -> None:
    target_engine = _build_engine(target_db)
    _initialize_target_database(target_engine)
    target_session_local = sessionmaker(autocommit=False, autoflush=False, bind=target_engine, class_=Session)
    restore_patch = _install_training_sidechannel_event_patch(
        _build_session_provider(target_session_local),
        sidechannel_events,
    )
    learning_args = Namespace(**vars(args))
    learning_args.train_windows = train_windows
    try:
        print(f"\n[{title}] 开始离线学习 {train_windows} 个窗口")
        await run_learning(_learning_args(learning_args, target_db=target_db, progress_jsonl=progress_jsonl))
    finally:
        restore_patch()
        target_engine.dispose()


def _rebuild_side_db_from_original(
    *,
    original_db: Path,
    side_db: Path,
    original_progress: Path,
    side_progress: Path,
    sidechannel_events: list[SidechannelLinkEvent],
) -> int:
    _remove_file(side_progress)
    _copy_sqlite_database(original_db, side_db)
    if original_progress.exists():
        shutil.copy2(original_progress, side_progress)
    side_engine = _build_engine(side_db)
    _initialize_target_database(side_engine)
    side_session_local = sessionmaker(autocommit=False, autoflush=False, bind=side_engine, class_=Session)
    with side_session_local() as side_session:
        return _write_sidechannel_link_events(side_session, sidechannel_events)


def _install_training_sidechannel_patch(get_target_session):
    import src.learners.behavior_learner as behavior_learner
    import src.learners.behavior_pattern_store as behavior_pattern_store

    original_upsert = behavior_pattern_store.upsert_behavior_experience
    original_upsert_pattern = behavior_pattern_store.upsert_behavior_pattern
    original_learner_upsert = behavior_learner.upsert_behavior_pattern

    def upsert_with_sidechannel(*args, **kwargs):
        path = original_upsert(*args, **kwargs)
        profile = kwargs.get("profile")
        if path is not None and profile is not None and getattr(path, "id", None) is not None:
            with get_target_session() as session:
                _write_profile_sidechannel_links(session, path=path, profile=profile)
        return path

    behavior_pattern_store.upsert_behavior_experience = upsert_with_sidechannel
    behavior_pattern_store.upsert_behavior_pattern = upsert_with_sidechannel
    behavior_learner.upsert_behavior_pattern = upsert_with_sidechannel

    def restore() -> None:
        behavior_pattern_store.upsert_behavior_experience = original_upsert
        behavior_pattern_store.upsert_behavior_pattern = original_upsert_pattern
        behavior_learner.upsert_behavior_pattern = original_learner_upsert

    return restore


def _install_training_sidechannel_event_patch(get_target_session, events: list[SidechannelLinkEvent]):
    import src.learners.behavior_learner as behavior_learner
    import src.learners.behavior_pattern_store as behavior_pattern_store

    original_upsert = behavior_pattern_store.upsert_behavior_experience
    original_upsert_pattern = behavior_pattern_store.upsert_behavior_pattern
    original_learner_upsert = behavior_learner.upsert_behavior_pattern

    def upsert_with_event_capture(*args, **kwargs):
        path = original_upsert(*args, **kwargs)
        profile = kwargs.get("scenario_profile")
        if profile is None:
            profile = kwargs.get("profile")
        if path is not None and profile is not None and getattr(path, "id", None) is not None:
            with get_target_session(auto_commit=False) as session:
                events.extend(_collect_profile_sidechannel_link_events(session, path=path, profile=profile))
        return path

    behavior_pattern_store.upsert_behavior_experience = upsert_with_event_capture
    behavior_pattern_store.upsert_behavior_pattern = upsert_with_event_capture
    behavior_learner.upsert_behavior_pattern = upsert_with_event_capture

    def restore() -> None:
        behavior_pattern_store.upsert_behavior_experience = original_upsert
        behavior_pattern_store.upsert_behavior_pattern = original_upsert_pattern
        behavior_learner.upsert_behavior_pattern = original_learner_upsert

    return restore


def _rebuild_sidechannel_links(session: Session) -> int:
    _create_sidechannel_table(session)
    session.exec(text(f"DELETE FROM {SIDECHANNEL_TABLE}"))
    tag_lookup = _load_tag_cluster_lookup(session)
    paths = session.exec(select(BehaviorExperiencePath).where(BehaviorExperiencePath.enabled.is_(True))).all()  # type: ignore[attr-defined]
    inserted = 0
    for path in paths:
        if path.id is None:
            continue
        cluster = session.get(BehaviorSceneCluster, path.scene_cluster_id)
        if cluster is None:
            continue
        tag_probs = _distribution_to_mapping(_load_cluster_distribution(cluster.tag_distribution), tag_lookup=tag_lookup)
        for tag_name, probability in tag_probs.items():
            if ":" not in tag_name:
                continue
            tag_kind, cluster_key = tag_name.split(":", 1)
            tag_kind = _normalize_tag_kind(tag_kind)
            if tag_kind not in SIDECHANNEL_KINDS or not cluster_key:
                continue
            session.exec(
                text(
                    f"""
                    INSERT OR REPLACE INTO {SIDECHANNEL_TABLE} (
                        session_id,
                        behavior_experience_path_id,
                        tag_kind,
                        cluster_key,
                        link_role,
                        weight,
                        count,
                        update_time
                    )
                    VALUES (
                        :session_id,
                        :path_id,
                        :tag_kind,
                        :cluster_key,
                        :link_role,
                        :weight,
                        :count,
                        CURRENT_TIMESTAMP
                    )
                    """
                ),
                params={
                    "session_id": path.session_id,
                    "path_id": path.id,
                    "tag_kind": tag_kind,
                    "cluster_key": cluster_key,
                    "link_role": "need" if tag_kind == "need" else "other_trait",
                    "weight": float(probability or 0.0),
                    "count": int(path.count or 0),
                },
            )
            inserted += 1
    session.commit()
    return inserted


def _count_sidechannel_links(session: Session) -> int:
    _create_sidechannel_table(session)
    row = session.exec(text(f"SELECT COUNT(*) FROM {SIDECHANNEL_TABLE}")).first()
    return int(row[0] or 0) if row is not None else 0


def _sidechannel_tag_scores(
    session: Session,
    *,
    session_ids: set[str],
    include_global: bool,
    profile: Any,
    allowed_cluster_ids: set[int],
) -> dict[int, float]:
    if not allowed_cluster_ids:
        return {}
    tag_lookup = _load_tag_cluster_lookup(session)
    tag_probs = {
        tag: probability
        for tag, probability in _distribution_to_mapping(
            build_scene_cluster_distribution(profile, tag_lookup=tag_lookup),
            tag_lookup=tag_lookup,
        ).items()
        if tag.split(":", 1)[0] in SIDECHANNEL_KINDS
    }
    if not tag_probs:
        return {}
    entries = []
    for tag_name, probability in tag_probs.items():
        tag_kind, cluster_key = tag_name.split(":", 1)
        entries.append((tag_kind, cluster_key, probability))
    tag_kinds = {tag_kind for tag_kind, _, _ in entries}
    cluster_keys = {cluster_key for _, cluster_key, _ in entries}
    probability_by_key = {(tag_kind, cluster_key): probability for tag_kind, cluster_key, probability in entries}
    params: dict[str, Any] = {
        "bonus_factor": SIDECHANNEL_BONUS_FACTOR,
    }
    kind_placeholders = []
    for index, tag_kind in enumerate(sorted(tag_kinds)):
        key = f"kind_{index}"
        kind_placeholders.append(f":{key}")
        params[key] = tag_kind
    cluster_placeholders = []
    for index, cluster_key in enumerate(sorted(cluster_keys)):
        key = f"cluster_{index}"
        cluster_placeholders.append(f":{key}")
        params[key] = cluster_key
    matched_cluster_placeholders = []
    for index, cluster_id in enumerate(sorted(allowed_cluster_ids)):
        key = f"matched_cluster_{index}"
        matched_cluster_placeholders.append(f":{key}")
        params[key] = cluster_id
    scope_clause = ""
    if not include_global:
        params["global_session"] = ""
        scope_keys = []
        for index, session_id in enumerate(sorted(session_ids)):
            key = f"session_{index}"
            scope_keys.append(f":{key}")
            params[key] = session_id
        if scope_keys:
            scope_clause = f"AND (session_id IN ({', '.join(scope_keys)}) OR session_id IS NULL)"
        else:
            scope_clause = "AND session_id IS NULL"
    rows = session.exec(
        text(
            f"""
            SELECT
                link.behavior_experience_path_id,
                link.tag_kind,
                link.cluster_key,
                link.weight,
                link.count
            FROM {SIDECHANNEL_TABLE} AS link
            INNER JOIN behavior_experience_paths AS path
                ON path.id = link.behavior_experience_path_id
            WHERE link.tag_kind IN ({", ".join(kind_placeholders)})
              AND link.cluster_key IN ({", ".join(cluster_placeholders)})
              AND path.scene_cluster_id IN ({", ".join(matched_cluster_placeholders)})
              AND path.enabled = 1
              {scope_clause.replace("session_id", "link.session_id")}
            """
        ),
        params=params,
    ).all()

    scores: dict[int, float] = {}
    for path_id, tag_kind, cluster_key, weight, count in rows:
        probability = probability_by_key.get((str(tag_kind), str(cluster_key)), 0.0)
        if probability <= 0:
            continue
        history_bonus = 1.0 + min(float(count or 0), 20.0) * 0.02
        score = probability * float(weight or 1.0) * history_bonus * SIDECHANNEL_BONUS_FACTOR
        scores[int(path_id)] = scores.get(int(path_id), 0.0) + score
    return dict(sorted(scores.items(), key=lambda item: item[1], reverse=True)[:SIDECHANNEL_TOPK])


def _score_paths_from_clusters(
    session: Session,
    *,
    cluster_scores: dict[int, float],
    session_ids: set[str],
    include_global: bool,
) -> dict[int, float]:
    if not cluster_scores:
        return {}
    statement = select(BehaviorExperiencePath).where(BehaviorExperiencePath.scene_cluster_id.in_(set(cluster_scores)))  # type: ignore[attr-defined]
    if not include_global:
        statement = statement.where(_session_scope_condition(BehaviorExperiencePath, session_ids))
    scores: dict[int, float] = {}
    for path in session.exec(statement).all():
        if path.id is None or not path.enabled:
            continue
        history_bonus = 1.0 + min(float(path.count or 0), 20.0) * 0.02
        scores[int(path.id)] = scores.get(int(path.id), 0.0) + cluster_scores.get(path.scene_cluster_id, 0.0) * history_bonus
    return scores


def _candidate_payloads(
    *,
    scores: dict[int, float],
    query_session_id: str,
    max_count: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for behavior_id, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)[:max_count]:
        candidate = behavior_pattern_to_dict(type("PathRef", (), {"id": behavior_id})())
        if not candidate:
            continue
        candidate["scene_tags"] = _scene_cluster_tag_summary(candidate.get("scene_cluster_id"))
        candidate["retrieval_score"] = round(float(score or 0.0), 4)
        candidates.append(_brief_candidate(candidate, query_session_id=query_session_id))
    return candidates


def _retrieve_original(
    *,
    session_id: str,
    profile: Any,
    max_count: int,
    include_global: bool,
) -> ModeResult:
    start = time.perf_counter()
    scores = retrieve_behavior_scores_from_scene_graph(
        session_ids={session_id},
        include_global=include_global,
        profile=profile,
        max_count=max_count,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    return ModeResult(
        candidates=_candidate_payloads(scores=scores, query_session_id=session_id, max_count=max_count),
        elapsed_ms=elapsed_ms,
    )


def _retrieve_sidechannel(
    *,
    session: Session,
    session_id: str,
    profile: Any,
    max_count: int,
    include_global: bool,
) -> ModeResult:
    start = time.perf_counter()
    cluster_scores, _ = _score_scene_clusters_by_expanded_tags(
        session,
        profile=profile,
        session_ids={session_id},
        include_global=include_global,
    )
    scores = _score_behavior_clusters(
        session,
        cluster_scores=cluster_scores,
        session_ids={session_id},
        include_global=include_global,
    )
    side_scores = _sidechannel_tag_scores(
        session,
        session_ids={session_id},
        include_global=include_global,
        profile=profile,
        allowed_cluster_ids=set(cluster_scores),
    )
    for path_id, score in side_scores.items():
        cluster_score_cap = max(scores.get(path_id, 0.0), 0.0) * SIDECHANNEL_BONUS_OVER_CLUSTER_CAP
        capped_score = min(score, cluster_score_cap) if cluster_score_cap > 0 else 0.0
        if capped_score > 0:
            scores[path_id] = scores.get(path_id, 0.0) + capped_score
    elapsed_ms = (time.perf_counter() - start) * 1000
    return ModeResult(
        candidates=_candidate_payloads(scores=scores, query_session_id=session_id, max_count=max_count),
        elapsed_ms=elapsed_ms,
    )


async def _analyze_window(window: EvalWindow) -> list[Any]:
    learner = BehaviorLearner(window.session_id)

    def resolve_learning_session_id(self, pending_messages: list[SessionMessage]) -> Optional[str]:
        return window.session_id

    learner._resolve_learning_session_id = resolve_learning_session_id.__get__(learner, BehaviorLearner)  # type: ignore[method-assign]
    return await learner._analyze_learning_scene_segments(window.messages, learning_session_id=window.session_id)


def _summarize_segments(segments: list[dict[str, Any]], *, mode: str) -> dict[str, Any]:
    total = len(segments)
    hits = sum(1 for item in segments if item[mode]["candidates"])
    top1_same = sum(1 for item in segments if item[mode]["candidates"] and item[mode]["candidates"][0].get("same_chat"))
    top1_cross = sum(1 for item in segments if item[mode]["candidates"] and not item[mode]["candidates"][0].get("same_chat"))
    candidate_count = sum(len(item[mode]["candidates"]) for item in segments)
    top_scores = [float(item[mode]["candidates"][0].get("score") or 0.0) for item in segments if item[mode]["candidates"]]
    elapsed = [float(item[mode]["elapsed_ms"] or 0.0) for item in segments]
    top_ids = [item[mode]["candidates"][0].get("id") for item in segments if item[mode]["candidates"]]
    return {
        "segment_count": total,
        "hit_segments": hits,
        "hit_rate": round(hits / total, 4) if total else 0.0,
        "avg_candidate_count": round(candidate_count / total, 4) if total else 0.0,
        "avg_top_score": round(sum(top_scores) / len(top_scores), 4) if top_scores else 0.0,
        "avg_elapsed_ms": round(sum(elapsed) / len(elapsed), 3) if elapsed else 0.0,
        "retrieval_elapsed": _duration_summary(elapsed),
        "top1_same_chat_count": top1_same,
        "top1_cross_chat_count": top1_cross,
        "unique_top1_count": len(set(top_ids)),
        "top1_distribution": dict(Counter(top_ids).most_common(10)),
    }


def _summarize_window_timings(window_timings: list[dict[str, Any]]) -> dict[str, Any]:
    scene_analysis_elapsed = [float(item["scene_analysis_elapsed_ms"] or 0.0) for item in window_timings]
    segment_counts = [int(item["segment_count"] or 0) for item in window_timings]
    return {
        "scene_analysis_elapsed": _duration_summary(scene_analysis_elapsed),
        "total_scene_segments": sum(segment_counts),
        "avg_segments_per_window": round(sum(segment_counts) / len(segment_counts), 3) if segment_counts else 0.0,
    }


async def _evaluate_compare(args: Namespace, *, original_db: Path, side_db: Path, progress_jsonl: Path) -> dict[str, Any]:
    evaluation_start = time.perf_counter()
    source_engine = _build_engine(args.source_db, readonly=True)
    side_engine = _build_engine(side_db)
    original_engine = _build_engine(original_db)
    _initialize_target_database(side_engine)
    _initialize_target_database(original_engine)

    side_session_local = sessionmaker(autocommit=False, autoflush=False, bind=side_engine, class_=Session)
    original_session_local = sessionmaker(autocommit=False, autoflush=False, bind=original_engine, class_=Session)
    source_session_local = sessionmaker(autocommit=False, autoflush=False, bind=source_engine, class_=Session)

    with side_session_local() as side_session:
        link_count = _count_sidechannel_links(side_session)
        if link_count <= 0:
            link_count = _rebuild_sidechannel_links(side_session)

    trained_message_ids = _load_trained_message_ids(progress_jsonl)
    with source_session_local() as source_session, side_session_local() as side_session:
        learned_session_ids = _discover_learned_session_ids(side_session)
        if not learned_session_ids:
            learned_session_ids = _discover_session_ids(source_session, min_messages=MIN_RANDOM_CHAT_MESSAGES)
        eval_windows = _build_eval_windows(
            source_session,
            session_ids=learned_session_ids,
            trained_message_ids=trained_message_ids,
            window_size=args.window_size,
            min_text_length=args.min_text_length,
            limit=args.limit,
            seed=args.seed + 991,
            samples=args.eval_windows,
            balanced_by_chat=True,
        )

    results: list[dict[str, Any]] = []
    window_timings: list[dict[str, Any]] = []
    side_provider = _build_session_provider(side_session_local)
    original_provider = _build_session_provider(original_session_local)
    _patch_behavior_model_name(args.model_name)

    for window_index, window in enumerate(eval_windows, start=1):
        print(f"[评测 {window_index}/{len(eval_windows)}] {window.chat_name} {window.session_id}")
        analysis_start = time.perf_counter()
        _patch_behavior_storage(original_provider)
        segments = await _analyze_window(window)
        analysis_elapsed_ms = (time.perf_counter() - analysis_start) * 1000
        window_timings.append(
            {
                "window_index": window_index,
                "session_id": window.session_id,
                "chat_name": window.chat_name,
                "segment_count": len(segments),
                "scene_analysis_elapsed_ms": _round_ms(analysis_elapsed_ms),
            }
        )
        for segment in segments:
            _patch_behavior_storage(original_provider)
            original_result = _retrieve_original(
                session_id=window.session_id,
                profile=segment.profile,
                max_count=args.max_count,
                include_global=True,
            )
            _patch_behavior_storage(side_provider)
            with side_session_local() as side_session:
                side_result = _retrieve_sidechannel(
                    session=side_session,
                    session_id=window.session_id,
                    profile=segment.profile,
                    max_count=args.max_count,
                    include_global=True,
                )
            results.append(
                {
                    "window_index": window_index,
                    "session_id": window.session_id,
                    "chat_name": window.chat_name,
                    "segment_id": segment.segment_id,
                    "title": segment.title,
                    "profile": _profile_payload(segment.profile),
                    "original": {
                        "elapsed_ms": round(original_result.elapsed_ms, 3),
                        "candidates": original_result.candidates,
                    },
                    "sidechannel": {
                        "elapsed_ms": round(side_result.elapsed_ms, 3),
                        "candidates": side_result.candidates,
                    },
                }
            )

    comparison = {
        "same_top1": 0,
        "different_top1": 0,
        "original_empty_side_hit": 0,
        "original_hit_side_empty": 0,
    }
    for item in results:
        original_candidates = item["original"]["candidates"]
        side_candidates = item["sidechannel"]["candidates"]
        if not original_candidates and side_candidates:
            comparison["original_empty_side_hit"] += 1
        if original_candidates and not side_candidates:
            comparison["original_hit_side_empty"] += 1
        if original_candidates and side_candidates:
            if original_candidates[0].get("id") == side_candidates[0].get("id"):
                comparison["same_top1"] += 1
            else:
                comparison["different_top1"] += 1

    return {
        "source_db": str(args.source_db),
        "original_db": str(original_db),
        "sidechannel_db": str(side_db),
        "progress_jsonl": str(progress_jsonl),
        "train_windows": args.train_windows,
        "append_train_windows": args.append_train_windows,
        "eval_windows": len(eval_windows),
        "segment_count": len(results),
        "sidechannel_link_count": link_count,
        "stats": {
            "original": _summarize_segments(results, mode="original"),
            "sidechannel": _summarize_segments(results, mode="sidechannel"),
            "comparison": comparison,
        },
        "timing": {
            "evaluation_wall_seconds": round(time.perf_counter() - evaluation_start, 3),
            "windows": _summarize_window_timings(window_timings),
        },
        "window_timings": window_timings,
        "segments": results,
    }


def _write_markdown(report: dict[str, Any], output: Path) -> None:
    timing = report.get("timing", {})
    window_timing = timing.get("windows", {})
    scene_analysis = window_timing.get("scene_analysis_elapsed", {})
    lines = [
        "# Need/Other Trait 旁路检索对照报告",
        "",
        f"- 训练窗口数：{report['train_windows']}",
        f"- 追加训练窗口数：{report.get('append_train_windows', 0)}",
        f"- 评测窗口数：{report['eval_windows']}",
        f"- 场景片段数：{report['segment_count']}",
        f"- 旁路链接数：{report['sidechannel_link_count']}",
        "",
        "## 耗时",
        "",
        f"- 训练墙钟耗时：{timing.get('learning_wall_seconds', 0.0)} s",
        f"- 评测墙钟耗时：{timing.get('evaluation_wall_seconds', 0.0)} s",
        f"- 总墙钟耗时：{timing.get('total_wall_seconds', 0.0)} s",
        f"- 场景概括耗时：avg={scene_analysis.get('avg_ms', 0.0)} ms, "
        f"p50={scene_analysis.get('p50_ms', 0.0)} ms, "
        f"p95={scene_analysis.get('p95_ms', 0.0)} ms, "
        f"max={scene_analysis.get('max_ms', 0.0)} ms",
        f"- 平均每窗口场景片段数：{window_timing.get('avg_segments_per_window', 0.0)}",
        "",
        "## LLM 用量",
        "",
    ]
    llm_usage = report.get("llm_usage", {}).get("original_db", {})
    if llm_usage.get("available"):
        total_usage = llm_usage["total"]
        lines.extend(
            [
                f"- 调用次数：{total_usage['count']}",
                f"- LLM 总耗时：{total_usage['total_time_cost_seconds']} s",
                f"- LLM 平均耗时：{total_usage['avg_time_cost_seconds']} s",
                f"- LLM p50/p95/max：{total_usage['p50_time_cost_seconds']} s / "
                f"{total_usage['p95_time_cost_seconds']} s / {total_usage['max_time_cost_seconds']} s",
                f"- Token：prompt={total_usage['prompt_tokens']}, "
                f"completion={total_usage['completion_tokens']}, total={total_usage['total_tokens']}",
                f"- 费用：{total_usage['cost_yuan']} 元",
                f"- 首字延迟：{'可用' if llm_usage.get('first_token_latency_available') else '当前记录器未落库'}",
                f"- 输出耗时：{'可用' if llm_usage.get('output_time_available') else '当前记录器未落库'}",
                "",
                "### 按 request_type",
                "",
            ]
        )
        for request_type, usage in llm_usage.get("by_request_type", {}).items():
            lines.append(
                f"- {request_type}：calls={usage['count']}, total={usage['total_time_cost_seconds']} s, "
                f"avg={usage['avg_time_cost_seconds']} s, p95={usage['p95_time_cost_seconds']} s, "
                f"cost={usage['cost_yuan']} 元, tokens={usage['total_tokens']}"
            )
        lines.append("")
    else:
        lines.extend([f"- 不可用：{llm_usage.get('reason', '未知原因')}", ""])
    lines.extend(
        [
        "## 指标",
        "",
        ]
    )
    for mode in ("original", "sidechannel"):
        stats = report["stats"][mode]
        retrieval_elapsed = stats.get("retrieval_elapsed", {})
        lines.extend(
            [
                f"### {mode}",
                "",
                f"- 命中率：{stats['hit_rate']:.2%}",
                f"- 命中片段：{stats['hit_segments']}/{stats['segment_count']}",
                f"- 平均候选数：{stats['avg_candidate_count']}",
                f"- 平均 Top1 分数：{stats['avg_top_score']}",
                f"- 平均检索耗时：{stats['avg_elapsed_ms']} ms",
                f"- 检索 p50/p95/max：{retrieval_elapsed.get('p50_ms', 0.0)} ms / "
                f"{retrieval_elapsed.get('p95_ms', 0.0)} ms / {retrieval_elapsed.get('max_ms', 0.0)} ms",
                f"- Top1 同 chat：{stats['top1_same_chat_count']}",
                f"- Top1 跨 chat：{stats['top1_cross_chat_count']}",
                "",
            ]
        )
    comparison = report["stats"]["comparison"]
    lines.extend(
        [
            "### 对照",
            "",
            f"- Top1 相同：{comparison['same_top1']}",
            f"- Top1 不同：{comparison['different_top1']}",
            f"- 原始空、旁路命中：{comparison['original_empty_side_hit']}",
            f"- 原始命中、旁路空：{comparison['original_hit_side_empty']}",
            "",
            "## 逐片段 Top1",
            "",
        ]
    )
    for item in report["segments"]:
        lines.append(f"### {item['window_index']} / {item['segment_id']} {item['title']}")
        lines.append("")
        lines.append(f"- summary：{item['profile']['summary']}")
        for mode in ("original", "sidechannel"):
            candidates = item[mode]["candidates"]
            if not candidates:
                lines.append(f"- {mode}：无命中")
                continue
            top = candidates[0]
            source = "同 chat" if top.get("same_chat") else "其他 chat"
            lines.append(
                f"- {mode} Top1：#{top.get('id')} score={top.get('score')} "
                f"cluster=#{top.get('scene_cluster_id')} {source}"
            )
            lines.append(f"  - 场景标签：{top.get('scene_tags') or ''}")
            lines.append(f"  - 行为：{top.get('action') or ''}")
        lines.append("")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")


async def run_compare(args: Namespace) -> None:
    total_start = time.perf_counter()
    args.source_db = _resolve_path(args.source_db)
    output_dir = _resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    original_db = output_dir / "original_learning.db"
    side_db = output_dir / "sidechannel_learning.db"
    original_progress = output_dir / "original_progress.jsonl"
    side_progress = output_dir / "sidechannel_progress.jsonl"

    sidechannel_events: list[SidechannelLinkEvent] = []
    learning_start = time.perf_counter()
    reused_learning = False
    if args.append_train_windows > 0:
        if not original_db.exists() or not original_progress.exists():
            raise FileNotFoundError(f"追加学习需要已有 original 库和进度文件: {output_dir}")
        if side_db.exists():
            side_engine = _build_engine(side_db)
            _initialize_target_database(side_engine)
            side_session_local = sessionmaker(autocommit=False, autoflush=False, bind=side_engine, class_=Session)
            with side_session_local() as side_session:
                sidechannel_events.extend(_read_sidechannel_link_events(side_session))
            side_engine.dispose()
        await _learn_with_sidechannel_capture(
            args,
            target_db=original_db,
            progress_jsonl=original_progress,
            sidechannel_events=sidechannel_events,
            train_windows=args.append_train_windows,
            title="original+append+capture",
        )
        link_count = _rebuild_side_db_from_original(
            original_db=original_db,
            side_db=side_db,
            original_progress=original_progress,
            side_progress=side_progress,
            sidechannel_events=sidechannel_events,
        )
        print(f"[sidechannel] 已基于追加后的 original 库重建，并写入旁路链接 {link_count} 条")
    elif args.reuse and original_db.exists() and side_db.exists() and original_progress.exists():
        print(f"[original/sidechannel] 复用已有双库: {output_dir}")
        reused_learning = True
    else:
        _remove_file(side_db)
        _remove_file(side_progress)
        _remove_file(original_db)
        _remove_file(original_progress)
        await _learn_with_sidechannel_capture(
            args,
            target_db=original_db,
            progress_jsonl=original_progress,
            sidechannel_events=sidechannel_events,
            train_windows=args.train_windows,
            title="original+capture",
        )
        link_count = _rebuild_side_db_from_original(
            original_db=original_db,
            side_db=side_db,
            original_progress=original_progress,
            side_progress=side_progress,
            sidechannel_events=sidechannel_events,
        )
        print(f"[sidechannel] 已复制 original 库，并写入训练时捕获的旁路链接 {link_count} 条")
    learning_elapsed_seconds = time.perf_counter() - learning_start

    report = await _evaluate_compare(args, original_db=original_db, side_db=side_db, progress_jsonl=side_progress)
    report["timing"]["learning_wall_seconds"] = round(learning_elapsed_seconds, 3)
    report["timing"]["learning_reused"] = reused_learning
    report["timing"]["total_wall_seconds"] = round(time.perf_counter() - total_start, 3)
    report["llm_usage"] = {
        "original_db": _llm_usage_summary(original_db),
        "sidechannel_db": _llm_usage_summary(side_db),
    }
    json_output = output_dir / "need_trait_sidechannel_compare.json"
    md_output = output_dir / "need_trait_sidechannel_compare.md"
    json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(report, md_output)
    print(f"\n报告已写入: {md_output}")
    print(f"JSON 已写入: {json_output}")


def parse_args() -> Namespace:
    parser = ArgumentParser(description="对照评测原始行为检索与 need/other_trait 旁路检索。")
    parser.add_argument("--source-db", default="data/MaiBot.db")
    parser.add_argument("--output-dir", default="data/behaviro_learn_test/need_trait_sidechannel_compare")
    parser.add_argument("--train-windows", type=int, default=50)
    parser.add_argument("--append-train-windows", type=int, default=0, help="在已有 output-dir 基础上追加学习的窗口数。")
    parser.add_argument("--eval-windows", type=int, default=20)
    parser.add_argument("--window-size", type=int, default=40)
    parser.add_argument("--max-count", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260611)
    parser.add_argument("--model-name", default="")
    parser.add_argument("--chat-id", action="append", default=[])
    parser.add_argument("--session-id", action="append", default=[])
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--min-text-length", type=int, default=1)
    parser.add_argument("--min-messages-for-extraction", type=int, default=10)
    parser.add_argument("--reuse", action="store_true", help="复用 output-dir 中已有双库和进度。")
    parser.add_argument(
        "--copy-original-to-sidechannel",
        action="store_true",
        help="兼容旧参数；当前默认就是单次学习后复制 original 并写入训练时捕获的旁路链接。",
    )
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="replace")
    asyncio.run(run_compare(parse_args()))


if __name__ == "__main__":
    main()
