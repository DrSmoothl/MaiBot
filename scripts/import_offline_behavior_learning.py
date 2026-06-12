from argparse import ArgumentParser, Namespace
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import hashlib
import json
import shutil
import sqlite3
import sys
import uuid


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SCENE_CLUSTER_REUSE_THRESHOLD = 0.72
EVIDENCE_HISTORY_LIMIT = 20
FEEDBACK_HISTORY_LIMIT = 30
MIN_BEHAVIOR_SCORE = -6.0
MAX_BEHAVIOR_SCORE = 8.0
IMPORT_RECORD_TABLE = "behavior_offline_import_records"


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
    else:
        connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clamp_score(score: float) -> float:
    return min(MAX_BEHAVIOR_SCORE, max(MIN_BEHAVIOR_SCORE, score))


def _load_json_list(raw_value: Any) -> list[Any]:
    if isinstance(raw_value, list):
        return raw_value
    if not isinstance(raw_value, str) or not raw_value.strip():
        return []
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def _dump_json_list(items: Iterable[Any]) -> str:
    return json.dumps(list(items), ensure_ascii=False)


def _load_distribution(raw_value: Any) -> list[dict[str, Any]]:
    return [item for item in _load_json_list(raw_value) if isinstance(item, dict)]


def _distribution_mapping(distribution: list[dict[str, Any]]) -> dict[str, float]:
    tag_probs: dict[str, float] = {}
    for item in distribution:
        tag = str(item.get("tag") or "").strip()
        if not tag or ":" not in tag:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability <= 0:
            continue
        tag_probs[tag] = tag_probs.get(tag, 0.0) + probability
    total = sum(tag_probs.values())
    if total <= 0:
        return {}
    return {tag: probability / total for tag, probability in tag_probs.items()}


def _mapping_to_distribution(tag_probs: dict[str, float]) -> str:
    total = sum(max(probability, 0.0) for probability in tag_probs.values())
    if total <= 0:
        return "[]"
    distribution = [
        {
            "tag": tag,
            "probability": round(max(probability, 0.0) / total, 6),
        }
        for tag, probability in sorted(tag_probs.items())
        if probability > 0
    ]
    return json.dumps(distribution, ensure_ascii=False, sort_keys=True)


def _distribution_overlap(left_raw: Any, right_raw: Any) -> float:
    left = _distribution_mapping(_load_distribution(left_raw))
    right = _distribution_mapping(_load_distribution(right_raw))
    if not left or not right:
        return 0.0
    shared_tags = set(left) & set(right)
    return round(sum(min(left[tag], right[tag]) for tag in shared_tags), 4)


def _merge_distributions(target_raw: Any, source_raw: Any, *, target_weight: int, source_weight: int) -> str:
    target = _distribution_mapping(_load_distribution(target_raw))
    source = _distribution_mapping(_load_distribution(source_raw))
    if not target:
        return _mapping_to_distribution(source)
    if not source:
        return _mapping_to_distribution(target)
    target_weight = max(int(target_weight or 0), 1)
    source_weight = max(int(source_weight or 0), 1)
    merged: dict[str, float] = {}
    for tag in set(target) | set(source):
        merged[tag] = (
            target.get(tag, 0.0) * float(target_weight)
            + source.get(tag, 0.0) * float(source_weight)
        ) / float(target_weight + source_weight)
    return _mapping_to_distribution(merged)


def _remap_distribution(raw_value: Any, tag_key_map: dict[tuple[str, str], str]) -> str:
    remapped: dict[str, float] = {}
    for item in _load_distribution(raw_value):
        tag = str(item.get("tag") or "").strip()
        if ":" not in tag:
            continue
        tag_kind, source_key = tag.split(":", 1)
        target_key = tag_key_map.get((tag_kind, source_key), source_key)
        remapped_tag = f"{tag_kind}:{target_key}"
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability > 0:
            remapped[remapped_tag] = remapped.get(remapped_tag, 0.0) + probability
    return _mapping_to_distribution(remapped)


def _json_fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _merge_json_history(target_raw: Any, source_raw: Any, *, limit: int) -> str:
    merged: list[Any] = []
    seen: set[str] = set()
    for item in [*_load_json_list(target_raw), *_load_json_list(source_raw)]:
        fingerprint = _json_fingerprint(item)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        merged.append(item)
    return _dump_json_list(merged[-limit:])


def _min_text(left: Optional[str], right: Optional[str]) -> str:
    values = [value for value in [left, right] if value]
    return min(values) if values else _now_text()


def _max_text(left: Optional[str], right: Optional[str]) -> str:
    values = [value for value in [left, right] if value]
    return max(values) if values else _now_text()


def _create_import_record_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {IMPORT_RECORD_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_path_id INTEGER NOT NULL,
            target_path_id INTEGER NOT NULL,
            source_scene_cluster_id INTEGER NOT NULL,
            target_scene_cluster_id INTEGER NOT NULL,
            imported_at TEXT NOT NULL,
            UNIQUE(source_db, source_path_id)
        )
        """
    )


def _load_imported_source_path_ids(connection: sqlite3.Connection, *, source_db_key: str) -> set[int]:
    _create_import_record_table(connection)
    rows = connection.execute(
        f"SELECT source_path_id FROM {IMPORT_RECORD_TABLE} WHERE source_db = ?",
        (source_db_key,),
    ).fetchall()
    return {int(row["source_path_id"]) for row in rows}


def _select_source_paths(
    source_connection: sqlite3.Connection,
    *,
    imported_source_path_ids: set[int],
    include_disabled: bool,
    limit: int,
) -> list[sqlite3.Row]:
    conditions = []
    params: list[Any] = []
    if not include_disabled:
        conditions.append("enabled = 1")
    if imported_source_path_ids:
        placeholders = ",".join("?" for _ in imported_source_path_ids)
        conditions.append(f"id NOT IN ({placeholders})")
        params.extend(sorted(imported_source_path_ids))
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = "LIMIT ?" if limit > 0 else ""
    if limit > 0:
        params.append(limit)
    return source_connection.execute(
        f"""
        SELECT *
        FROM behavior_experience_paths
        {where_clause}
        ORDER BY id ASC
        {limit_clause}
        """,
        params,
    ).fetchall()


def _fetch_rows_by_ids(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    ids: set[int],
) -> dict[int, sqlite3.Row]:
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"SELECT * FROM {table_name} WHERE id IN ({placeholders})",
        sorted(ids),
    ).fetchall()
    return {int(row["id"]): row for row in rows}


def _referenced_tag_cluster_keys(scene_rows: Iterable[sqlite3.Row]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for scene in scene_rows:
        for item in _load_distribution(scene["tag_distribution"]):
            tag = str(item.get("tag") or "").strip()
            if ":" not in tag:
                continue
            tag_kind, cluster_key = tag.split(":", 1)
            if tag_kind and cluster_key:
                keys.add((tag_kind, cluster_key))
    return keys


def _choose_target_cluster_key(
    target_connection: sqlite3.Connection,
    *,
    tag_kind: str,
    source_tags: list[str],
) -> str:
    if not source_tags:
        return f"tc_{uuid.uuid4().hex}"
    placeholders = ",".join("?" for _ in source_tags)
    rows = target_connection.execute(
        f"""
        SELECT cluster_key, SUM(source_count) AS total_count, COUNT(*) AS tag_count
        FROM behavior_scene_tag_clusters
        WHERE tag_kind = ? AND tag IN ({placeholders})
        GROUP BY cluster_key
        ORDER BY tag_count DESC, total_count DESC
        """,
        [tag_kind, *source_tags],
    ).fetchall()
    if rows:
        return str(rows[0]["cluster_key"])
    return f"tc_{uuid.uuid4().hex}"


def _merge_tag_clusters(
    source_connection: sqlite3.Connection,
    target_connection: sqlite3.Connection,
    *,
    referenced_keys: set[tuple[str, str]],
    stats: Counter[str],
) -> dict[tuple[str, str], str]:
    if not referenced_keys:
        return {}

    source_rows: list[sqlite3.Row] = []
    sorted_keys = sorted(referenced_keys)
    for start_index in range(0, len(sorted_keys), 400):
        key_batch = sorted_keys[start_index : start_index + 400]
        clauses = []
        params: list[Any] = []
        for tag_kind, cluster_key in key_batch:
            clauses.append("(tag_kind = ? AND cluster_key = ?)")
            params.extend([tag_kind, cluster_key])
        source_rows.extend(
            source_connection.execute(
                f"""
                SELECT *
                FROM behavior_scene_tag_clusters
                WHERE {" OR ".join(clauses)}
                ORDER BY tag_kind ASC, cluster_key ASC, id ASC
                """,
                params,
            ).fetchall()
        )

    rows_by_source_key: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
    for row in source_rows:
        rows_by_source_key[(str(row["tag_kind"]), str(row["cluster_key"]))].append(row)

    tag_key_map: dict[tuple[str, str], str] = {}
    for source_key, rows in rows_by_source_key.items():
        tag_kind, source_cluster_key = source_key
        source_tags = [str(row["tag"]) for row in rows if row["tag"]]
        target_cluster_key = _choose_target_cluster_key(
            target_connection,
            tag_kind=tag_kind,
            source_tags=source_tags,
        )
        tag_key_map[(tag_kind, source_cluster_key)] = target_cluster_key
        for row in rows:
            existing = target_connection.execute(
                """
                SELECT *
                FROM behavior_scene_tag_clusters
                WHERE tag_kind = ? AND tag = ?
                """,
                (row["tag_kind"], row["tag"]),
            ).fetchone()
            if existing is None:
                target_connection.execute(
                    """
                    INSERT INTO behavior_scene_tag_clusters (tag_kind, tag, cluster_key, source_count, update_time)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        row["tag_kind"],
                        row["tag"],
                        target_cluster_key,
                        int(row["source_count"] or 0),
                        _max_text(row["update_time"], None),
                    ),
                )
                stats["tag_cluster_rows_inserted"] += 1
                continue
            target_connection.execute(
                """
                UPDATE behavior_scene_tag_clusters
                SET cluster_key = ?,
                    source_count = source_count + ?,
                    update_time = ?
                WHERE id = ?
                """,
                (
                    target_cluster_key,
                    int(row["source_count"] or 0),
                    _max_text(existing["update_time"], row["update_time"]),
                    existing["id"],
                ),
            )
            stats["tag_cluster_rows_updated"] += 1
            if existing["cluster_key"] != target_cluster_key:
                stats["tag_cluster_rows_rekeyed"] += 1
    return tag_key_map


def _find_existing_text_node(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    hash_column: str,
    session_id: Optional[str],
    text_hash: str,
) -> Optional[sqlite3.Row]:
    return connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE session_id IS ? AND {hash_column} = ?
        """,
        (session_id, text_hash),
    ).fetchone()


def _merge_text_nodes(
    target_connection: sqlite3.Connection,
    *,
    source_rows: dict[int, sqlite3.Row],
    table_name: str,
    text_column: str,
    hash_column: str,
    stats_prefix: str,
    stats: Counter[str],
) -> dict[int, int]:
    id_map: dict[int, int] = {}
    for source_id, row in sorted(source_rows.items()):
        existing = _find_existing_text_node(
            target_connection,
            table_name=table_name,
            hash_column=hash_column,
            session_id=row["session_id"],
            text_hash=row[hash_column],
        )
        if existing is None:
            cursor = target_connection.execute(
                f"""
                INSERT INTO {table_name}
                    (session_id, {text_column}, {hash_column}, source_count, create_time, update_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["session_id"],
                    row[text_column],
                    row[hash_column],
                    int(row["source_count"] or 0),
                    row["create_time"] or _now_text(),
                    row["update_time"] or _now_text(),
                ),
            )
            id_map[source_id] = int(cursor.lastrowid)
            stats[f"{stats_prefix}_inserted"] += 1
            continue
        target_connection.execute(
            f"""
            UPDATE {table_name}
            SET source_count = source_count + ?,
                update_time = ?
            WHERE id = ?
            """,
            (
                int(row["source_count"] or 0),
                _max_text(existing["update_time"], row["update_time"]),
                existing["id"],
            ),
        )
        id_map[source_id] = int(existing["id"])
        stats[f"{stats_prefix}_updated"] += 1
    return id_map


def _merge_scene_clusters(
    target_connection: sqlite3.Connection,
    *,
    source_scene_rows: dict[int, sqlite3.Row],
    tag_key_map: dict[tuple[str, str], str],
    stats: Counter[str],
) -> dict[int, int]:
    scene_id_map: dict[int, int] = {}
    for source_scene_id, source_scene in sorted(source_scene_rows.items()):
        remapped_distribution = _remap_distribution(source_scene["tag_distribution"], tag_key_map)
        target_candidates = target_connection.execute(
            """
            SELECT *
            FROM behavior_scene_clusters
            WHERE session_id IS ?
            """,
            (source_scene["session_id"],),
        ).fetchall()
        best_candidate: Optional[sqlite3.Row] = None
        best_overlap = 0.0
        for candidate in target_candidates:
            overlap = _distribution_overlap(candidate["tag_distribution"], remapped_distribution)
            if overlap > best_overlap:
                best_overlap = overlap
                best_candidate = candidate

        if best_candidate is not None and best_overlap >= SCENE_CLUSTER_REUSE_THRESHOLD:
            merged_source_count = int(best_candidate["source_count"] or 0) + int(source_scene["source_count"] or 0)
            merged_distribution = _merge_distributions(
                best_candidate["tag_distribution"],
                remapped_distribution,
                target_weight=int(best_candidate["source_count"] or 0),
                source_weight=int(source_scene["source_count"] or 0),
            )
            target_score = float(best_candidate["score"] or 0.0)
            source_score = float(source_scene["score"] or 0.0)
            target_weight = max(int(best_candidate["source_count"] or 0), 1)
            source_weight = max(int(source_scene["source_count"] or 0), 1)
            merged_score = (target_score * target_weight + source_score * source_weight) / (
                target_weight + source_weight
            )
            target_connection.execute(
                """
                UPDATE behavior_scene_clusters
                SET tag_distribution = ?,
                    source_count = ?,
                    score = ?,
                    update_time = ?
                WHERE id = ?
                """,
                (
                    merged_distribution,
                    merged_source_count,
                    merged_score,
                    _max_text(best_candidate["update_time"], source_scene["update_time"]),
                    best_candidate["id"],
                ),
            )
            scene_id_map[source_scene_id] = int(best_candidate["id"])
            stats["scene_clusters_updated"] += 1
            continue

        cursor = target_connection.execute(
            """
            INSERT INTO behavior_scene_clusters (session_id, tag_distribution, source_count, score, update_time)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_scene["session_id"],
                remapped_distribution,
                int(source_scene["source_count"] or 0),
                float(source_scene["score"] or 0.0),
                source_scene["update_time"] or _now_text(),
            ),
        )
        scene_id_map[source_scene_id] = int(cursor.lastrowid)
        stats["scene_clusters_inserted"] += 1
    return scene_id_map


def _find_existing_path(
    connection: sqlite3.Connection,
    *,
    source_path: sqlite3.Row,
    scene_cluster_id: int,
    action_id: int,
    outcome_id: int,
) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM behavior_experience_paths
        WHERE session_id IS ?
          AND scene_cluster_id = ?
          AND action_id = ?
          AND outcome_id = ?
          AND actor_type = ?
          AND learning_type = ?
        """,
        (
            source_path["session_id"],
            scene_cluster_id,
            action_id,
            outcome_id,
            source_path["actor_type"],
            source_path["learning_type"],
        ),
    ).fetchone()


def _merge_paths(
    target_connection: sqlite3.Connection,
    *,
    source_paths: list[sqlite3.Row],
    scene_id_map: dict[int, int],
    action_id_map: dict[int, int],
    outcome_id_map: dict[int, int],
    source_db_key: str,
    stats: Counter[str],
) -> None:
    imported_at = _now_text()
    for source_path in source_paths:
        source_path_id = int(source_path["id"])
        scene_cluster_id = scene_id_map[int(source_path["scene_cluster_id"])]
        action_id = action_id_map[int(source_path["action_id"])]
        outcome_id = outcome_id_map[int(source_path["outcome_id"])]
        existing = _find_existing_path(
            target_connection,
            source_path=source_path,
            scene_cluster_id=scene_cluster_id,
            action_id=action_id,
            outcome_id=outcome_id,
        )
        if existing is None:
            cursor = target_connection.execute(
                """
                INSERT INTO behavior_experience_paths (
                    session_id, scene_cluster_id, action_id, outcome_id,
                    actor_type, learning_type, evidence_list, feedback_list,
                    count, activation_count, success_count, failure_count,
                    score, enabled, last_active_time, last_feedback_time,
                    create_time, update_time
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_path["session_id"],
                    scene_cluster_id,
                    action_id,
                    outcome_id,
                    source_path["actor_type"],
                    source_path["learning_type"],
                    source_path["evidence_list"] or "[]",
                    source_path["feedback_list"] or "[]",
                    int(source_path["count"] or 0),
                    int(source_path["activation_count"] or 0),
                    int(source_path["success_count"] or 0),
                    int(source_path["failure_count"] or 0),
                    float(source_path["score"] or 0.0),
                    int(source_path["enabled"] or 0),
                    source_path["last_active_time"],
                    source_path["last_feedback_time"],
                    source_path["create_time"] or _now_text(),
                    source_path["update_time"] or _now_text(),
                ),
            )
            target_path_id = int(cursor.lastrowid)
            stats["paths_inserted"] += 1
        else:
            target_path_id = int(existing["id"])
            target_connection.execute(
                """
                UPDATE behavior_experience_paths
                SET evidence_list = ?,
                    feedback_list = ?,
                    count = count + ?,
                    activation_count = activation_count + ?,
                    success_count = success_count + ?,
                    failure_count = failure_count + ?,
                    score = ?,
                    enabled = ?,
                    last_active_time = ?,
                    last_feedback_time = ?,
                    create_time = ?,
                    update_time = ?
                WHERE id = ?
                """,
                (
                    _merge_json_history(existing["evidence_list"], source_path["evidence_list"], limit=EVIDENCE_HISTORY_LIMIT),
                    _merge_json_history(existing["feedback_list"], source_path["feedback_list"], limit=FEEDBACK_HISTORY_LIMIT),
                    int(source_path["count"] or 0),
                    int(source_path["activation_count"] or 0),
                    int(source_path["success_count"] or 0),
                    int(source_path["failure_count"] or 0),
                    _clamp_score(float(existing["score"] or 0.0) + float(source_path["score"] or 0.0)),
                    1 if bool(existing["enabled"]) or bool(source_path["enabled"]) else 0,
                    _max_text(existing["last_active_time"], source_path["last_active_time"]),
                    _max_text(existing["last_feedback_time"], source_path["last_feedback_time"]),
                    _min_text(existing["create_time"], source_path["create_time"]),
                    _max_text(existing["update_time"], source_path["update_time"]),
                    target_path_id,
                ),
            )
            stats["paths_updated"] += 1

        target_connection.execute(
            f"""
            INSERT OR IGNORE INTO {IMPORT_RECORD_TABLE} (
                source_db, source_path_id, target_path_id,
                source_scene_cluster_id, target_scene_cluster_id, imported_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                source_db_key,
                source_path_id,
                target_path_id,
                int(source_path["scene_cluster_id"]),
                scene_cluster_id,
                imported_at,
            ),
        )
        stats["paths_recorded"] += 1


def _database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = [
        "behavior_scene_tag_clusters",
        "behavior_scene_clusters",
        "behavior_actions",
        "behavior_outcomes",
        "behavior_experience_paths",
    ]
    return {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }


def import_offline_behavior(args: Namespace) -> dict[str, Any]:
    source_db = _resolve_path(args.source_db)
    target_db = _resolve_path(args.target_db)
    if not source_db.exists():
        raise FileNotFoundError(f"源离线学习库不存在: {source_db}")
    if not target_db.exists():
        raise FileNotFoundError(f"目标主库不存在: {target_db}")

    source_db_key = str(source_db.resolve())
    backup_path = ""
    if args.execute and args.backup:
        backup_path = str(target_db.with_suffix(f".before_behavior_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
        shutil.copy2(target_db, backup_path)

    stats: Counter[str] = Counter()
    with _connect(source_db, readonly=True) as source_connection, _connect(target_db) as target_connection:
        target_connection.execute("BEGIN")
        try:
            before_counts = _database_counts(target_connection)
            imported_source_path_ids = _load_imported_source_path_ids(
                target_connection,
                source_db_key=source_db_key,
            )
            source_paths = _select_source_paths(
                source_connection,
                imported_source_path_ids=imported_source_path_ids,
                include_disabled=args.include_disabled,
                limit=args.limit,
            )
            stats["source_paths_selected"] = len(source_paths)
            if source_paths:
                scene_ids = {int(row["scene_cluster_id"]) for row in source_paths}
                action_ids = {int(row["action_id"]) for row in source_paths}
                outcome_ids = {int(row["outcome_id"]) for row in source_paths}
                source_scenes = _fetch_rows_by_ids(
                    source_connection,
                    table_name="behavior_scene_clusters",
                    ids=scene_ids,
                )
                source_actions = _fetch_rows_by_ids(
                    source_connection,
                    table_name="behavior_actions",
                    ids=action_ids,
                )
                source_outcomes = _fetch_rows_by_ids(
                    source_connection,
                    table_name="behavior_outcomes",
                    ids=outcome_ids,
                )
                referenced_keys = _referenced_tag_cluster_keys(source_scenes.values())
                tag_key_map = _merge_tag_clusters(
                    source_connection,
                    target_connection,
                    referenced_keys=referenced_keys,
                    stats=stats,
                )
                scene_id_map = _merge_scene_clusters(
                    target_connection,
                    source_scene_rows=source_scenes,
                    tag_key_map=tag_key_map,
                    stats=stats,
                )
                action_id_map = _merge_text_nodes(
                    target_connection,
                    source_rows=source_actions,
                    table_name="behavior_actions",
                    text_column="action",
                    hash_column="action_hash",
                    stats_prefix="actions",
                    stats=stats,
                )
                outcome_id_map = _merge_text_nodes(
                    target_connection,
                    source_rows=source_outcomes,
                    table_name="behavior_outcomes",
                    text_column="outcome",
                    hash_column="outcome_hash",
                    stats_prefix="outcomes",
                    stats=stats,
                )
                _merge_paths(
                    target_connection,
                    source_paths=source_paths,
                    scene_id_map=scene_id_map,
                    action_id_map=action_id_map,
                    outcome_id_map=outcome_id_map,
                    source_db_key=source_db_key,
                    stats=stats,
                )
            after_counts = _database_counts(target_connection)
            if args.execute:
                target_connection.commit()
            else:
                target_connection.rollback()
        except Exception:
            target_connection.rollback()
            raise

    return {
        "generated_at": _now_text(),
        "executed": bool(args.execute),
        "source_db": str(source_db),
        "target_db": str(target_db),
        "backup_path": backup_path,
        "include_disabled": bool(args.include_disabled),
        "limit": int(args.limit or 0),
        "before_counts": before_counts,
        "after_counts": after_counts,
        "stats": dict(stats),
    }


def parse_args() -> Namespace:
    parser = ArgumentParser(description="将离线行为学习库合并入主库，并重映射自增 ID。")
    parser.add_argument("--source-db", default="data/behaviro_learn_test/offline_behavior_learning.db")
    parser.add_argument("--target-db", default="data/MaiBot.db")
    parser.add_argument("--limit", type=int, default=0, help="最多迁移多少条尚未导入的 path，0 表示不限制。")
    parser.add_argument("--include-disabled", action="store_true", help="同时迁移 disabled 的行为路径。")
    parser.add_argument("--execute", action="store_true", help="真正写入目标主库；不传时只 dry-run 并回滚。")
    parser.add_argument("--no-backup", dest="backup", action="store_false", help="执行写入前不备份目标主库。")
    parser.set_defaults(backup=True)
    parser.add_argument(
        "--json-output",
        default="data/behaviro_learn_test/offline_behavior_import_report.json",
    )
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="replace")
    args = parse_args()
    report = import_offline_behavior(args)
    output_path = _resolve_path(args.json_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    mode = "执行写入" if args.execute else "dry-run"
    print(f"行为离线学习导入完成（{mode}）")
    print(f"JSON report: {output_path}")
    print(json.dumps(report["stats"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
