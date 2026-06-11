from argparse import ArgumentParser, Namespace
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import json
import shutil
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]

EVIDENCE_HISTORY_LIMIT = 20
FEEDBACK_HISTORY_LIMIT = 30
IMPORT_RECORD_TABLE = "behavior_offline_import_records"


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=120.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 120000")
    return connection


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


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


def _json_fingerprint(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


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


def _load_distribution(raw_value: Any) -> list[dict[str, Any]]:
    return [item for item in _load_json_list(raw_value) if isinstance(item, dict)]


def _distribution_mapping(raw_value: Any) -> dict[str, float]:
    tag_probs: dict[str, float] = {}
    for item in _load_distribution(raw_value):
        tag = str(item.get("tag") or "").strip()
        if not tag:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability > 0:
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
        {"tag": tag, "probability": round(max(probability, 0.0) / total, 6)}
        for tag, probability in sorted(tag_probs.items())
        if probability > 0
    ]
    return json.dumps(distribution, ensure_ascii=False, sort_keys=True)


def _distribution_overlap(left_raw: Any, right_raw: Any) -> float:
    left = _distribution_mapping(left_raw)
    right = _distribution_mapping(right_raw)
    if not left or not right:
        return 0.0
    return round(sum(min(left[tag], right[tag]) for tag in set(left) & set(right)), 4)


def _merge_distributions(rows: list[sqlite3.Row]) -> str:
    weighted: dict[str, float] = {}
    total_weight = 0
    for row in rows:
        weight = max(int(row["source_count"] or 0), 1)
        total_weight += weight
        for tag, probability in _distribution_mapping(row["tag_distribution"]).items():
            weighted[tag] = weighted.get(tag, 0.0) + probability * weight
    if total_weight <= 0:
        return "[]"
    return _mapping_to_distribution({tag: value / total_weight for tag, value in weighted.items()})


def _scene_session_ids(raw_session_id: Any) -> set[str]:
    normalized = str(raw_session_id or "").strip()
    if not normalized:
        return set()
    if normalized.startswith("["):
        try:
            parsed = json.loads(normalized)
        except (TypeError, ValueError):
            return {normalized}
        if isinstance(parsed, list):
            return {str(item or "").strip() for item in parsed if str(item or "").strip()}
    return {normalized}


def _dump_scene_session_ids(session_ids: set[str]) -> str:
    sorted_session_ids = sorted(session_ids)
    if len(sorted_session_ids) == 1:
        return sorted_session_ids[0]
    return json.dumps(sorted_session_ids, ensure_ascii=False)


class UnionFind:
    def __init__(self, values: Iterable[int]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: int) -> int:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _load_scene_clusters(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM behavior_scene_clusters
        ORDER BY id ASC
        """
    ).fetchall()


def _split_group_by_pair_floor(
    group: list[sqlite3.Row],
    *,
    min_pair_overlap: float,
) -> list[list[sqlite3.Row]]:
    if len(group) <= 2 or min_pair_overlap <= 0:
        return [group]

    sorted_group = sorted(
        group,
        key=lambda row: (
            -int(row["source_count"] or 0),
            -float(row["score"] or 0.0),
            int(row["id"]),
        ),
    )
    subgroups: list[list[sqlite3.Row]] = []
    for row in sorted_group:
        placed = False
        for subgroup in subgroups:
            if all(
                _distribution_overlap(row["tag_distribution"], member["tag_distribution"]) >= min_pair_overlap
                for member in subgroup
            ):
                subgroup.append(row)
                placed = True
                break
        if not placed:
            subgroups.append([row])
    return [subgroup for subgroup in subgroups if len(subgroup) > 1]


def _build_merge_groups(
    rows: list[sqlite3.Row],
    *,
    threshold: float,
    min_pair_overlap: float,
    allow_same_chat: bool,
) -> list[list[sqlite3.Row]]:
    ids = [int(row["id"]) for row in rows]
    row_by_id = {int(row["id"]): row for row in rows}
    session_ids_by_id = {int(row["id"]): _scene_session_ids(row["session_id"]) for row in rows}
    union_find = UnionFind(ids)

    for left_index, left in enumerate(rows):
        left_id = int(left["id"])
        left_sessions = session_ids_by_id[left_id]
        if not left_sessions:
            continue
        for right in rows[left_index + 1 :]:
            right_id = int(right["id"])
            right_sessions = session_ids_by_id[right_id]
            if not right_sessions:
                continue
            if not allow_same_chat and left_sessions == right_sessions:
                continue
            if not allow_same_chat and len(left_sessions | right_sessions) <= 1:
                continue
            overlap = _distribution_overlap(left["tag_distribution"], right["tag_distribution"])
            if overlap >= threshold:
                union_find.union(left_id, right_id)

    groups_by_root: dict[int, list[sqlite3.Row]] = {}
    for row_id, row in row_by_id.items():
        groups_by_root.setdefault(union_find.find(row_id), []).append(row)
    merge_groups: list[list[sqlite3.Row]] = []
    for group in groups_by_root.values():
        if len(group) <= 1:
            continue
        if not allow_same_chat and len(set().union(*[_scene_session_ids(row["session_id"]) for row in group])) <= 1:
            continue
        merge_groups.extend(_split_group_by_pair_floor(group, min_pair_overlap=min_pair_overlap))
    return merge_groups


def _choose_representative(group: list[sqlite3.Row]) -> sqlite3.Row:
    return max(
        group,
        key=lambda row: (
            int(row["source_count"] or 0),
            float(row["score"] or 0.0),
            -int(row["id"]),
        ),
    )


def _max_text(left: Optional[str], right: Optional[str]) -> str:
    values = [value for value in [left, right] if value]
    return max(values) if values else _now_text()


def _min_text(left: Optional[str], right: Optional[str]) -> str:
    values = [value for value in [left, right] if value]
    return min(values) if values else _now_text()


def _find_equivalent_path(
    connection: sqlite3.Connection,
    *,
    path: sqlite3.Row,
    scene_cluster_id: int,
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
          AND id != ?
        """,
        (
            path["session_id"],
            scene_cluster_id,
            path["action_id"],
            path["outcome_id"],
            path["actor_type"],
            path["learning_type"],
            path["id"],
        ),
    ).fetchone()


def _merge_path_into_existing(
    connection: sqlite3.Connection,
    *,
    source_path: sqlite3.Row,
    target_path: sqlite3.Row,
) -> None:
    connection.execute(
        """
        UPDATE behavior_experience_paths
        SET evidence_list = ?,
            feedback_list = ?,
            count = count + ?,
            activation_count = activation_count + ?,
            success_count = success_count + ?,
            failure_count = failure_count + ?,
            score = score + ?,
            enabled = ?,
            last_active_time = ?,
            last_feedback_time = ?,
            create_time = ?,
            update_time = ?
        WHERE id = ?
        """,
        (
            _merge_json_history(target_path["evidence_list"], source_path["evidence_list"], limit=EVIDENCE_HISTORY_LIMIT),
            _merge_json_history(target_path["feedback_list"], source_path["feedback_list"], limit=FEEDBACK_HISTORY_LIMIT),
            int(source_path["count"] or 0),
            int(source_path["activation_count"] or 0),
            int(source_path["success_count"] or 0),
            int(source_path["failure_count"] or 0),
            float(source_path["score"] or 0.0),
            1 if bool(target_path["enabled"]) or bool(source_path["enabled"]) else 0,
            _max_text(target_path["last_active_time"], source_path["last_active_time"]),
            _max_text(target_path["last_feedback_time"], source_path["last_feedback_time"]),
            _min_text(target_path["create_time"], source_path["create_time"]),
            _max_text(target_path["update_time"], source_path["update_time"]),
            target_path["id"],
        ),
    )
    _update_import_records_path(connection, old_path_id=int(source_path["id"]), new_path_id=int(target_path["id"]))
    connection.execute("DELETE FROM behavior_experience_paths WHERE id = ?", (source_path["id"],))


def _update_import_records_path(connection: sqlite3.Connection, *, old_path_id: int, new_path_id: int) -> None:
    table_exists = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (IMPORT_RECORD_TABLE,),
    ).fetchone()
    if table_exists is None:
        return
    connection.execute(
        f"UPDATE {IMPORT_RECORD_TABLE} SET target_path_id = ? WHERE target_path_id = ?",
        (new_path_id, old_path_id),
    )


def _update_import_records_scene(connection: sqlite3.Connection, *, old_scene_id: int, new_scene_id: int) -> None:
    table_exists = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (IMPORT_RECORD_TABLE,),
    ).fetchone()
    if table_exists is None:
        return
    connection.execute(
        f"UPDATE {IMPORT_RECORD_TABLE} SET target_scene_cluster_id = ? WHERE target_scene_cluster_id = ?",
        (new_scene_id, old_scene_id),
    )


def _apply_group_merge(connection: sqlite3.Connection, *, group: list[sqlite3.Row], stats: Counter[str]) -> None:
    representative = _choose_representative(group)
    representative_id = int(representative["id"])
    merged_session_ids = set().union(*[_scene_session_ids(row["session_id"]) for row in group])
    total_source_count = sum(max(int(row["source_count"] or 0), 1) for row in group)
    weighted_score = sum(float(row["score"] or 0.0) * max(int(row["source_count"] or 0), 1) for row in group)
    merged_score = weighted_score / float(total_source_count) if total_source_count else 0.0
    merged_update_time = max(str(row["update_time"] or "") for row in group) or _now_text()
    merged_distribution = _merge_distributions(group)

    connection.execute(
        """
        UPDATE behavior_scene_clusters
        SET session_id = ?,
            tag_distribution = ?,
            source_count = ?,
            score = ?,
            update_time = ?
        WHERE id = ?
        """,
        (
            _dump_scene_session_ids(merged_session_ids),
            merged_distribution,
            total_source_count,
            merged_score,
            merged_update_time,
            representative_id,
        ),
    )

    loser_ids = [int(row["id"]) for row in group if int(row["id"]) != representative_id]
    for loser_id in loser_ids:
        loser_paths = connection.execute(
            "SELECT * FROM behavior_experience_paths WHERE scene_cluster_id = ? ORDER BY id ASC",
            (loser_id,),
        ).fetchall()
        for path in loser_paths:
            equivalent = _find_equivalent_path(
                connection,
                path=path,
                scene_cluster_id=representative_id,
            )
            if equivalent is not None:
                _merge_path_into_existing(connection, source_path=path, target_path=equivalent)
                stats["paths_merged_due_unique_constraint"] += 1
                continue
            connection.execute(
                "UPDATE behavior_experience_paths SET scene_cluster_id = ?, update_time = ? WHERE id = ?",
                (representative_id, _now_text(), path["id"]),
            )
            stats["paths_relinked"] += 1
        _update_import_records_scene(connection, old_scene_id=loser_id, new_scene_id=representative_id)
        connection.execute("DELETE FROM behavior_scene_clusters WHERE id = ?", (loser_id,))
        stats["scene_clusters_deleted"] += 1

    stats["scene_cluster_groups_merged"] += 1
    stats["scene_clusters_representatives_updated"] += 1
    stats["merged_chat_id_memberships"] += len(merged_session_ids)


def _database_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = [
        "behavior_scene_clusters",
        "behavior_experience_paths",
    ]
    return {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }


def merge_scene_clusters(args: Namespace) -> dict[str, Any]:
    target_db = _resolve_path(args.target_db)
    if not target_db.exists():
        raise FileNotFoundError(f"目标数据库不存在: {target_db}")

    backup_path = ""
    if args.execute and args.backup:
        backup_path = str(target_db.with_suffix(f".before_scene_cluster_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
        shutil.copy2(target_db, backup_path)

    stats: Counter[str] = Counter()
    with _connect(target_db) as connection:
        connection.execute("BEGIN IMMEDIATE" if args.execute else "BEGIN")
        try:
            before_counts = _database_counts(connection)
            scene_clusters = _load_scene_clusters(connection)
            merge_groups = _build_merge_groups(
                scene_clusters,
                threshold=args.threshold,
                min_pair_overlap=args.min_pair_overlap,
                allow_same_chat=args.allow_same_chat,
            )
            if args.limit > 0:
                merge_groups = merge_groups[: args.limit]
            stats["candidate_scene_clusters"] = len(scene_clusters)
            stats["candidate_merge_groups"] = len(merge_groups)
            stats["candidate_scene_clusters_to_merge"] = sum(len(group) for group in merge_groups)
            stats["candidate_scene_clusters_after_merge"] = len(merge_groups)
            for group in merge_groups:
                _apply_group_merge(connection, group=group, stats=stats)
            after_counts = _database_counts(connection)
            if args.execute:
                connection.commit()
            else:
                connection.rollback()
        except Exception:
            connection.rollback()
            raise

    return {
        "generated_at": _now_text(),
        "executed": bool(args.execute),
        "target_db": str(target_db),
        "backup_path": backup_path,
        "threshold": float(args.threshold),
        "min_pair_overlap": float(args.min_pair_overlap),
        "allow_same_chat": bool(args.allow_same_chat),
        "limit": int(args.limit or 0),
        "before_counts": before_counts,
        "after_counts": after_counts,
        "stats": dict(stats),
    }


def parse_args() -> Namespace:
    parser = ArgumentParser(description="跨 chat_id 合并行为 scene_cluster，并用 JSON 数组记录覆盖的 chat_id。")
    parser.add_argument("--target-db", default="data/MaiBot.db")
    parser.add_argument("--threshold", type=float, default=0.72)
    parser.add_argument(
        "--min-pair-overlap",
        type=float,
        default=0.0,
        help="合并组内任意两簇的最低 overlap；0 表示只按连通分量合并。",
    )
    parser.add_argument("--allow-same-chat", action="store_true", help="允许同一个 chat_id 内的 scene_cluster 互相合并。")
    parser.add_argument("--limit", type=int, default=0, help="最多执行多少个合并组，0 表示不限制。")
    parser.add_argument("--execute", action="store_true", help="真正写入目标库；默认 dry-run 回滚。")
    parser.add_argument("--no-backup", dest="backup", action="store_false", help="执行前不备份目标库。")
    parser.set_defaults(backup=True)
    parser.add_argument(
        "--json-output",
        default="data/behaviro_learn_test/scene_cluster_cross_chat_merge_report.json",
    )
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="replace")
    args = parse_args()
    report = merge_scene_clusters(args)
    output_path = _resolve_path(args.json_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    mode = "执行写入" if args.execute else "dry-run"
    print(f"跨 chat_id scene_cluster 合并完成（{mode}）")
    print(f"JSON report: {output_path}")
    print(json.dumps(report["stats"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
