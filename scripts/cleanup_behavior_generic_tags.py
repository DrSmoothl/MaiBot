from argparse import ArgumentParser, Namespace
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import json
import sqlite3
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.learners.behavior_generic_tags import is_behavior_generic_tag  # noqa: E402


DEFAULT_DB_PATH = "data/MaiBot.db"
DEFAULT_JSON_OUTPUT = "data/analysis/behavior_generic_tag_cleanup_report.json"
DEFAULT_MD_OUTPUT = "data/analysis/behavior_generic_tag_cleanup_report.md"
BATCH_SIZE = 500
LOW_SIGNAL_SCENE_MAX_SOURCE_COUNT = 1


def _load_json_list(raw_value: Any) -> list[Any]:
    if isinstance(raw_value, list):
        return raw_value
    if not isinstance(raw_value, str) or not raw_value.strip():
        return []
    try:
        parsed_value = json.loads(raw_value)
    except (TypeError, ValueError):
        return []
    return parsed_value if isinstance(parsed_value, list) else []


def _split_tag_ref(raw_value: Any) -> tuple[str, str] | None:
    tag_ref = str(raw_value or "").strip()
    if ":" not in tag_ref:
        return None
    tag_kind, cluster_key = tag_ref.split(":", 1)
    tag_kind = tag_kind.strip()
    cluster_key = cluster_key.strip()
    if not tag_kind or not cluster_key:
        return None
    return tag_kind, cluster_key


def _normalize_distribution(items: Sequence[dict[str, Any]]) -> str:
    weighted_items: list[tuple[str, float]] = []
    for item in items:
        tag_ref = str(item.get("tag") or "").strip()
        if not tag_ref:
            continue
        try:
            probability = float(item.get("probability") or 0.0)
        except (TypeError, ValueError):
            continue
        if probability <= 0:
            continue
        weighted_items.append((tag_ref, probability))

    total_probability = sum(probability for _, probability in weighted_items)
    if total_probability <= 0:
        return "[]"
    normalized_items = [
        {"tag": tag_ref, "probability": round(probability / total_probability, 6)}
        for tag_ref, probability in sorted(weighted_items)
    ]
    return json.dumps(normalized_items, ensure_ascii=False, sort_keys=True)


def _distribution_refs(items: Sequence[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for item in items:
        tag_ref = str(item.get("tag") or "").strip()
        if tag_ref:
            refs.append(tag_ref)
    return refs


def _low_signal_scene_delete_reason(items: Sequence[dict[str, Any]], source_count: int) -> str:
    domain_ref_count = 0
    other_ref_count = 0
    for item in items:
        split_ref = _split_tag_ref(item.get("tag"))
        if split_ref is None:
            continue
        tag_kind, _ = split_ref
        if tag_kind == "domain":
            domain_ref_count += 1
        else:
            other_ref_count += 1

    if domain_ref_count == 0:
        return "no_domain_tag"
    if (
        domain_ref_count == 1
        and other_ref_count == 0
        and source_count <= LOW_SIGNAL_SCENE_MAX_SOURCE_COUNT
    ):
        return "single_domain_single_source"
    return ""


def _build_scene_delete_plan(
    row: sqlite3.Row,
    *,
    delete_reason: str,
    removed_refs: Sequence[dict[str, str]],
    remaining_refs: Sequence[str],
) -> dict[str, Any]:
    return {
        "scene_cluster_id": int(row["id"]),
        "session_id": row["session_id"],
        "source_count": int(row["source_count"] or 0),
        "delete_reason": delete_reason,
        "removed_refs": list(removed_refs),
        "remaining_refs": list(remaining_refs),
    }


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    return any(row["name"] == column_name for row in connection.execute(f"PRAGMA table_info({table_name})"))


def _iter_batches(values: Sequence[int], *, batch_size: int = BATCH_SIZE) -> Iterable[list[int]]:
    for index in range(0, len(values), batch_size):
        yield list(values[index : index + batch_size])


def _delete_by_ids(connection: sqlite3.Connection, table_name: str, column_name: str, ids: Sequence[int]) -> int:
    deleted_count = 0
    for batch in _iter_batches(list(ids)):
        placeholders = ",".join("?" for _ in batch)
        cursor = connection.execute(
            f"DELETE FROM {table_name} WHERE {column_name} IN ({placeholders})",
            batch,
        )
        deleted_count += int(cursor.rowcount or 0)
    return deleted_count


def _delete_paths_by_scene_ids(connection: sqlite3.Connection, scene_cluster_ids: Sequence[int]) -> int:
    if not scene_cluster_ids:
        return 0
    deleted_count = 0
    for batch in _iter_batches(list(scene_cluster_ids)):
        placeholders = ",".join("?" for _ in batch)
        cursor = connection.execute(
            f"DELETE FROM behavior_experience_paths WHERE scene_cluster_id IN ({placeholders})",
            batch,
        )
        deleted_count += int(cursor.rowcount or 0)
    return deleted_count


def _delete_import_records_by_scene_ids(connection: sqlite3.Connection, scene_cluster_ids: Sequence[int]) -> int:
    if not scene_cluster_ids or not _table_exists(connection, "behavior_offline_import_records"):
        return 0

    columns = []
    for column_name in ("target_scene_cluster_id", "source_scene_cluster_id"):
        if _column_exists(connection, "behavior_offline_import_records", column_name):
            columns.append(column_name)
    if not columns:
        return 0

    deleted_count = 0
    for batch in _iter_batches(list(scene_cluster_ids)):
        placeholders = ",".join("?" for _ in batch)
        where_clause = " OR ".join(f"{column_name} IN ({placeholders})" for column_name in columns)
        params = []
        for _ in columns:
            params.extend(batch)
        cursor = connection.execute(
            f"DELETE FROM behavior_offline_import_records WHERE {where_clause}",
            params,
        )
        deleted_count += int(cursor.rowcount or 0)
    return deleted_count


def _load_tag_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _table_exists(connection, "behavior_scene_tag_clusters"):
        return []
    return list(
        connection.execute(
            """
            SELECT id, tag_kind, tag, cluster_key, source_count
            FROM behavior_scene_tag_clusters
            ORDER BY id
            """
        )
    )


def _load_scene_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _table_exists(connection, "behavior_scene_clusters"):
        return []
    return list(
        connection.execute(
            """
            SELECT id, session_id, tag_distribution, source_count
            FROM behavior_scene_clusters
            ORDER BY id
            """
        )
    )


def build_cleanup_plan(connection: sqlite3.Connection) -> dict[str, Any]:
    tag_rows = _load_tag_rows(connection)
    scene_rows = _load_scene_rows(connection)

    rows_by_key: dict[tuple[str, str], list[sqlite3.Row]] = defaultdict(list)
    generic_tag_rows: list[sqlite3.Row] = []
    for row in tag_rows:
        key = (str(row["tag_kind"] or ""), str(row["cluster_key"] or ""))
        if key[0] and key[1]:
            rows_by_key[key].append(row)
        if is_behavior_generic_tag(row["tag_kind"], row["tag"]):
            generic_tag_rows.append(row)

    generic_tag_row_ids = {int(row["id"]) for row in generic_tag_rows}
    remaining_keys: set[tuple[str, str]] = set()
    emptied_keys: set[tuple[str, str]] = set()
    for key, rows in rows_by_key.items():
        remaining_rows = [row for row in rows if int(row["id"]) not in generic_tag_row_ids]
        if remaining_rows:
            remaining_keys.add(key)
        elif any(int(row["id"]) in generic_tag_row_ids for row in rows):
            emptied_keys.add(key)

    scene_updates: list[dict[str, Any]] = []
    scene_deletes: list[dict[str, Any]] = []
    removed_ref_reason_counts: Counter[str] = Counter()
    scene_delete_reason_counts: Counter[str] = Counter()
    referenced_keys: set[tuple[str, str]] = set()

    for row in scene_rows:
        raw_items = _load_json_list(row["tag_distribution"])
        kept_items: list[dict[str, Any]] = []
        removed_refs: list[dict[str, str]] = []
        scene_changed = not raw_items
        for item in raw_items:
            if not isinstance(item, dict):
                removed_ref_reason_counts["invalid_item"] += 1
                scene_changed = True
                continue
            split_ref = _split_tag_ref(item.get("tag"))
            if split_ref is None:
                removed_ref_reason_counts["invalid_ref"] += 1
                removed_refs.append({"tag": str(item.get("tag") or ""), "reason": "invalid_ref"})
                scene_changed = True
                continue
            referenced_keys.add(split_ref)
            if split_ref in emptied_keys:
                removed_ref_reason_counts["emptied_generic_tag_cluster"] += 1
                removed_refs.append({"tag": str(item.get("tag") or ""), "reason": "emptied_generic_tag_cluster"})
                scene_changed = True
                continue
            if split_ref not in remaining_keys:
                removed_ref_reason_counts["dangling_tag_cluster"] += 1
                removed_refs.append({"tag": str(item.get("tag") or ""), "reason": "dangling_tag_cluster"})
                scene_changed = True
                continue
            kept_items.append(item)

        new_distribution = _normalize_distribution(kept_items)
        old_distribution = str(row["tag_distribution"] or "[]")
        normalized_items = [item for item in _load_json_list(new_distribution) if isinstance(item, dict)]
        source_count = int(row["source_count"] or 0)
        if new_distribution == "[]" and scene_changed:
            delete_reason = "empty_after_ref_cleanup" if removed_refs else "empty_distribution"
            scene_delete_reason_counts[delete_reason] += 1
            scene_deletes.append(
                _build_scene_delete_plan(
                    row,
                    delete_reason=delete_reason,
                    removed_refs=removed_refs,
                    remaining_refs=[],
                )
            )
        elif delete_reason := _low_signal_scene_delete_reason(normalized_items, source_count):
            scene_delete_reason_counts[delete_reason] += 1
            scene_deletes.append(
                _build_scene_delete_plan(
                    row,
                    delete_reason=delete_reason,
                    removed_refs=removed_refs,
                    remaining_refs=_distribution_refs(normalized_items),
                )
            )
        elif scene_changed and new_distribution != old_distribution:
            scene_updates.append(
                {
                    "scene_cluster_id": int(row["id"]),
                    "old_distribution": old_distribution,
                    "new_distribution": new_distribution,
                    "removed_refs": removed_refs,
                }
            )

    deleted_scene_ids = [plan["scene_cluster_id"] for plan in scene_deletes]
    deleted_path_count = 0
    low_signal_deleted_scene_ids = [
        plan["scene_cluster_id"]
        for plan in scene_deletes
        if plan["delete_reason"] in {"empty_distribution", "no_domain_tag", "single_domain_single_source"}
    ]
    low_signal_deleted_scene_id_set = set(low_signal_deleted_scene_ids)
    low_signal_deleted_path_count = 0
    if deleted_scene_ids and _table_exists(connection, "behavior_experience_paths"):
        for batch in _iter_batches(deleted_scene_ids):
            placeholders = ",".join("?" for _ in batch)
            row = connection.execute(
                f"SELECT COUNT(*) AS count FROM behavior_experience_paths WHERE scene_cluster_id IN ({placeholders})",
                batch,
            ).fetchone()
            deleted_path_count += int(row["count"] or 0)
    if low_signal_deleted_scene_ids and _table_exists(connection, "behavior_experience_paths"):
        for batch in _iter_batches(low_signal_deleted_scene_ids):
            placeholders = ",".join("?" for _ in batch)
            row = connection.execute(
                f"SELECT COUNT(*) AS count FROM behavior_experience_paths WHERE scene_cluster_id IN ({placeholders})",
                batch,
            ).fetchone()
            low_signal_deleted_path_count += int(row["count"] or 0)

    generic_rows_by_kind = Counter(str(row["tag_kind"] or "") for row in generic_tag_rows)
    generic_rows_by_cluster = Counter((str(row["tag_kind"] or ""), str(row["cluster_key"] or "")) for row in generic_tag_rows)
    top_generic_clusters = [
        {
            "kind": kind,
            "cluster_key": cluster_key,
            "generic_member_count": count,
            "generic_tags": [
                str(row["tag"])
                for row in generic_tag_rows
                if str(row["tag_kind"] or "") == kind and str(row["cluster_key"] or "") == cluster_key
            ][:16],
        }
        for (kind, cluster_key), count in generic_rows_by_cluster.most_common(30)
    ]

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tag_rows_scanned": len(tag_rows),
        "scene_clusters_scanned": len(scene_rows),
        "generic_tag_rows": len(generic_tag_rows),
        "generic_tag_rows_by_kind": dict(generic_rows_by_kind),
        "affected_tag_cluster_keys": len(generic_rows_by_cluster),
        "emptied_tag_cluster_keys": len(emptied_keys),
        "remaining_tag_cluster_keys": len(remaining_keys),
        "referenced_tag_cluster_keys": len(referenced_keys),
        "scene_clusters_to_update": len(scene_updates),
        "scene_clusters_to_delete": len(scene_deletes),
        "low_signal_scene_clusters_to_delete": len(low_signal_deleted_scene_ids),
        "behavior_paths_to_delete": deleted_path_count,
        "low_signal_behavior_paths_to_delete": low_signal_deleted_path_count,
        "removed_scene_ref_reason_counts": dict(removed_ref_reason_counts),
        "scene_delete_reason_counts": dict(scene_delete_reason_counts),
        "generic_tag_row_ids": sorted(generic_tag_row_ids),
        "scene_updates": scene_updates,
        "scene_deletes": scene_deletes,
        "low_signal_scene_deletes": [
            plan for plan in scene_deletes if plan["scene_cluster_id"] in low_signal_deleted_scene_id_set
        ],
        "top_generic_clusters": top_generic_clusters,
    }


def apply_cleanup_plan(connection: sqlite3.Connection, plan: dict[str, Any]) -> dict[str, int]:
    now = datetime.now().isoformat(timespec="seconds")
    stats = {
        "tag_rows_deleted": 0,
        "scene_clusters_updated": 0,
        "behavior_paths_deleted": 0,
        "offline_import_records_deleted": 0,
        "scene_clusters_deleted": 0,
    }

    try:
        connection.execute("BEGIN")
        stats["tag_rows_deleted"] = _delete_by_ids(
            connection,
            "behavior_scene_tag_clusters",
            "id",
            plan["generic_tag_row_ids"],
        )

        deleted_scene_ids = {int(item["scene_cluster_id"]) for item in plan["scene_deletes"]}
        for update_plan in plan["scene_updates"]:
            scene_cluster_id = int(update_plan["scene_cluster_id"])
            if scene_cluster_id in deleted_scene_ids:
                continue
            cursor = connection.execute(
                """
                UPDATE behavior_scene_clusters
                SET tag_distribution = ?,
                    update_time = ?
                WHERE id = ?
                """,
                (update_plan["new_distribution"], now, scene_cluster_id),
            )
            stats["scene_clusters_updated"] += int(cursor.rowcount or 0)

        scene_cluster_ids = sorted(deleted_scene_ids)
        stats["behavior_paths_deleted"] = _delete_paths_by_scene_ids(connection, scene_cluster_ids)
        stats["offline_import_records_deleted"] = _delete_import_records_by_scene_ids(connection, scene_cluster_ids)
        stats["scene_clusters_deleted"] = _delete_by_ids(
            connection,
            "behavior_scene_clusters",
            "id",
            scene_cluster_ids,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return stats


def _backup_database(db_path: Path) -> Path:
    backup_dir = db_path.parent / "db_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_before_behavior_generic_tag_cleanup_{datetime.now():%Y%m%d_%H%M%S}{db_path.suffix}"
    source = sqlite3.connect(db_path)
    try:
        target = sqlite3.connect(backup_path)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
    return backup_path


def _write_reports(
    plan: dict[str, Any],
    *,
    json_output: Path,
    md_output: Path,
    applied: bool,
    backup_path: str,
    apply_stats: dict[str, int],
) -> None:
    json_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)

    report = dict(plan)
    report["applied"] = applied
    report["backup_path"] = backup_path
    report["apply_stats"] = apply_stats
    slim_report = dict(report)
    slim_report["generic_tag_row_ids"] = plan["generic_tag_row_ids"][:200]
    slim_report["scene_updates"] = [
        {key: value for key, value in scene_update.items() if key != "old_distribution"}
        for scene_update in plan["scene_updates"][:200]
    ]
    slim_report["scene_deletes"] = plan["scene_deletes"][:200]
    json_output.write_text(json.dumps(slim_report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 行为泛 tag 清理报告",
        "",
        f"- applied: {applied}",
        f"- backup_path: `{backup_path}`" if backup_path else "- backup_path: 无",
        f"- tag_rows_scanned: {plan['tag_rows_scanned']}",
        f"- generic_tag_rows: {plan['generic_tag_rows']}",
        f"- affected_tag_cluster_keys: {plan['affected_tag_cluster_keys']}",
        f"- emptied_tag_cluster_keys: {plan['emptied_tag_cluster_keys']}",
        f"- scene_clusters_to_update: {plan['scene_clusters_to_update']}",
        f"- scene_clusters_to_delete: {plan['scene_clusters_to_delete']}",
        f"- low_signal_scene_clusters_to_delete: {plan['low_signal_scene_clusters_to_delete']}",
        f"- behavior_paths_to_delete: {plan['behavior_paths_to_delete']}",
        f"- low_signal_behavior_paths_to_delete: {plan['low_signal_behavior_paths_to_delete']}",
        f"- removed_scene_ref_reason_counts: `{json.dumps(plan['removed_scene_ref_reason_counts'], ensure_ascii=False)}`",
        f"- scene_delete_reason_counts: `{json.dumps(plan['scene_delete_reason_counts'], ensure_ascii=False)}`",
        f"- apply_stats: `{json.dumps(apply_stats, ensure_ascii=False)}`",
        "",
        "## 高频泛 tag 簇样例",
    ]
    for cluster in plan["top_generic_clusters"][:20]:
        lines.append("")
        lines.append(f"### {cluster['kind']}:{cluster['cluster_key']}")
        lines.append(f"- generic_member_count: {cluster['generic_member_count']}")
        lines.append(f"- generic_tags: {' / '.join(cluster['generic_tags'])}")

    lines.append("")
    lines.append("## 场景簇删除样例")
    for scene_delete in plan["scene_deletes"][:20]:
        removed_tags = [item["tag"] for item in scene_delete["removed_refs"][:8]]
        lines.append("")
        lines.append(f"### scene #{scene_delete['scene_cluster_id']}")
        lines.append(f"- source_count: {scene_delete['source_count']}")
        lines.append(f"- delete_reason: {scene_delete['delete_reason']}")
        lines.append(f"- remaining_refs: {' / '.join(scene_delete['remaining_refs'])}")
        lines.append(f"- removed_refs: {' / '.join(removed_tags)}")

    md_output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> Namespace:
    parser = ArgumentParser(description="清理行为场景学习中的泛 tag 和低信息场景簇。默认 dry-run。")
    parser.add_argument("--db", default=DEFAULT_DB_PATH)
    parser.add_argument("--json-output", default=DEFAULT_JSON_OUTPUT)
    parser.add_argument("--md-output", default=DEFAULT_MD_OUTPUT)
    parser.add_argument("--apply", action="store_true", help="实际修改目标数据库。默认只输出报告。")
    parser.add_argument("--backup", action="store_true", help="apply 前使用 SQLite backup API 备份数据库。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    backup_path = ""
    if args.apply and args.backup:
        backup_path = str(_backup_database(db_path))

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    apply_stats = {
        "tag_rows_deleted": 0,
        "scene_clusters_updated": 0,
        "behavior_paths_deleted": 0,
        "offline_import_records_deleted": 0,
        "scene_clusters_deleted": 0,
    }
    try:
        plan = build_cleanup_plan(connection)
        if args.apply:
            apply_stats = apply_cleanup_plan(connection, plan)
    finally:
        connection.close()

    _write_reports(
        plan,
        json_output=Path(args.json_output),
        md_output=Path(args.md_output),
        applied=bool(args.apply),
        backup_path=backup_path,
        apply_stats=apply_stats,
    )
    print(
        json.dumps(
            {
                "db": str(db_path),
                "applied": bool(args.apply),
                "backup_path": backup_path,
                "generic_tag_rows": plan["generic_tag_rows"],
                "emptied_tag_cluster_keys": plan["emptied_tag_cluster_keys"],
                "scene_clusters_to_update": plan["scene_clusters_to_update"],
                "scene_clusters_to_delete": plan["scene_clusters_to_delete"],
                "low_signal_scene_clusters_to_delete": plan["low_signal_scene_clusters_to_delete"],
                "behavior_paths_to_delete": plan["behavior_paths_to_delete"],
                "low_signal_behavior_paths_to_delete": plan["low_signal_behavior_paths_to_delete"],
                "scene_delete_reason_counts": plan["scene_delete_reason_counts"],
                "apply_stats": apply_stats,
                "json_output": args.json_output,
                "md_output": args.md_output,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
