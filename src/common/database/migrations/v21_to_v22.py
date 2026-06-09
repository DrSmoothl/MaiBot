"""v21 to v22 schema migration: remove legacy v1 backup tables."""

from __future__ import annotations

from sqlalchemy.engine import Connection

from src.common.logger import get_logger

from .models import MigrationExecutionContext

logger = get_logger("database_migration")

LEGACY_V1_BACKUP_TABLE_PREFIX = "__legacy_v1_"
LEGACY_V1_BACKUP_TABLES = (
    "__legacy_v1_action_records",
    "__legacy_v1_chat_history",
    "__legacy_v1_chat_streams",
    "__legacy_v1_emoji",
    "__legacy_v1_emoji_description_cache",
    "__legacy_v1_expression",
    "__legacy_v1_group_info",
    "__legacy_v1_image_descriptions",
    "__legacy_v1_images",
    "__legacy_v1_jargon",
    "__legacy_v1_llm_usage",
    "__legacy_v1_messages",
    "__legacy_v1_online_time",
    "__legacy_v1_person_info",
    "__legacy_v1_thinking_back",
)
LEGACY_V1_STALE_TABLES = (
    "chat_history",
    "thinking_back",
)
LEGACY_V1_CLEANUP_TABLES = LEGACY_V1_BACKUP_TABLES + LEGACY_V1_STALE_TABLES


def migrate_v21_to_v22(context: MigrationExecutionContext) -> None:
    """Drop legacy v1 backup tables and reclaim SQLite file space."""

    existing_cleanup_tables = _existing_tables(context.connection, LEGACY_V1_CLEANUP_TABLES)
    context.start_progress(
        total_tables=max(len(existing_cleanup_tables), 1) + 1,
        total_records=0,
        description="v21 -> v22 migration progress",
        table_unit_name="table",
        record_unit_name="record",
    )

    for table_name in existing_cleanup_tables:
        _drop_table(context.connection, table_name)
        context.advance_progress(records=0, completed_tables=1, item_name=table_name)

    if not existing_cleanup_tables:
        context.advance_progress(records=0, completed_tables=1, item_name="legacy_v1_cleanup")

    _vacuum_database_best_effort(context.connection)
    context.advance_progress(records=0, completed_tables=1, item_name="vacuum")

    logger.info(
        "v21 -> v22 database migration completed: dropped legacy v1 tables: "
        f"{', '.join(existing_cleanup_tables) if existing_cleanup_tables else 'none'}"
    )


def has_legacy_v1_cleanup_tables(connection: Connection) -> bool:
    """Return whether the database still contains legacy v1 cleanup targets."""

    return bool(_existing_tables(connection, LEGACY_V1_CLEANUP_TABLES))


def _existing_tables(connection: Connection, table_names: tuple[str, ...]) -> list[str]:
    rows = connection.exec_driver_sql(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ({placeholders})
        ORDER BY name
        """.format(placeholders=", ".join("?" for _ in table_names)),
        table_names,
    ).all()
    return [str(row[0]) for row in rows]


def _drop_table(connection: Connection, table_name: str) -> None:
    escaped_table_name = table_name.replace('"', '""')
    connection.exec_driver_sql(f'DROP TABLE IF EXISTS "{escaped_table_name}"')


def _vacuum_database_best_effort(connection: Connection) -> None:
    """Try to reclaim SQLite file space without blocking the migration on failure."""

    connection.commit()
    try:
        connection.exec_driver_sql("VACUUM")
    except Exception as exc:
        logger.warning(f"v21 -> v22 database VACUUM skipped after cleanup: {exc}")
