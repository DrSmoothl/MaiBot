"""v27 schema 升级到 v28：新增维护任务表并移除工具 prompt 冗余列。"""

from sqlalchemy.engine import Connection

from src.common.database.tool_record_payload_cleanup import clean_tool_record_payload
from src.common.logger import get_logger

from .models import MigrationExecutionContext
from .schema import SQLiteSchemaInspector

logger = get_logger("database_migration")

_TOOL_RECORD_COPY_BATCH_SIZE = 1000
_TOOL_RECORD_CLEANUP_TASK_NAME = "tool_record_prompt_payload_cleanup_v1"
_PHASE_AWAITING_VACUUM = "awaiting_vacuum"
_PHASE_DONE = "done"
_STATUS_PENDING = "pending"
_STATUS_DONE = "done"


def migrate_v27_to_v28(context: MigrationExecutionContext) -> None:
    """新增一次性维护任务状态表，并移除 ``tool_records`` 中的 prompt 冗余列。"""

    tool_record_total = _count_tool_records_for_rebuild(context.connection)
    context.start_progress(
        total_tables=2,
        total_records=tool_record_total,
        description="v27 -> v28 迁移进度",
        table_unit_name="表",
        record_unit_name="记录",
    )

    _create_one_time_maintenance_tasks_table(context.connection)
    context.advance_progress(records=0, completed_tables=1, item_name="one_time_maintenance_tasks")
    scanned_records, changed_records, last_processed_id = _rebuild_tool_records_without_prompt_columns(context)
    if scanned_records > 0:
        _save_tool_record_cleanup_state(
            context.connection,
            scanned_records=scanned_records,
            changed_records=changed_records,
            last_processed_id=last_processed_id,
        )
    context.advance_progress(records=0, completed_tables=1, item_name="tool_records")

    logger.info("v27 -> v28 数据库迁移完成：维护任务表已就绪，tool_records prompt 冗余列已移除")


def _count_tool_records_for_rebuild(connection: Connection) -> int:
    if not _should_rebuild_tool_records(connection):
        return 0
    row = connection.exec_driver_sql("SELECT COUNT(*) FROM tool_records").fetchone()
    return int(row[0] or 0) if row is not None else 0


def _create_one_time_maintenance_tasks_table(connection: Connection) -> None:
    schema_inspector = SQLiteSchemaInspector()
    if not schema_inspector.table_exists(connection, "one_time_maintenance_tasks"):
        connection.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS one_time_maintenance_tasks (
                task_name VARCHAR(100) NOT NULL,
                phase VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                cursor_id INTEGER NOT NULL DEFAULT 0,
                stats_json TEXT NOT NULL DEFAULT '{}',
                last_error TEXT,
                completed_at DATETIME,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (task_name)
            )
            """
        )

    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS ix_one_time_maintenance_tasks_updated_at
        ON one_time_maintenance_tasks (updated_at)
        """
    )


def _should_rebuild_tool_records(connection: Connection) -> bool:
    schema_inspector = SQLiteSchemaInspector()
    if not schema_inspector.table_exists(connection, "tool_records"):
        return False

    table_schema = schema_inspector.get_table_schema(connection, "tool_records")
    if table_schema is None:
        return False
    return table_schema.has_column("tool_builtin_prompt") or table_schema.has_column("tool_display_prompt")


def _save_tool_record_cleanup_state(
    connection: Connection,
    *,
    scanned_records: int,
    changed_records: int,
    last_processed_id: int,
) -> None:
    phase = _PHASE_AWAITING_VACUUM if changed_records > 0 else _PHASE_DONE
    status = _STATUS_PENDING if changed_records > 0 else _STATUS_DONE
    connection.exec_driver_sql(
        """
        INSERT INTO one_time_maintenance_tasks (
            task_name, phase, status, cursor_id, stats_json,
            last_error, completed_at, updated_at
        )
        VALUES (
            ?, ?, ?, ?, ?,
            NULL,
            CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT(task_name) DO UPDATE SET
            phase = excluded.phase,
            status = excluded.status,
            cursor_id = excluded.cursor_id,
            stats_json = excluded.stats_json,
            last_error = NULL,
            completed_at = CASE
                WHEN ? = 1 THEN CURRENT_TIMESTAMP
                ELSE one_time_maintenance_tasks.completed_at
            END,
            updated_at = excluded.updated_at
        """,
        (
            _TOOL_RECORD_CLEANUP_TASK_NAME,
            phase,
            status,
            last_processed_id,
            f'{{"scanned_records":{scanned_records},"updated_records":{changed_records}}}',
            1 if changed_records == 0 else 0,
            1 if changed_records == 0 else 0,
        ),
    )


def _rebuild_tool_records_without_prompt_columns(context: MigrationExecutionContext) -> tuple[int, int, int]:
    connection = context.connection
    if not _should_rebuild_tool_records(connection):
        return 0, 0, 0

    logger.info("v27 -> v28 开始优化数据库 tool_records：移除冗余 prompt 列，并同步清理 tool_data 历史大字段")
    connection.exec_driver_sql("DROP TABLE IF EXISTS tool_records_v28")
    connection.exec_driver_sql(
        """
        CREATE TABLE tool_records_v28 (
            id INTEGER NOT NULL,
            tool_id VARCHAR(255) NOT NULL,
            timestamp DATETIME NOT NULL,
            session_id VARCHAR(255) NOT NULL,
            tool_name VARCHAR(255) NOT NULL,
            tool_reasoning VARCHAR,
            tool_data VARCHAR,
            PRIMARY KEY (id)
        )
        """
    )
    last_copied_id = 0
    scanned_records = 0
    changed_records = 0
    while True:
        rows = connection.exec_driver_sql(
            """
            SELECT
                id,
                tool_id,
                timestamp,
                session_id,
                tool_name,
                tool_reasoning,
                tool_data
            FROM tool_records
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (last_copied_id, _TOOL_RECORD_COPY_BATCH_SIZE),
        ).fetchall()
        if not rows:
            break

        batch_last_id = int(rows[-1][0])
        insert_rows = []
        batch_changed_records = 0
        for row in rows:
            raw_tool_data = str(row[6] or "")
            cleanup_result = clean_tool_record_payload(raw_tool_data) if raw_tool_data else None
            if cleanup_result is not None and cleanup_result.changed:
                tool_data = cleanup_result.tool_data
                batch_changed_records += 1
            else:
                tool_data = row[6]
            insert_rows.append(
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    tool_data,
                )
            )
        connection.exec_driver_sql(
            """
            INSERT INTO tool_records_v28 (
                id,
                tool_id,
                timestamp,
                session_id,
                tool_name,
                tool_reasoning,
                tool_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        copied_count = len(rows)
        scanned_records += copied_count
        changed_records += batch_changed_records
        context.advance_progress(records=copied_count, completed_tables=0, item_name="tool_records")
        last_copied_id = batch_last_id

    logger.info(
        "v27 -> v28 tool_records 数据复制与粗筛清理完成："
        f"复制 {scanned_records} 条，清理 tool_data {changed_records} 条，开始替换旧表并重建索引"
    )
    connection.exec_driver_sql("DROP TABLE tool_records")
    connection.exec_driver_sql("ALTER TABLE tool_records_v28 RENAME TO tool_records")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_tool_records_session_id ON tool_records (session_id)")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_tool_records_timestamp ON tool_records (timestamp)")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_tool_records_tool_id ON tool_records (tool_id)")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_tool_records_tool_name ON tool_records (tool_name)")
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS ix_tool_records_timestamp_tool_name
        ON tool_records(timestamp, tool_name)
        """
    )
    logger.info("v27 -> v28 tool_records 重建完成")
    return scanned_records, changed_records, last_copied_id
