"""v29 schema 升级到 v30：调整 LLM 使用记录归属字段。"""

from sqlalchemy.engine import Connection

from src.common.logger import get_logger

from .models import MigrationExecutionContext
from .schema import SQLiteSchemaInspector

logger = get_logger("database_migration")


def migrate_v29_to_v30(context: MigrationExecutionContext) -> None:
    """重建 ``llm_usage``，移除 endpoint/user_type 并新增 session_id。"""

    existing_records = _count_existing_llm_usage(context.connection)
    context.start_progress(
        total_tables=1,
        total_records=existing_records,
        description="v29 -> v30 迁移进度",
        table_unit_name="表",
        record_unit_name="记录",
    )

    _replace_llm_usage_with_v30_table(context.connection)
    context.advance_progress(records=existing_records, completed_tables=1, item_name="llm_usage")
    logger.info("v29 -> v30 数据库迁移完成：llm_usage 已改为记录 session_id 并移除 endpoint/user_type")


def _count_existing_llm_usage(connection: Connection) -> int:
    schema_inspector = SQLiteSchemaInspector()
    if not schema_inspector.table_exists(connection, "llm_usage"):
        return 0
    row = connection.exec_driver_sql("SELECT COUNT(*) FROM llm_usage").fetchone()
    return int(row[0] or 0) if row is not None else 0


def _replace_llm_usage_with_v30_table(connection: Connection) -> None:
    schema_inspector = SQLiteSchemaInspector()
    if not schema_inspector.table_exists(connection, "llm_usage"):
        _create_llm_usage_v30_table(connection, "llm_usage")
        _create_llm_usage_v30_indexes(connection)
        return

    connection.exec_driver_sql("ALTER TABLE llm_usage RENAME TO llm_usage_v29_backup")
    _create_llm_usage_v30_table(connection, "llm_usage")
    connection.exec_driver_sql(
        """
        INSERT INTO llm_usage (
            id,
            model_name,
            model_assign_name,
            model_api_provider_name,
            session_id,
            task_name,
            request_type,
            time_cost,
            timestamp,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            prompt_cache_enabled,
            prompt_cache_hit_tokens,
            prompt_cache_miss_tokens,
            cost
        )
        SELECT
            id,
            model_name,
            model_assign_name,
            model_api_provider_name,
            '',
            task_name,
            request_type,
            COALESCE(time_cost, 0),
            timestamp,
            COALESCE(prompt_tokens, 0),
            COALESCE(completion_tokens, 0),
            COALESCE(total_tokens, 0),
            COALESCE(prompt_cache_enabled, 0),
            COALESCE(prompt_cache_hit_tokens, 0),
            COALESCE(prompt_cache_miss_tokens, 0),
            COALESCE(cost, 0)
        FROM llm_usage_v29_backup
        """
    )
    connection.exec_driver_sql("DROP TABLE llm_usage_v29_backup")
    _create_llm_usage_v30_indexes(connection)


def _create_llm_usage_v30_table(connection: Connection, table_name: str) -> None:
    connection.exec_driver_sql(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER NOT NULL,
            model_name VARCHAR(255) NOT NULL,
            model_assign_name VARCHAR(255),
            model_api_provider_name VARCHAR(255) NOT NULL,
            session_id VARCHAR(255) NOT NULL DEFAULT '',
            task_name VARCHAR(100),
            request_type VARCHAR(50) NOT NULL,
            time_cost FLOAT NOT NULL DEFAULT 0,
            timestamp DATETIME,
            prompt_tokens INTEGER NOT NULL,
            completion_tokens INTEGER NOT NULL,
            total_tokens INTEGER NOT NULL,
            prompt_cache_enabled BOOLEAN NOT NULL DEFAULT 0,
            prompt_cache_hit_tokens INTEGER NOT NULL DEFAULT 0,
            prompt_cache_miss_tokens INTEGER NOT NULL DEFAULT 0,
            cost FLOAT NOT NULL,
            PRIMARY KEY (id)
        )
        """
    )


def _create_llm_usage_v30_indexes(connection: Connection) -> None:
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_llm_usage_model_name ON llm_usage (model_name)")
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_llm_usage_model_assign_name ON llm_usage (model_assign_name)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_llm_usage_model_api_provider_name ON llm_usage (model_api_provider_name)"
    )
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_llm_usage_session_id ON llm_usage (session_id)")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_llm_usage_task_name ON llm_usage (task_name)")
    connection.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_llm_usage_timestamp ON llm_usage (timestamp)")
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_llm_usage_timestamp_request_type ON llm_usage(timestamp, request_type)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_llm_usage_timestamp_model_name ON llm_usage(timestamp, model_name)"
    )
    connection.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_llm_usage_timestamp_model_assign_name "
        "ON llm_usage(timestamp, model_assign_name)"
    )
