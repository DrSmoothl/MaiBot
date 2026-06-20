from contextlib import contextmanager
from types import SimpleNamespace
from typing import Iterator

from sqlmodel import Session, SQLModel, create_engine, select
import pytest

from src.common.database.database_model import ModelUsage
from src.common.database.migrations.models import MigrationExecutionContext
from src.common.database.migrations.schema import SQLiteSchemaInspector
from src.common.database.migrations.v29_to_v30 import migrate_v29_to_v30
from src.llm_models import utils as llm_utils


def test_model_usage_schema_records_session_id_without_endpoint_or_user_type() -> None:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[ModelUsage.__table__])

    with engine.connect() as connection:
        table_schema = SQLiteSchemaInspector().get_table_schema(connection, "llm_usage")

    assert table_schema.has_column("session_id")
    assert not table_schema.has_column("endpoint")
    assert not table_schema.has_column("user_type")


def test_v29_to_v30_migration_rebuilds_llm_usage_with_session_id() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE llm_usage (
                id INTEGER NOT NULL,
                model_name VARCHAR(255) NOT NULL,
                model_assign_name VARCHAR(255),
                model_api_provider_name VARCHAR(255) NOT NULL,
                endpoint VARCHAR(255),
                user_type VARCHAR(6),
                task_name VARCHAR(100),
                request_type VARCHAR(50) NOT NULL,
                time_cost FLOAT,
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
        connection.exec_driver_sql(
            """
            INSERT INTO llm_usage (
                id,
                model_name,
                model_assign_name,
                model_api_provider_name,
                endpoint,
                user_type,
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
            VALUES (
                1,
                'model-a',
                'assign-a',
                'provider-a',
                '/chat/completions',
                'SYSTEM',
                'replyer',
                'maisaka.replyer',
                1.25,
                '2026-06-21 00:00:00',
                10,
                2,
                12,
                1,
                3,
                7,
                0.01
            )
            """
        )

        migrate_v29_to_v30(
            MigrationExecutionContext(
                connection=connection,
                current_version=29,
                target_version=30,
                step_index=1,
                step_name="v29_to_v30",
                total_steps=1,
            )
        )

        table_schema = SQLiteSchemaInspector().get_table_schema(connection, "llm_usage")
        row = connection.exec_driver_sql(
            """
            SELECT
                id,
                model_name,
                session_id,
                task_name,
                request_type,
                prompt_tokens,
                prompt_cache_hit_tokens,
                cost
            FROM llm_usage
            """
        ).mappings().one()

    assert table_schema.has_column("session_id")
    assert not table_schema.has_column("endpoint")
    assert not table_schema.has_column("user_type")
    assert dict(row) == {
        "id": 1,
        "model_name": "model-a",
        "session_id": "",
        "task_name": "replyer",
        "request_type": "maisaka.replyer",
        "prompt_tokens": 10,
        "prompt_cache_hit_tokens": 3,
        "cost": 0.01,
    }


def test_llm_usage_recorder_persists_session_id(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine, tables=[ModelUsage.__table__])

    @contextmanager
    def fake_get_db_session(auto_commit: bool = True) -> Iterator[Session]:
        session = Session(engine)
        try:
            yield session
            if auto_commit:
                session.commit()
        finally:
            session.close()

    monkeypatch.setattr(llm_utils, "get_db_session", fake_get_db_session)

    model_info = SimpleNamespace(
        model_identifier="provider-model-a",
        name="model-a",
        api_provider="provider-a",
        cache=False,
        price_in=1.0,
        price_out=2.0,
        cache_price_in=0.5,
    )
    model_usage = SimpleNamespace(
        model_name="provider-model-a",
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        prompt_cache_hit_tokens=0,
        prompt_cache_miss_tokens=0,
    )

    llm_utils.LLMUsageRecorder().record_usage_to_database(
        model_info=model_info,
        model_usage=model_usage,
        user_id="system",
        request_type="maisaka.replyer",
        task_name="replyer",
        session_id="session-a",
        time_cost=1.234,
    )

    with Session(engine) as session:
        record = session.exec(select(ModelUsage)).one()

    assert record.session_id == "session-a"
    assert record.model_name == "provider-model-a"
    assert record.model_assign_name == "model-a"
    assert record.request_type == "maisaka.replyer"
