from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import asyncio

import pytest
from sqlalchemy import create_engine, inspect
from sqlmodel import Session

from src.common.database.database_model import ModelTaskUsage
from src.common.database.migrations.builtin import build_default_migration_registry
from src.common.database.migrations.manager import DatabaseMigrationManager
from src.common.database.migrations.v43_to_v44 import migrate_v43_to_v44
from src.services import model_task_usage_service as service


@pytest.fixture
def usage_store(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'usage.db'}")
    with engine.begin() as connection:
        migrate_v43_to_v44(SimpleNamespace(connection=connection))

    @contextmanager
    def session_factory(auto_commit=True):
        with Session(engine) as session:
            yield session
            if auto_commit:
                session.commit()

    monkeypatch.setattr(service, 'get_db_session', session_factory)
    yield engine
    engine.dispose()


def test_migration_is_repeatable_and_matches_model(usage_store):
    with usage_store.begin() as connection:
        migrate_v43_to_v44(SimpleNamespace(connection=connection))
    columns = {column['name'] for column in inspect(usage_store).get_columns('model_task_usage')}
    assert columns == set(ModelTaskUsage.__table__.columns.keys())


def test_registered_upgrade_preserves_existing_data(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'upgrade.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql('CREATE TABLE existing_data (id INTEGER PRIMARY KEY, value TEXT)')
        connection.exec_driver_sql("INSERT INTO existing_data VALUES (1, 'keep')")
        connection.exec_driver_sql('PRAGMA user_version=43')
    manager = DatabaseMigrationManager(engine, registry=build_default_migration_registry())
    manager.migrate(target_version=44)
    with engine.connect() as connection:
        assert connection.exec_driver_sql('PRAGMA user_version').scalar_one() == 44
        assert connection.exec_driver_sql('SELECT value FROM existing_data').scalar_one() == 'keep'
    assert manager.migrate(target_version=44).is_empty()
    engine.dispose()


def test_usage_groups_by_task_model_provider_and_counts_failures(usage_store):
    now = datetime.now()
    for task, model, provider, duration, success in [
        ('replyer', 'shared', 'a', 2.0, True),
        ('replyer', 'shared', 'a', 4.0, True),
        ('replyer', 'shared', 'a', 30.0, False),
        ('replyer', 'shared', 'b', 8.0, True),
        ('planner', 'shared', 'a', 10.0, True),
        ('replyer', 'failed-only', 'a', 20.0, False),
    ]:
        service.record_model_task_usage(task, model, model, provider, duration, success, now)
    service.record_model_task_usage('replyer', 'old', 'old', 'a', 1.0, True, now - timedelta(hours=25))
    result = service.get_model_task_usage('replyer')
    assert result['request_count'] == 5
    rows = {(row['model_name'], row['provider_name']): row for row in result['models']}
    assert rows['shared', 'a']['avg_duration'] == 3.0
    assert rows['shared', 'a']['failure_rate'] == pytest.approx(1 / 3)
    assert rows['shared', 'b']['avg_duration'] == 8.0
    assert rows['failed-only', 'a']['avg_duration'] is None
    assert rows['failed-only', 'a']['failure_rate'] == 1
    assert service.get_model_task_usage('unused')['models'] == []


@pytest.mark.asyncio
async def test_tracker_records_success_failure_and_timeout(usage_store):
    async with service.track_model_task_usage('utils', 'model', 'id', 'provider'):
        pass
    with pytest.raises(ValueError, match='provider error'):
        async with service.track_model_task_usage('utils', 'model', 'id', 'provider'):
            raise ValueError('provider error')
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.01):
            async with service.track_model_task_usage('utils', 'model', 'id', 'provider'):
                await asyncio.sleep(10)
    result = service.get_model_task_usage('utils')
    assert result['request_count'] == 3
    assert result['models'][0]['failure_count'] == 2
