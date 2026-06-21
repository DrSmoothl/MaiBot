from itertools import chain, repeat

from sqlmodel import create_engine

from src.common.database.migrations.models import MigrationExecutionContext
import src.common.database.migrations.v30_to_v31 as migration


def test_v30_to_v31_randomly_cleans_low_domain_scene_clusters(monkeypatch) -> None:
    random_values = chain([0.5, 0.9], repeat(0.99))
    monkeypatch.setattr(migration.random, "random", lambda: next(random_values))

    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE behavior_scene_tag_clusters (
                id INTEGER PRIMARY KEY,
                tag_kind TEXT NOT NULL,
                tag TEXT NOT NULL,
                cluster_key TEXT NOT NULL,
                source_count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE behavior_scene_clusters (
                id INTEGER PRIMARY KEY,
                session_id TEXT,
                tag_distribution TEXT NOT NULL DEFAULT '[]',
                source_count INTEGER NOT NULL DEFAULT 0,
                update_time TEXT
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE behavior_experience_paths (
                id INTEGER PRIMARY KEY,
                session_id TEXT,
                scene_cluster_id INTEGER NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE behavior_offline_import_records (
                id INTEGER PRIMARY KEY,
                target_scene_cluster_id INTEGER,
                source_scene_cluster_id INTEGER
            )
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO behavior_scene_tag_clusters (id, tag_kind, tag, cluster_key, source_count, update_time)
            VALUES
                (1, 'domain', '插件开发', 'tc_1', 1, '2026-06-21'),
                (2, 'domain', '接口调试', 'tc_2', 1, '2026-06-21'),
                (3, 'domain', '权限配置', 'tc_3', 1, '2026-06-21'),
                (4, 'domain', '日志分析', 'tc_4', 1, '2026-06-21')
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO behavior_scene_clusters (id, session_id, tag_distribution, source_count, update_time)
            VALUES
                (10, 'session-a', '[{"tag":"domain:tc_1","probability":1.0}]', 2, '2026-06-21'),
                (
                    11,
                    'session-a',
                    '[{"tag":"domain:tc_1","probability":0.5},{"tag":"domain:tc_2","probability":0.5}]',
                    2,
                    '2026-06-21'
                ),
                (
                    12,
                    'session-a',
                    '[{"tag":"domain:tc_1","probability":0.34},{"tag":"domain:tc_2","probability":0.33},{"tag":"domain:tc_3","probability":0.33}]',
                    2,
                    '2026-06-21'
                ),
                (
                    13,
                    'session-a',
                    '[{"tag":"domain:tc_1","probability":0.25},{"tag":"domain:tc_2","probability":0.25},{"tag":"domain:tc_3","probability":0.25},{"tag":"domain:tc_4","probability":0.25}]',
                    2,
                    '2026-06-21'
                )
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO behavior_experience_paths (id, session_id, scene_cluster_id)
            VALUES
                (100, 'session-a', 10),
                (101, 'session-a', 11),
                (102, 'session-a', 12),
                (103, 'session-a', 13)
            """
        )
        connection.exec_driver_sql(
            """
            INSERT INTO behavior_offline_import_records (id, target_scene_cluster_id, source_scene_cluster_id)
            VALUES
                (200, 10, NULL),
                (201, NULL, 11),
                (202, 12, NULL)
            """
        )

        migration.migrate_v30_to_v31(
            MigrationExecutionContext(
                connection=connection,
                current_version=30,
                target_version=31,
                step_index=1,
                step_name="v30_to_v31",
                total_steps=1,
            )
        )

        remaining_scene_ids = connection.exec_driver_sql(
            "SELECT id FROM behavior_scene_clusters ORDER BY id"
        ).mappings().all()
        remaining_path_ids = connection.exec_driver_sql(
            "SELECT id FROM behavior_experience_paths ORDER BY id"
        ).mappings().all()
        remaining_import_ids = connection.exec_driver_sql(
            "SELECT id FROM behavior_offline_import_records ORDER BY id"
        ).mappings().all()

    assert [row["id"] for row in remaining_scene_ids] == [12, 13]
    assert [row["id"] for row in remaining_path_ids] == [102, 103]
    assert [row["id"] for row in remaining_import_ids] == [202]
