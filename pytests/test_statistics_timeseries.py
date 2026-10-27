from contextlib import contextmanager
from datetime import datetime, timedelta

from sqlalchemy import create_engine, event
from sqlmodel import Session

import pytest

from src.chat.utils.statistics_timeseries import collect_time_series
from src.services import statistics_service
from src.services.statistics_service import StatisticsMessageRow


class SinglePass:
    def __init__(self, rows):
        self.rows = rows
        self.used = False

    def __iter__(self):
        assert not self.used, "明细不应被重复遍历"
        self.used = True
        yield from self.rows


def usage(timestamp, cost, inputs, outputs=0, model="assigned", request_type="module.reply"):
    return {
        "timestamp": timestamp, "cost": cost, "prompt_tokens": inputs, "completion_tokens": outputs,
        "model_assign_name": model, "model_name": "raw", "request_type": request_type,
    }


def test_series_preserves_boundaries_received_time_and_reply_semantics():
    now = datetime(2026, 10, 27, 12)
    records = SinglePass([
        usage(now - timedelta(days=30), 2, 20),
        usage(now - timedelta(hours=6), 1, 100, 20),
        usage(now - timedelta(minutes=10), 3, 30, model=None, request_type=None),
        usage(now, 0.5, None, 5),
        usage(now + timedelta(minutes=10), 7, 70),
    ])
    messages = SinglePass([
        StatisticsMessageRow(now - timedelta(hours=6), "qq", "human", "g", None, "Human"),
        StatisticsMessageRow(now - timedelta(days=90), "qq", "human", "g", None, "Human",
                             now - timedelta(minutes=10)),
        StatisticsMessageRow(now, "qq", "bot", None, None, None),
        StatisticsMessageRow(now, "qq", "", None, None, None),
        StatisticsMessageRow(now + timedelta(minutes=10), "qq", "human", None, None, "Human"),
    ])
    online = SinglePass([(now - timedelta(minutes=30), now + timedelta(minutes=30))])
    charts, metrics = collect_time_series(now, now, records, messages, online, lambda platform, user: user == "bot")

    six_hours = charts["6h"]
    assert len(six_hours["time_labels"]) == 37
    assert six_hours["total_cost_data"][0] == 1
    assert six_hours["total_cost_data"][-2:] == [3, 0.5]
    assert sum(six_hours["total_cost_data"]) == 4.5  # 末桶之后的记录不计入。
    assert six_hours["cost_by_model"]["raw"][-2] == 3
    assert six_hours["cost_by_module"]["unknown"][-2] == 3
    assert six_hours["message_by_chat"]["群g"][0] == 1
    assert six_hours["message_by_chat"]["群g"][-2] == 1
    assert six_hours["message_by_chat"]["用户bot"][-1] == 1
    assert charts["24h"]["total_cost_data"][-1] == 7.5  # 15 分钟末桶仍包含 +10 分钟记录。

    day = metrics["24h"]
    assert day["cost_per_100_messages"][-2:] == [300, 250]
    assert day["cost_per_100_replies"][-2:] == [0, 750]
    assert day["cost_per_hour"][-2:] == [6, 15]
    assert day["tokens_per_hour"][-2:] == [60, 150]
    assert metrics["30d"]["cost_per_hour"][-2] == 8
    assert metrics["30d"]["tokens_per_hour"][-2] == 300
    assert all(source.used for source in (records, messages, online))


def test_empty_series_and_independent_chart_clock():
    now = datetime(2026, 10, 27, 12)
    charts, metrics = collect_time_series(now, now, iter(()), iter(()), iter(()), lambda *_: False)
    assert {key: len(value["time_labels"]) for key, value in charts.items()} == {
        "6h": 37, "12h": 49, "24h": 97, "48h": 97,
    }
    assert {key: len(value["time_labels"]) for key, value in metrics.items()} == {"24h": 25, "7d": 8, "30d": 31}
    assert all(value == 0 for series in metrics.values() for value in series["cost_per_hour"])
    charts, _ = collect_time_series(
        now, now + timedelta(minutes=5), [usage(now - timedelta(hours=6), 1, 1)], [], [], lambda *_: False,
    )
    assert sum(charts["6h"]["total_cost_data"]) == 0


def test_message_iterator_uses_one_query_and_preserves_received_time(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'messages.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE mai_messages (timestamp DATETIME, received_at DATETIME, platform TEXT, "
            "user_id TEXT, group_id TEXT, group_name TEXT, user_nickname TEXT)"
        )
        connection.exec_driver_sql(
            "INSERT INTO mai_messages VALUES (?, ?, 'qq', 'u', NULL, NULL, 'User')",
            [(f"2026-10-27 12:00:0{i}.000000", None) for i in range(5)]
            + [("2026-10-26 12:00:00.000000", "2026-10-27 12:00:05.000001"),
               ("2026-10-28 12:00:00.000000", "2026-10-26 12:00:00.000000")],
        )

    @contextmanager
    def session_factory(auto_commit=False):
        with Session(engine) as session:
            yield session

    monkeypatch.setattr(statistics_service, "get_db_session", session_factory)
    queries = []
    event.listen(engine, "before_cursor_execute", lambda conn, cur, sql, params, context, many: queries.append(sql))
    rows = list(statistics_service.iter_messages_since(datetime(2026, 10, 27, 12), batch_size=2))
    assert len(rows) == 6
    assert rows[-1].statistics_time == datetime(2026, 10, 27, 12, 0, 5, 1)
    assert len(queries) == 1
    with pytest.raises(ValueError, match="batch_size"):
        list(statistics_service.iter_messages_since(datetime(2026, 10, 27), batch_size=0))
    engine.dispose()
