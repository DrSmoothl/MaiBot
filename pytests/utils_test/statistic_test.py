"""统计模块数据库会话行为测试。"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from types import ModuleType
from typing import Any, Callable, Iterator

import sys

import pytest

from src.chat.utils import statistic


class _DummyResult:
    """模拟 SQLModel 查询结果对象。"""

    def all(self) -> list[Any]:
        """返回空结果集。

        Returns:
            list[Any]: 空列表。
        """
        return []


class _DummySession:
    """模拟数据库 Session。"""

    def exec(self, statement: Any) -> _DummyResult:
        """执行查询语句并返回空结果。

        Args:
            statement: 待执行的查询语句。

        Returns:
            _DummyResult: 空结果对象。
        """
        del statement
        return _DummyResult()


def _build_fake_get_db_session(calls: list[bool]) -> Callable[[bool], Iterator[_DummySession]]:
    """构造一个记录 auto_commit 参数的假会话工厂。

    Args:
        calls: 用于记录每次调用 auto_commit 参数的列表。

    Returns:
        Callable[[bool], Iterator[_DummySession]]: 可替换 `get_db_session` 的上下文管理器工厂。
    """

    @contextmanager
    def _fake_get_db_session(auto_commit: bool = True) -> Iterator[_DummySession]:
        """记录会话参数并返回假 Session。

        Args:
            auto_commit: 是否启用自动提交。

        Yields:
            Iterator[_DummySession]: 假 Session 对象。
        """
        calls.append(auto_commit)
        yield _DummySession()

    return _fake_get_db_session


def _build_statistic_task() -> statistic.StatisticOutputTask:
    """构造一个最小可用的统计任务实例。

    Returns:
        statistic.StatisticOutputTask: 跳过 `__init__` 的测试实例。
    """
    task = statistic.StatisticOutputTask.__new__(statistic.StatisticOutputTask)
    task.name_mapping = {}
    return task


def _is_bot_self(platform: str, user_id: str) -> bool:
    """返回固定的非机器人身份判断结果。

    Args:
        platform: 平台名称。
        user_id: 用户 ID。

    Returns:
        bool: 始终返回 ``False``。
    """
    del platform
    del user_id
    return False


def test_statistic_read_queries_disable_auto_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    """统计模块的纯读查询应关闭自动提交，避免 Session 退出后对象被 expire。"""
    calls: list[bool] = []
    now = datetime.now()
    task = _build_statistic_task()

    monkeypatch.setattr(statistic, "get_db_session", _build_fake_get_db_session(calls))

    utils_module = ModuleType("src.chat.utils.utils")
    utils_module.is_bot_self = _is_bot_self
    monkeypatch.setitem(sys.modules, "src.chat.utils.utils", utils_module)
    monkeypatch.setattr(statistic, "fetch_online_time_since", lambda query_start_time: [])
    monkeypatch.setattr(statistic, "fetch_model_usage_since", lambda query_start_time: [])
    monkeypatch.setattr(statistic, "fetch_messages_since", lambda query_start_time: [])
    monkeypatch.setattr(statistic, "count_tool_records_since", lambda query_start_time, tool_name: 0)

    task._collect_message_count_for_period([("last_hour", now - timedelta(hours=1))])
    task._collect_interval_data(now, hours=1, interval_minutes=60)
    task._collect_metrics_interval_data(now, hours=1, interval_hours=1)

    assert calls == []


def test_model_request_cache_rate_ignores_disabled_model_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """模型未开启 cache 时，其 prompt token 不进入缓存命中率分母。"""

    now = datetime.now()
    records = [
        {
            "timestamp": now,
            "request_type": "chat.reply",
            "model_api_provider_name": "provider",
            "model_assign_name": "cache-enabled",
            "model_name": "gpt-a",
            "prompt_tokens": 10,
            "completion_tokens": 2,
            "prompt_cache_enabled": True,
            "prompt_cache_hit_tokens": 4,
            "prompt_cache_miss_tokens": 6,
            "cost": 0.01,
            "time_cost": 1.0,
            "session_id": "g_validation",
        },
        {
            "timestamp": now,
            "request_type": "chat.reply",
            "model_api_provider_name": "provider",
            "model_assign_name": "cache-disabled",
            "model_name": "gpt-b",
            "prompt_tokens": 10,
            "completion_tokens": 2,
            "prompt_cache_enabled": False,
            "prompt_cache_hit_tokens": 0,
            "prompt_cache_miss_tokens": 10,
            "cost": 0.01,
            "time_cost": 1.0,
            "session_id": "",
        },
    ]
    monkeypatch.setattr(statistic, "fetch_model_usage_since", lambda query_start_time: records)

    stats = statistic.StatisticOutputTask._collect_model_request_for_period([("last_hour", now - timedelta(hours=1))])
    period_stats = stats["last_hour"]

    assert period_stats[statistic.TOTAL_REQ_CNT] == 2
    assert period_stats[statistic.IN_TOK_BY_MODEL]["cache-disabled"] == 10
    assert period_stats[statistic.CACHE_HIT_TOK] == 4
    assert period_stats[statistic.CACHE_MISS_TOK] == 6
    assert period_stats[statistic.CACHE_MISS_TOK_BY_MODEL]["cache-disabled"] == 0
    assert period_stats[statistic.COST_BY_CHAT]["g_validation"] == 0.01
    assert period_stats[statistic.COST_BY_CHAT][statistic.GLOBAL_COST_SESSION_KEY] == 0.01
    assert "" not in period_stats[statistic.COST_BY_CHAT]


def test_html_report_encodes_chat_names_in_tables_and_charts(tmp_path) -> None:
    report_path = tmp_path / "maibot_statistics.html"
    now = datetime.now()
    chat_name = '</script><span data-case="report-rendering">&'

    task = statistic.StatisticOutputTask(str(report_path))
    stats = {}
    for period_key, _duration, _label in task.stat_period:
        period_data = task._build_stat_period_data()
        period_data[statistic.MSG_CNT_BY_CHAT]["g_validation"] = 1
        period_data[statistic.COST_BY_CHAT]["g_validation"] = 0.12
        period_data[statistic.COST_BY_CHAT][statistic.GLOBAL_COST_SESSION_KEY] = 0.34
        period_data[statistic.TOTAL_MSG_CNT] = 1
        stats[period_key] = period_data
    task.name_mapping["g_validation"] = (chat_name, now.timestamp())

    task._generate_html_report(stats, now)
    generated_html = report_path.read_text(encoding="utf-8")

    assert chat_name not in generated_html
    assert "&lt;/script&gt;&lt;span data-case=&quot;report-rendering&quot;&gt;&amp;" in generated_html
    assert "\\u003c/script\\u003e\\u003cspan data-case=" in generated_html
    assert 'class="pie-chart-grid"' in generated_html
    assert 'class="pie-chart-canvas-wrap"' in generated_html
    assert 'class="pie-chart-legend"' in generated_html
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in generated_html
    assert "height: 450px;" in generated_html
    assert "max-height: 128px;" in generated_html
    assert "overflow-y: auto;" in generated_html
    assert "maintainAspectRatio: false" in generated_html
    assert "display: false" in generated_html
    assert "聊天流花费分布" in generated_html
    assert "全局" in generated_html
    assert "chatCostPieChart" in generated_html
    assert "chatCostPieLegend" in generated_html
