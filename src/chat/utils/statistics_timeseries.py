"""一次遍历生成报表图表和指标趋势，不持有历史明细。"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Mapping, Tuple, cast

if TYPE_CHECKING:
    from src.services.statistics_service import StatisticsMessageRow


@dataclass
class _Window:
    start: datetime
    hours: int
    interval_minutes: int
    size: int = field(init=False)
    seconds: int = field(init=False)
    start_timestamp: float = field(init=False)

    def __post_init__(self) -> None:
        # 保留旧报表的末尾时间桶，包括恰好落在报告时间上的记录。
        self.size = self.hours * 60 // self.interval_minutes + 1
        self.seconds = self.interval_minutes * 60
        self.start_timestamp = self.start.timestamp()

    def index(self, elapsed_seconds: float) -> int:
        return int(elapsed_seconds // self.seconds)

    def labels(self, format_string: str) -> List[str]:
        return [
            (self.start + timedelta(minutes=i * self.interval_minutes)).strftime(format_string)
            for i in range(self.size)
        ]


@dataclass
class _Chart:
    window: _Window
    costs: List[float] = field(init=False)
    by_model: Dict[str, List[float]] = field(default_factory=dict)
    by_module: Dict[str, List[float]] = field(default_factory=dict)
    by_chat: Dict[str, List[int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.costs = [0.0] * self.window.size

    def result(self) -> Dict[str, object]:
        return {
            "time_labels": self.window.labels("%H:%M"),
            "total_cost_data": self.costs,
            "cost_by_model": self.by_model,
            "cost_by_module": self.by_module,
            "message_by_chat": self.by_chat,
        }


@dataclass
class _Metrics:
    window: _Window
    costs: List[float] = field(init=False)
    tokens: List[int] = field(init=False)
    messages: List[int] = field(init=False)
    replies: List[int] = field(init=False)
    online_hours: List[float] = field(init=False)

    def __post_init__(self) -> None:
        size = self.window.size
        self.costs = [0.0] * size
        self.tokens = [0] * size
        self.messages = [0] * size
        self.replies = [0] * size
        self.online_hours = [0.0] * size

    def result(self) -> Dict[str, object]:
        return {
            "time_labels": self.window.labels("%H:%M" if self.window.interval_minutes == 60 else "%m-%d"),
            "cost_per_100_messages": [
                cost / count * 100 if count > 0 else 0.0
                for cost, count in zip(self.costs, self.messages, strict=True)
            ],
            "cost_per_hour": [
                cost / hours if hours > 0 else 0.0
                for cost, hours in zip(self.costs, self.online_hours, strict=True)
            ],
            "tokens_per_hour": [
                tokens / hours if hours > 0 else 0.0
                for tokens, hours in zip(self.tokens, self.online_hours, strict=True)
            ],
            "cost_per_100_replies": [
                cost / count * 100 if count > 0 else 0.0
                for cost, count in zip(self.costs, self.replies, strict=True)
            ],
        }


def collect_time_series(
    now: datetime,
    chart_now: datetime,
    usage: Iterable[Mapping[str, object]],
    messages: Iterable["StatisticsMessageRow"],
    online_intervals: Iterable[Tuple[datetime, datetime]],
    is_bot_self: Callable[[str, str], bool],
) -> Tuple[Dict[str, Dict[str, object]], Dict[str, Dict[str, object]]]:
    """共享数据读取，保留图表和趋势各自的时间起点、分桶方式及回复口径。"""

    # 图表：6/12/24/48 小时，分别按 10/15/15/30 分钟分桶。
    charts = {
        key: _Chart(_Window(chart_now - timedelta(hours=hours), hours, minutes))
        for key, hours, minutes in (("6h", 6, 10), ("12h", 12, 15), ("24h", 24, 15), ("48h", 48, 30))
    }
    # 趋势：24 小时按小时，7 天及 30 天按天分桶。
    metrics = {
        key: _Metrics(_Window(now - timedelta(hours=hours), hours, minutes))
        for key, hours, minutes in (("24h", 24, 60), ("7d", 168, 1440), ("30d", 720, 1440))
    }

    for record in usage:
        timestamp = cast(datetime, record["timestamp"])
        cost = cast(float | None, record["cost"]) or 0.0
        tokens = (cast(int | None, record["prompt_tokens"]) or 0) + (
            cast(int | None, record["completion_tokens"]) or 0
        )
        model = cast(str | None, record["model_assign_name"]) or cast(str | None, record["model_name"]) or "unknown"
        request_type = cast(str | None, record["request_type"]) or "unknown"
        module = request_type.split(".")[0] if "." in request_type else request_type
        for chart in charts.values():
            index = chart.window.index((timestamp - chart.window.start).total_seconds())
            if 0 <= index < chart.window.size:
                chart.costs[index] += cost
                for breakdown, name in ((chart.by_model, model), (chart.by_module, module)):
                    if name not in breakdown:
                        breakdown[name] = [0.0] * chart.window.size
                    breakdown[name][index] += cost
        for series in metrics.values():
            index = series.window.index((timestamp - series.window.start).total_seconds())
            if 0 <= index < series.window.size:
                series.costs[index] += cost
                series.tokens[index] += tokens

    for message in messages:
        # 新入站按接收时间，历史和出站按消息时间；沿用消息的本地时间戳分桶方式。
        timestamp = message.statistics_time.timestamp()
        if message.group_id:
            chat_name = message.group_name or f"群{message.group_id}"
        elif message.user_id:
            chat_name = message.user_nickname or f"用户{message.user_id}"
        else:
            chat_name = None
        if chat_name:
            for chart in charts.values():
                index = chart.window.index(timestamp - chart.window.start_timestamp)
                if 0 <= index < chart.window.size:
                    if chat_name not in chart.by_chat:
                        chart.by_chat[chat_name] = [0] * chart.window.size
                    chart.by_chat[chat_name][index] += 1
        # 趋势仍按机器人发送的消息计回复数，不替换成终端统计的 reply 工具调用次数。
        reply = None
        for series in metrics.values():
            index = series.window.index(timestamp - series.window.start_timestamp)
            if 0 <= index < series.window.size:
                series.messages[index] += 1
                if reply is None:
                    reply = is_bot_self(message.platform or "", message.user_id or "")
                series.replies[index] += int(reply)

    for start, end in online_intervals:
        for series in metrics.values():
            # 在线区间按与各桶的实际重叠时长累计，保留末尾桶，不裁剪到报告时间。
            step = timedelta(minutes=series.window.interval_minutes)
            for index in range(series.window.size):
                bucket_start = series.window.start + index * step
                overlap_start = max(start, bucket_start)
                overlap_end = min(end, bucket_start + step)
                if overlap_end > overlap_start:
                    series.online_hours[index] += (overlap_end - overlap_start).total_seconds() / 3600.0

    return (
        {key: chart.result() for key, chart in charts.items()},
        {key: series.result() for key, series in metrics.items()},
    )
