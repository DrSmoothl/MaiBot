from collections import deque
from types import SimpleNamespace

import pytest
import time

from src.maisaka import runtime as runtime_module
from src.maisaka.runtime import (
    EXTERNAL_MESSAGE_BURST_INTERVAL_SECONDS,
    IDLE_COMPENSATION_MIN_AVERAGE_INTERVAL_SECONDS,
    MaisakaHeartFlowChatting,
)


def _make_runtime() -> MaisakaHeartFlowChatting:
    """构造仅包含空窗补偿相关状态的轻量 runtime，绕过重量级初始化。"""
    runtime = object.__new__(MaisakaHeartFlowChatting)
    runtime._recent_external_message_intervals = deque()
    runtime._last_external_message_received_at = None
    runtime._last_message_received_at = 0.0
    runtime.log_prefix = "[test]"
    return runtime


def _external_message() -> SimpleNamespace:
    """构造一条来自普通用户的外部消息桩。"""
    return SimpleNamespace(
        platform="qq",
        message_info=SimpleNamespace(user_info=SimpleNamespace(user_id="user-1")),
    )


def _record_series(runtime: MaisakaHeartFlowChatting, offsets: list[float]) -> None:
    """以接近当前时间的时间戳依次喂入外部消息，避免被 30 分钟窗口裁剪。"""
    base = time.time()
    message = _external_message()
    for offset in offsets:
        runtime._record_external_message_interval(message, base + offset)


def test_burst_intervals_excluded_from_interval_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    """连发（短间隔）消息不应计入平均间隔统计。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    # 0s/2s/10s/12s 收到消息：2s、2s 属连发抖动应被忽略，仅 8s 间隔计入
    _record_series(runtime, [0.0, 2.0, 10.0, 12.0])
    recorded = [interval for _, interval in runtime._recent_external_message_intervals]
    assert len(recorded) == 1
    assert recorded == pytest.approx([8.0])


def test_burst_boundary_interval_is_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    """恰好等于 burst 阈值（5.0s）的间隔属边界外（严格 <），应被记录。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    message = _external_message()
    # 用精确时间戳构造恰好等于阈值的间隔，避免浮点误差落到阈值另一侧
    runtime._record_external_message_interval(message, 1000.0)
    runtime._record_external_message_interval(message, 1000.0 + EXTERNAL_MESSAGE_BURST_INTERVAL_SECONDS)
    recorded = [interval for _, interval in runtime._recent_external_message_intervals]
    assert recorded == pytest.approx([EXTERNAL_MESSAGE_BURST_INTERVAL_SECONDS])


def test_average_interval_applies_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    """统计值低于下限时，平均间隔应被抬升到下限值。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    # 仅一个 8s 间隔，低于下限
    _record_series(runtime, [0.0, 8.0])
    assert runtime._get_recent_average_external_message_interval() == IDLE_COMPENSATION_MIN_AVERAGE_INTERVAL_SECONDS


def test_average_interval_preserves_large_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """统计值高于下限时，平均间隔应保持原值。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    # 两个 60s 间隔，均高于下限
    _record_series(runtime, [0.0, 60.0, 120.0])
    assert runtime._get_recent_average_external_message_interval() == pytest.approx(60.0)


def test_average_interval_none_without_any_external_message() -> None:
    """从未见过外部消息时，平均间隔应返回 None（不启用空窗补偿）。"""
    runtime = _make_runtime()
    assert runtime._get_recent_average_external_message_interval() is None


def test_average_interval_falls_back_when_only_burst_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    """只收到连发（无有效间隔样本）时，应回退到下限值而非 None，避免补偿与延迟调度失效。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    # 全部间隔 2s，均被 burst 过滤跳过，样本队列为空
    _record_series(runtime, [0.0, 2.0, 4.0])
    assert not runtime._recent_external_message_intervals
    assert runtime._get_recent_average_external_message_interval() == IDLE_COMPENSATION_MIN_AVERAGE_INTERVAL_SECONDS


def test_idle_compensation_recovers_after_burst_only_traffic(monkeypatch: pytest.MonkeyPatch) -> None:
    """启动后只收到一阵连发时，空窗补偿仍应在足够空窗后触发，待处理消息不得永久挂起。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    # 一阵连发（间隔均 < 5s、无样本入队）后陷入长沉默
    base = time.time() - 100000.0
    message = _external_message()
    for offset in [0.0, 2.0, 4.0]:
        runtime._record_external_message_interval(message, base + offset)
    assert runtime._should_trigger_message_turn_by_idle_compensation(pending_count=1, trigger_threshold=10) is True


def test_idle_compensation_blocks_without_pending_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """没有待处理消息时，纯空窗不应触发回复。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    _record_series(runtime, [0.0, 60.0, 120.0])
    runtime._last_external_message_received_at = time.time() - 100000.0
    assert runtime._should_trigger_message_turn_by_idle_compensation(pending_count=0, trigger_threshold=10) is False


def test_idle_compensation_caps_silence_contribution(monkeypatch: pytest.MonkeyPatch) -> None:
    """空窗折算量封顶，需配合真实待处理消息才可能触发。"""
    monkeypatch.setattr(runtime_module, "is_bot_self", lambda *_: False)
    runtime = _make_runtime()
    _record_series(runtime, [0.0, 60.0, 120.0])  # 平均间隔 60s

    # 空窗较短：折算量不足，单条待处理消息不触发
    runtime._last_external_message_received_at = time.time() - 60.0
    assert runtime._should_trigger_message_turn_by_idle_compensation(pending_count=1, trigger_threshold=10) is False

    # 空窗极久：折算量封顶到 threshold-1，与单条消息合计恰好达阈值才触发
    runtime._last_external_message_received_at = time.time() - 100000.0
    assert runtime._should_trigger_message_turn_by_idle_compensation(pending_count=1, trigger_threshold=10) is True
