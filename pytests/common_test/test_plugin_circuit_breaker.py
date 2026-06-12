from src.plugin_runtime.host import circuit_breaker
from src.plugin_runtime.host.circuit_breaker import PluginCircuitBreaker


def test_plugin_circuit_breaker_opens_and_skips(monkeypatch):
    now = 1000.0
    monkeypatch.setattr(circuit_breaker.time, "monotonic", lambda: now)

    breaker = PluginCircuitBreaker(failure_threshold=2, base_cooldown_sec=10, max_cooldown_sec=20)
    first = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(first, "timeout")

    second = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(second, "timeout")

    skipped = breaker.try_acquire("plugin.a", "handler", "hook:test")
    assert not skipped.allowed
    assert "熔断中" in skipped.reason


def test_plugin_circuit_breaker_half_open_allows_single_probe(monkeypatch):
    now_value = [1000.0]
    monkeypatch.setattr(circuit_breaker.time, "monotonic", lambda: now_value[0])

    breaker = PluginCircuitBreaker(failure_threshold=1, base_cooldown_sec=10, max_cooldown_sec=20)
    failed = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(failed, "timeout")

    now_value[0] += 10.1
    probe = breaker.try_acquire("plugin.a", "handler", "hook:test")
    parallel_probe = breaker.try_acquire("plugin.a", "handler", "hook:test")

    assert probe.allowed
    assert probe.half_open
    assert not parallel_probe.allowed
    assert "半开测试" in parallel_probe.reason


def test_plugin_circuit_breaker_half_open_success_recovers(monkeypatch):
    now_value = [1000.0]
    monkeypatch.setattr(circuit_breaker.time, "monotonic", lambda: now_value[0])

    breaker = PluginCircuitBreaker(failure_threshold=1, base_cooldown_sec=10, max_cooldown_sec=20)
    failed = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(failed, "timeout")

    now_value[0] += 10.1
    probe = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_success(probe)

    allowed = breaker.try_acquire("plugin.a", "handler", "hook:test")
    assert allowed.allowed
    assert not allowed.half_open


def test_plugin_circuit_breaker_half_open_failure_backs_off(monkeypatch):
    now_value = [1000.0]
    monkeypatch.setattr(circuit_breaker.time, "monotonic", lambda: now_value[0])

    breaker = PluginCircuitBreaker(failure_threshold=1, base_cooldown_sec=10, max_cooldown_sec=20)
    failed = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(failed, "timeout")

    now_value[0] += 10.1
    probe = breaker.try_acquire("plugin.a", "handler", "hook:test")
    breaker.record_failure(probe, "timeout")

    skipped = breaker.try_acquire("plugin.a", "handler", "hook:test")
    assert not skipped.allowed

    now_value[0] += 10.1
    still_skipped = breaker.try_acquire("plugin.a", "handler", "hook:test")
    assert not still_skipped.allowed

    now_value[0] += 10.1
    next_probe = breaker.try_acquire("plugin.a", "handler", "hook:test")
    assert next_probe.allowed
    assert next_probe.half_open
