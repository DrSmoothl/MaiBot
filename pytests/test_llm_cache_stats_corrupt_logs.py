import json

import pytest

from src.services import llm_cache_stats


@pytest.mark.parametrize("bad_bytes", [b"\xff", b"\xe4\xb8", b"\xe4\xb8x"])
def test_corrupt_line_does_not_discard_valid_events(tmp_path, monkeypatch, caplog, bad_bytes):
    monkeypatch.setattr(llm_cache_stats, "CACHE_STATS_DIR", tmp_path)
    first = {"model_name": "中文模型", "prompt_tokens": 10}
    last = {"model_name": "正常模型", "prompt_tokens": 20}
    path = tmp_path / "usage_20261026.jsonl"
    content = (
        json.dumps(first, ensure_ascii=False).encode("utf-8") + b"\r\n"
        + b'{"model_name":"' + bad_bytes + b'"}\n'
        + b"\ninvalid json\n[]\n"
        + json.dumps(last, ensure_ascii=False).encode("utf-8") + b"\n"
        + bad_bytes  # 文件尾部的截断 UTF-8 记录也只跳过该行。
    )
    path.write_bytes(content)
    next_day = {"model_name": "次日模型", "prompt_tokens": 30}
    (tmp_path / "usage_20261027.jsonl").write_text(json.dumps(next_day), encoding="utf-8")

    assert llm_cache_stats._read_usage_events() == [first, last, next_day]
    assert f"{path.name}:2" in caplog.text
    assert f"{path.name}:7" in caplog.text
    assert path.read_bytes() == content


def test_corrupt_history_does_not_stop_either_html_report(tmp_path, monkeypatch):
    monkeypatch.setattr(llm_cache_stats, "CACHE_STATS_DIR", tmp_path)
    event = {
        "run_id": "test-run", "timestamp": "2026-10-27T12:00:00", "task_name": "replyer",
        "request_type": "maisaka.replyer", "model_name": "test-model", "prompt_tokens": 100,
        "prompt_cache_hit_tokens": 50, "prompt_cache_miss_tokens": 50,
    }
    (tmp_path / "usage_20261027.jsonl").write_bytes(
        b'\xff\n' + json.dumps(event).encode("utf-8") + b"\n"
    )

    llm_cache_stats._write_html_report([])

    for filename in (llm_cache_stats.REPORT_FILE_NAME, llm_cache_stats.SESSION_REPORT_FILE_NAME):
        html = (tmp_path / filename).read_text(encoding="utf-8")
        assert "test-run" in html
        assert "</html>" in html


@pytest.mark.parametrize("initial_events", [[], [{"run_id": "initial-run", "prompt_tokens": 100}]])
def test_reports_share_one_history_snapshot(tmp_path, monkeypatch, initial_events):
    monkeypatch.setattr(llm_cache_stats, "CACHE_STATS_DIR", tmp_path)
    path = tmp_path / "usage_20261027.jsonl"
    path.write_text("".join(json.dumps(event) + "\n" for event in initial_events), encoding="utf-8")
    read_history = llm_cache_stats._read_usage_events
    read_count = 0

    def read_then_append():
        nonlocal read_count
        read_count += 1
        events = read_history()
        # 模拟报告生成期间新增调用；两份报告都应使用读取时的同一份历史。
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps({"run_id": "late-run", "prompt_tokens": 999}) + "\n")
        return events

    monkeypatch.setattr(llm_cache_stats, "_read_usage_events", read_then_append)
    llm_cache_stats._write_html_report([])

    assert read_count == 1
    for filename in (llm_cache_stats.REPORT_FILE_NAME, llm_cache_stats.SESSION_REPORT_FILE_NAME):
        html = (tmp_path / filename).read_text(encoding="utf-8")
        assert "</html>" in html
        assert "late-run" not in html
        assert ("initial-run" in html) == bool(initial_events)
