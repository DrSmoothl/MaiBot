from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, DefaultDict, Iterable, Optional

import csv
import json
import math
import re
import sqlite3
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROMPT_ROOT = PROJECT_ROOT / "logs" / "maisaka_prompt"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "MaiBot.db"
DEFAULT_STAGES = ("timing_gate", "planner")
REQUEST_TYPE_BY_STAGE = {
    "timing_gate": "maisaka.timing_gate",
    "planner": "maisaka.planner",
}
CJK_PATTERN = re.compile(r"[\u3400-\u9fff\uf900-\ufaff\u3040-\u30ff\uac00-\ud7af]")
LATIN_PATTERN = re.compile(r"[A-Za-z0-9_]+")
STEM_TIMESTAMP_PATTERN = re.compile(r"^(\d{10,})")


@dataclass
class UsageRecord:
    id: int
    timestamp: datetime
    request_type: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model_assign_name: str


@dataclass
class SectionStats:
    chars: int = 0
    estimated_tokens: int = 0
    calibrated_tokens: float = 0.0
    occurrences: int = 0


@dataclass
class PromptRecord:
    path: Path
    stage: str
    chat_dir: str
    timestamp: datetime
    section_chars: dict[str, int]
    section_estimated_tokens: dict[str, int]
    tool_definition_chars: dict[str, int]
    tool_definition_estimated_tokens: dict[str, int]
    usage: Optional[UsageRecord]
    message_count: int
    tool_count: int
    output_tool_names: list[str]

    @property
    def actual_prompt_tokens(self) -> int:
        return self.usage.prompt_tokens if self.usage is not None else 0

    @property
    def estimated_prompt_tokens(self) -> int:
        return sum(self.section_estimated_tokens.values())


def parse_datetime_filter(value: str | None) -> datetime | None:
    if value is None:
        return None

    normalized_value = value.strip()
    if not normalized_value:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized_value, fmt)
        except ValueError:
            continue
    raise ValueError(f"无法解析时间: {value!r}，请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")


def parse_recent_filter(value: str | None) -> datetime | None:
    if value is None:
        return None

    normalized_value = value.strip().lower()
    if not normalized_value:
        return None

    match = re.fullmatch(r"(\d+(?:\.\d+)?)([mhdw])", normalized_value)
    if match is None:
        raise ValueError(f"无法解析最近时间: {value!r}，请使用 30m、24h、7d 或 2w")

    amount = float(match.group(1))
    if amount <= 0:
        raise ValueError(f"最近时间必须大于 0: {value!r}")

    unit = match.group(2)
    if unit == "m":
        delta = timedelta(minutes=amount)
    elif unit == "h":
        delta = timedelta(hours=amount)
    elif unit == "d":
        delta = timedelta(days=amount)
    else:
        delta = timedelta(weeks=amount)
    return datetime.now() - delta


def parse_file_timestamp(path: Path) -> datetime:
    match = STEM_TIMESTAMP_PATTERN.match(path.stem)
    if match is None:
        return datetime.fromtimestamp(path.stat().st_mtime)

    raw_timestamp = int(match.group(1))
    if raw_timestamp > 10_000_000_000:
        return datetime.fromtimestamp(raw_timestamp / 1000)
    return datetime.fromtimestamp(raw_timestamp)


def estimate_tokens(text: str) -> int:
    """轻量估算中英混合文本 token 数，用于分段占比分析。"""

    if not text:
        return 0

    cjk_count = len(CJK_PATTERN.findall(text))
    latin_chars = sum(len(match.group(0)) for match in LATIN_PATTERN.finditer(text))
    other_chars = max(len(text) - cjk_count - latin_chars, 0)
    latin_tokens = math.ceil(latin_chars / 4)
    other_tokens = math.ceil(other_chars / 3)
    return max(1, cjk_count + latin_tokens + other_tokens)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def classify_message_section(message: dict[str, Any]) -> str:
    role = str(message.get("role") or "").strip().lower()
    content = str(message.get("content_text") or message.get("content") or "")
    stripped = content.lstrip()

    if role == "system":
        return "system"
    if role == "assistant":
        return "assistant_history"
    if role == "tool":
        return "tool_result"
    if "<system-reminder>" in content:
        return "system_reminder"
    if stripped.startswith("当前时间：") or stripped.startswith("Current time:"):
        return "time_context"
    if stripped.startswith("<message "):
        return "chat_message"
    if stripped.startswith("{") and '"success"' in stripped:
        return "tool_result"
    return "other_context"


def extract_output_tool_names(data: dict[str, Any]) -> list[str]:
    output = data.get("output")
    if not isinstance(output, dict):
        return []
    raw_tool_calls = output.get("tool_calls")
    if not isinstance(raw_tool_calls, list):
        return []
    tool_names: list[str] = []
    for tool_call in raw_tool_calls:
        if not isinstance(tool_call, dict):
            continue
        tool_name = str(tool_call.get("name") or "").strip()
        if tool_name:
            tool_names.append(tool_name)
    return tool_names


def extract_tool_definition_name(tool_definition: dict[str, Any]) -> str:
    function_definition = tool_definition.get("function")
    if isinstance(function_definition, dict):
        name = str(function_definition.get("name") or "").strip()
        if name:
            return name
    name = str(tool_definition.get("name") or "").strip()
    return name or "unknown"


def load_prompt_record(
    path: Path,
    *,
    stage: str,
    usage: Optional[UsageRecord],
) -> PromptRecord:
    data = json.loads(path.read_text(encoding="utf-8"))
    section_texts: DefaultDict[str, list[str]] = defaultdict(list)

    raw_messages = data.get("messages")
    messages = raw_messages if isinstance(raw_messages, list) else []
    for raw_message in messages:
        if not isinstance(raw_message, dict):
            continue
        section = classify_message_section(raw_message)
        content = str(raw_message.get("content_text") or raw_message.get("content") or "")
        section_texts[section].append(content)

    raw_tool_definitions = data.get("tool_definitions")
    tool_definitions = raw_tool_definitions if isinstance(raw_tool_definitions, list) else []
    tool_definition_texts: DefaultDict[str, list[str]] = defaultdict(list)
    if tool_definitions:
        section_texts["tool_definitions"].append(compact_json(tool_definitions))
        for raw_tool_definition in tool_definitions:
            if not isinstance(raw_tool_definition, dict):
                continue
            tool_name = extract_tool_definition_name(raw_tool_definition)
            tool_definition_texts[tool_name].append(compact_json(raw_tool_definition))

    section_chars = {
        section: sum(len(text) for text in texts)
        for section, texts in section_texts.items()
    }
    section_estimated_tokens = {
        section: estimate_tokens("\n".join(texts))
        for section, texts in section_texts.items()
    }
    tool_definition_chars = {
        tool_name: sum(len(text) for text in texts)
        for tool_name, texts in tool_definition_texts.items()
    }
    tool_definition_estimated_tokens = {
        tool_name: estimate_tokens("\n".join(texts))
        for tool_name, texts in tool_definition_texts.items()
    }

    return PromptRecord(
        path=path,
        stage=stage,
        chat_dir=path.parent.name,
        timestamp=parse_file_timestamp(path),
        section_chars=section_chars,
        section_estimated_tokens=section_estimated_tokens,
        tool_definition_chars=tool_definition_chars,
        tool_definition_estimated_tokens=tool_definition_estimated_tokens,
        usage=usage,
        message_count=len(messages),
        tool_count=len(tool_definitions),
        output_tool_names=extract_output_tool_names(data),
    )


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def load_usage_records(db_path: Path, since: datetime | None, until: datetime | None) -> list[UsageRecord]:
    if not db_path.exists():
        return []

    where_clauses = ["request_type IN ('maisaka.timing_gate', 'maisaka.planner')"]
    parameters: list[Any] = []
    if since is not None:
        where_clauses.append("timestamp >= ?")
        parameters.append(since.strftime("%Y-%m-%d %H:%M:%S"))
    if until is not None:
        where_clauses.append("timestamp <= ?")
        parameters.append(until.strftime("%Y-%m-%d %H:%M:%S"))

    sql = f"""
        SELECT
            id,
            timestamp,
            request_type,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            COALESCE(model_assign_name, '') AS model_assign_name
        FROM llm_usage
        WHERE {" AND ".join(where_clauses)}
        ORDER BY timestamp ASC, id ASC
    """

    usage_records: list[UsageRecord] = []
    with connect_readonly(db_path) as connection:
        for row in connection.execute(sql, parameters):
            timestamp_text = str(row["timestamp"] or "").split(".", 1)[0]
            try:
                timestamp = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            usage_records.append(
                UsageRecord(
                    id=int(row["id"]),
                    timestamp=timestamp,
                    request_type=str(row["request_type"] or ""),
                    prompt_tokens=int(row["prompt_tokens"] or 0),
                    completion_tokens=int(row["completion_tokens"] or 0),
                    total_tokens=int(row["total_tokens"] or 0),
                    model_assign_name=str(row["model_assign_name"] or ""),
                )
            )
    return usage_records


def find_matching_usage(
    *,
    usage_records: Iterable[UsageRecord],
    used_usage_ids: set[int],
    stage: str,
    timestamp: datetime,
    max_seconds: float,
) -> Optional[UsageRecord]:
    request_type = REQUEST_TYPE_BY_STAGE.get(stage, "")
    best_record: Optional[UsageRecord] = None
    best_delta = max_seconds + 1.0
    for usage in usage_records:
        if usage.id in used_usage_ids:
            continue
        if usage.request_type != request_type:
            continue
        delta = abs((usage.timestamp - timestamp).total_seconds())
        if delta <= max_seconds and delta < best_delta:
            best_record = usage
            best_delta = delta
    if best_record is not None:
        used_usage_ids.add(best_record.id)
    return best_record


def iter_prompt_files(
    root: Path,
    stages: list[str],
    *,
    chat_filter: str,
    since: datetime | None,
    until: datetime | None,
) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for stage in stages:
        stage_dir = root / stage
        if not stage_dir.exists():
            continue
        for path in stage_dir.rglob("*.json"):
            if chat_filter and chat_filter not in path.parent.name:
                continue
            timestamp = parse_file_timestamp(path)
            if since is not None and timestamp < since:
                continue
            if until is not None and timestamp > until:
                continue
            files.append((stage, path))
    files.sort(key=lambda item: parse_file_timestamp(item[1]))
    return files


def apply_limit(files: list[tuple[str, Path]], limit: int) -> list[tuple[str, Path]]:
    if limit <= 0 or len(files) <= limit:
        return files
    return files[-limit:]


def aggregate_records(records: list[PromptRecord]) -> dict[str, dict[str, SectionStats]]:
    aggregates: dict[str, dict[str, SectionStats]] = defaultdict(lambda: defaultdict(SectionStats))
    for record in records:
        estimated_total = max(record.estimated_prompt_tokens, 1)
        actual_prompt_tokens = record.actual_prompt_tokens
        stages = (record.stage, "all")
        for section, estimated_tokens in record.section_estimated_tokens.items():
            calibrated_tokens = (
                actual_prompt_tokens * estimated_tokens / estimated_total
                if actual_prompt_tokens > 0
                else 0.0
            )
            for stage in stages:
                stats = aggregates[stage][section]
                stats.chars += record.section_chars.get(section, 0)
                stats.estimated_tokens += estimated_tokens
                stats.calibrated_tokens += calibrated_tokens
                stats.occurrences += 1
    return aggregates


def get_stage_records(records: list[PromptRecord], stage: str) -> list[PromptRecord]:
    if stage == "all":
        return records
    return [record for record in records if record.stage == stage]


def variance(values: list[float]) -> float:
    if not values:
        return 0.0
    mean_value = sum(values) / len(values)
    return sum((value - mean_value) ** 2 for value in values) / len(values)


def standard_deviation(value: float) -> float:
    return math.sqrt(max(value, 0.0))


def calibrated_section_value(record: PromptRecord, section: str) -> float:
    if record.usage is None:
        return 0.0
    estimated_total = max(record.estimated_prompt_tokens, 1)
    estimated_tokens = record.section_estimated_tokens.get(section, 0)
    return record.actual_prompt_tokens * estimated_tokens / estimated_total


def calibrated_tool_value(record: PromptRecord, tool_name: str) -> float:
    if record.usage is None:
        return 0.0
    estimated_total = max(record.estimated_prompt_tokens, 1)
    estimated_tokens = record.tool_definition_estimated_tokens.get(tool_name, 0)
    return record.actual_prompt_tokens * estimated_tokens / estimated_total


def aggregate_tool_definitions(records: list[PromptRecord]) -> dict[str, dict[str, SectionStats]]:
    aggregates: dict[str, dict[str, SectionStats]] = defaultdict(lambda: defaultdict(SectionStats))
    for record in records:
        estimated_total = max(record.estimated_prompt_tokens, 1)
        actual_prompt_tokens = record.actual_prompt_tokens
        stages = (record.stage, "all")
        for tool_name, estimated_tokens in record.tool_definition_estimated_tokens.items():
            calibrated_tokens = (
                actual_prompt_tokens * estimated_tokens / estimated_total
                if actual_prompt_tokens > 0
                else 0.0
            )
            for stage in stages:
                stats = aggregates[stage][tool_name]
                stats.chars += record.tool_definition_chars.get(tool_name, 0)
                stats.estimated_tokens += estimated_tokens
                stats.calibrated_tokens += calibrated_tokens
                stats.occurrences += 1
    return aggregates


def format_int(value: int | float) -> str:
    return f"{round(value):,}"


def format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def build_section_rows(records: list[PromptRecord]) -> list[dict[str, Any]]:
    aggregates = aggregate_records(records)
    rows: list[dict[str, Any]] = []
    for stage in ("all", *DEFAULT_STAGES):
        stage_records = get_stage_records(records, stage)
        matched_stage_records = [record for record in stage_records if record.usage is not None]
        section_map = aggregates.get(stage)
        if not section_map:
            continue
        estimated_total = sum(stats.estimated_tokens for stats in section_map.values())
        calibrated_total = sum(stats.calibrated_tokens for stats in section_map.values())
        for section, stats in sorted(
            section_map.items(),
            key=lambda item: item[1].calibrated_tokens or item[1].estimated_tokens,
            reverse=True,
        ):
            estimated_variance = variance(
                [float(record.section_estimated_tokens.get(section, 0)) for record in stage_records]
            )
            calibrated_variance = variance(
                [calibrated_section_value(record, section) for record in matched_stage_records]
            )
            rows.append(
                {
                    "stage": stage,
                    "section": section,
                    "chars": stats.chars,
                    "estimated_tokens": stats.estimated_tokens,
                    "estimated_percent": stats.estimated_tokens / estimated_total if estimated_total else 0.0,
                    "estimated_mean": stats.estimated_tokens / len(stage_records) if stage_records else 0.0,
                    "estimated_variance": estimated_variance,
                    "estimated_stddev": standard_deviation(estimated_variance),
                    "calibrated_tokens": stats.calibrated_tokens,
                    "calibrated_percent": stats.calibrated_tokens / calibrated_total if calibrated_total else 0.0,
                    "calibrated_mean": stats.calibrated_tokens / len(matched_stage_records)
                    if matched_stage_records
                    else 0.0,
                    "calibrated_variance": calibrated_variance,
                    "calibrated_stddev": standard_deviation(calibrated_variance),
                    "occurrences": stats.occurrences,
                }
            )
    return rows


def build_tool_rows(records: list[PromptRecord]) -> list[dict[str, Any]]:
    aggregates = aggregate_tool_definitions(records)
    rows: list[dict[str, Any]] = []
    prompt_totals = {
        stage: sum(
            record.actual_prompt_tokens
            for record in (records if stage == "all" else [item for item in records if item.stage == stage])
            if record.usage is not None
        )
        for stage in ("all", *DEFAULT_STAGES)
    }
    for stage in ("all", *DEFAULT_STAGES):
        stage_records = get_stage_records(records, stage)
        matched_stage_records = [record for record in stage_records if record.usage is not None]
        tool_map = aggregates.get(stage)
        if not tool_map:
            continue
        estimated_total = sum(stats.estimated_tokens for stats in tool_map.values())
        calibrated_total = sum(stats.calibrated_tokens for stats in tool_map.values())
        prompt_total = prompt_totals.get(stage, 0)
        for tool_name, stats in sorted(
            tool_map.items(),
            key=lambda item: item[1].calibrated_tokens or item[1].estimated_tokens,
            reverse=True,
        ):
            estimated_variance = variance(
                [
                    float(record.tool_definition_estimated_tokens.get(tool_name, 0))
                    for record in stage_records
                ]
            )
            calibrated_variance = variance(
                [calibrated_tool_value(record, tool_name) for record in matched_stage_records]
            )
            rows.append(
                {
                    "stage": stage,
                    "tool_name": tool_name,
                    "chars": stats.chars,
                    "estimated_tokens": stats.estimated_tokens,
                    "estimated_percent_in_tools": stats.estimated_tokens / estimated_total if estimated_total else 0.0,
                    "estimated_mean": stats.estimated_tokens / len(stage_records) if stage_records else 0.0,
                    "estimated_variance": estimated_variance,
                    "estimated_stddev": standard_deviation(estimated_variance),
                    "calibrated_tokens": stats.calibrated_tokens,
                    "calibrated_percent_in_tools": stats.calibrated_tokens / calibrated_total
                    if calibrated_total
                    else 0.0,
                    "calibrated_percent_in_prompt": stats.calibrated_tokens / prompt_total
                    if prompt_total
                    else 0.0,
                    "calibrated_mean": stats.calibrated_tokens / len(matched_stage_records)
                    if matched_stage_records
                    else 0.0,
                    "calibrated_variance": calibrated_variance,
                    "calibrated_stddev": standard_deviation(calibrated_variance),
                    "occurrences": stats.occurrences,
                }
            )
    return rows


def print_section_table(records: list[PromptRecord]) -> None:
    matched_records = [record for record in records if record.usage is not None]
    actual_total = sum(record.actual_prompt_tokens for record in matched_records)
    estimated_total = sum(record.estimated_prompt_tokens for record in records)
    print("Maisaka Prompt 分段统计")
    print(f"- 请求数: {len(records)}")
    print(f"- 匹配真实 prompt_tokens: {len(matched_records)} / {len(records)}")
    print(f"- 真实 prompt_tokens 合计: {format_int(actual_total)}")
    print(f"- 估算 prompt tokens 合计: {format_int(estimated_total)}")
    print("- calibrated_tokens 为按估算比例分摊真实 prompt_tokens；无法单独拆出模型协议开销。")
    print()

    rows = build_section_rows(records)
    for stage in ("all", *DEFAULT_STAGES):
        stage_rows = [row for row in rows if row["stage"] == stage]
        if not stage_rows:
            continue
        stage_records = records if stage == "all" else [record for record in records if record.stage == stage]
        stage_matched_records = [record for record in stage_records if record.usage is not None]
        stage_actual_total = sum(record.actual_prompt_tokens for record in stage_matched_records)
        print(f"[{stage}] 请求数={len(stage_records)} 真实prompt={format_int(stage_actual_total)}")
        print("section                 calibrated  cal%   mean   std    var       estimated  est%    chars    count")
        for row in stage_rows:
            print(
                f"{row['section']:<22}"
                f"{format_int(row['calibrated_tokens']):>11}  "
                f"{format_percent(row['calibrated_percent']):>6}  "
                f"{format_int(row['calibrated_mean']):>5}  "
                f"{format_int(row['calibrated_stddev']):>5}  "
                f"{format_int(row['calibrated_variance']):>7}  "
                f"{format_int(row['estimated_tokens']):>9}  "
                f"{format_percent(row['estimated_percent']):>6}  "
                f"{format_int(row['chars']):>7}  "
                f"{row['occurrences']:>5}"
            )
        print()


def print_tool_table(records: list[PromptRecord], top_tools: int) -> None:
    if top_tools <= 0:
        return

    rows = build_tool_rows(records)
    section_rows = build_section_rows(records)
    stage_prompt_totals = {
        stage: sum(
            record.actual_prompt_tokens
            for record in (records if stage == "all" else [item for item in records if item.stage == stage])
            if record.usage is not None
        )
        for stage in ("all", *DEFAULT_STAGES)
    }
    tool_definition_totals = {
        str(row["stage"]): float(row["calibrated_tokens"])
        for row in section_rows
        if row["section"] == "tool_definitions"
    }

    print(f"Top {top_tools} 工具 schema 明细")
    for stage in ("all", *DEFAULT_STAGES):
        stage_rows = [row for row in rows if row["stage"] == stage][:top_tools]
        if not stage_rows:
            continue
        print(
            f"[{stage}] 真实prompt={format_int(stage_prompt_totals.get(stage, 0))} "
            f"工具schema合计={format_int(tool_definition_totals.get(stage, 0))}"
        )
        print("tool                         calibrated  prompt%  tools%  mean   std    var       estimated  chars    count")
        prompt_total = max(stage_prompt_totals.get(stage, 0), 0)
        for row in stage_rows:
            prompt_percent = (
                float(row["calibrated_tokens"]) / prompt_total
                if prompt_total
                else 0.0
            )
            print(
                f"{row['tool_name']:<28}"
                f"{format_int(row['calibrated_tokens']):>11}  "
                f"{format_percent(prompt_percent):>7}  "
                f"{format_percent(row['calibrated_percent_in_tools']):>6}  "
                f"{format_int(row['calibrated_mean']):>5}  "
                f"{format_int(row['calibrated_stddev']):>5}  "
                f"{format_int(row['calibrated_variance']):>7}  "
                f"{format_int(row['estimated_tokens']):>9}  "
                f"{format_int(row['chars']):>7}  "
                f"{row['occurrences']:>5}"
            )
        print()


def print_top_records(records: list[PromptRecord], top: int) -> None:
    if top <= 0:
        return

    print(f"Top {top} 单请求分段占比")
    sorted_records = sorted(
        records,
        key=lambda record: record.actual_prompt_tokens or record.estimated_prompt_tokens,
        reverse=True,
    )[:top]
    for record in sorted_records:
        denominator = max(record.estimated_prompt_tokens, 1)
        top_sections = sorted(
            record.section_estimated_tokens.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:4]
        section_text = ", ".join(
            f"{section}={format_percent(tokens / denominator)}"
            for section, tokens in top_sections
        )
        actual_text = (
            f"actual={format_int(record.actual_prompt_tokens)}"
            if record.actual_prompt_tokens > 0
            else "actual=未匹配"
        )
        tool_text = ",".join(record.output_tool_names) or "-"
        print(
            f"- {record.stage} {record.timestamp:%Y-%m-%d %H:%M:%S} "
            f"{actual_text} est={format_int(record.estimated_prompt_tokens)} "
            f"tools={tool_text} {record.chat_dir}/{record.path.name}: {section_text}"
        )
    print()


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)


def write_json(records: list[PromptRecord], output_path: Path) -> None:
    payload = {
        "records": [
            {
                "path": str(record.path),
                "stage": record.stage,
                "chat_dir": record.chat_dir,
                "timestamp": record.timestamp.isoformat(sep=" "),
                "actual_prompt_tokens": record.actual_prompt_tokens,
                "estimated_prompt_tokens": record.estimated_prompt_tokens,
                "message_count": record.message_count,
                "tool_count": record.tool_count,
                "output_tool_names": record.output_tool_names,
                "sections": {
                    section: {
                        "chars": record.section_chars.get(section, 0),
                        "estimated_tokens": estimated_tokens,
                    }
                    for section, estimated_tokens in record.section_estimated_tokens.items()
                },
                "tool_definitions": {
                    tool_name: {
                        "chars": record.tool_definition_chars.get(tool_name, 0),
                        "estimated_tokens": estimated_tokens,
                    }
                    for tool_name, estimated_tokens in record.tool_definition_estimated_tokens.items()
                },
                "matched_usage_id": record.usage.id if record.usage is not None else None,
            }
            for record in records
        ],
        "sections": build_section_rows(records),
        "tools": build_tool_rows(records),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> Namespace:
    parser = ArgumentParser(description="统计 Maisaka timing_gate/planner prompt 中各段内容的 token 占比。")
    parser.add_argument("--root", type=Path, default=DEFAULT_PROMPT_ROOT, help="Maisaka prompt 日志根目录。")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="MaiBot.db 路径，用于匹配真实 prompt_tokens。")
    parser.add_argument(
        "--stages",
        default=",".join(DEFAULT_STAGES),
        help="逗号分隔阶段，默认 timing_gate,planner。",
    )
    parser.add_argument("--chat-filter", default="", help="只分析目录名包含该文本的聊天。")
    parser.add_argument("--since", default="", help="起始时间，格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS。")
    parser.add_argument("--until", default="", help="结束时间，格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS。")
    parser.add_argument("--recent", default="", help="最近时间窗口，例如 30m、24h、7d。")
    parser.add_argument("--limit", type=int, default=200, help="最多分析最新 N 条；0 表示不限制。")
    parser.add_argument("--usage-match-window", type=float, default=20.0, help="日志与 llm_usage 最大匹配秒差。")
    parser.add_argument("--top", type=int, default=10, help="输出 token 最大的前 N 个请求。")
    parser.add_argument("--top-tools", type=int, default=20, help="每个阶段输出 schema 最大的前 N 个工具。")
    parser.add_argument("--csv", type=Path, default=None, help="导出 section 汇总 CSV。")
    parser.add_argument("--json", type=Path, default=None, help="导出明细 JSON。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stages = [stage.strip() for stage in str(args.stages or "").split(",") if stage.strip()]
    unknown_stages = [stage for stage in stages if stage not in REQUEST_TYPE_BY_STAGE]
    if unknown_stages:
        print(f"不支持的阶段: {', '.join(unknown_stages)}", file=sys.stderr)
        return 2

    since = parse_datetime_filter(args.since) or parse_recent_filter(args.recent)
    until = parse_datetime_filter(args.until)
    prompt_files = apply_limit(
        iter_prompt_files(
            args.root,
            stages,
            chat_filter=str(args.chat_filter or "").strip(),
            since=since,
            until=until,
        ),
        int(args.limit),
    )
    if not prompt_files:
        print("没有找到符合条件的 prompt 日志。")
        return 0

    usage_records = load_usage_records(args.db, since, until)
    used_usage_ids: set[int] = set()
    records: list[PromptRecord] = []
    for stage, path in prompt_files:
        timestamp = parse_file_timestamp(path)
        usage = find_matching_usage(
            usage_records=usage_records,
            used_usage_ids=used_usage_ids,
            stage=stage,
            timestamp=timestamp,
            max_seconds=float(args.usage_match_window),
        )
        records.append(load_prompt_record(path, stage=stage, usage=usage))

    print_section_table(records)
    print_tool_table(records, int(args.top_tools))
    print_top_records(records, int(args.top))

    rows = build_section_rows(records)
    if args.csv is not None:
        write_csv(rows, args.csv)
        print(f"CSV 已导出: {args.csv}")
    if args.json is not None:
        write_json(records, args.json)
        print(f"JSON 已导出: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
