"""批量对比表达方式的三种选取方式。

三种方式：
1. 旧流程实际选中项：从日志/消息库中读取已选表达 ID。
2. 精细选择模拟：复用正式 selector prompt，从旧候选池里挑 0-3 条。
3. Planner 向量召回：用 planner 推理和 reply_guide 做 query，召回表达候选。
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from ast import literal_eval
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from random import Random
from sys import path as sys_path
from typing import Any, List, Sequence

import argparse
import asyncio
import json
import re
import sqlite3
import sys

from json_repair import repair_json
import numpy as np

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from src.common.data_models.llm_service_data_models import LLMGenerationOptions  # noqa: E402
from src.common.database.database import DATABASE_URL  # noqa: E402
from src.services.embedding_service import EmbeddingServiceClient  # noqa: E402
from src.services.llm_service import LLMServiceClient  # noqa: E402


@dataclass(frozen=True)
class ExpressionCandidate:
    """表达方式候选。"""

    id: int
    situation: str
    style: str
    count: int
    cluster_id: int | None = None


@dataclass(frozen=True)
class PlannerReplySample:
    """一次 planner 触发 reply 的样本。"""

    sample_id: str
    source_log: str
    planner_line: int
    replyer_line: int
    timestamp: str
    content: str
    reply_guide: str
    target_message_id: str
    actual_reply: str
    old_selected_ids: List[int]
    target_session_id: str = ""
    target_timestamp: str = ""
    target_speaker: str = ""
    target_text: str = ""
    prompt_history_lines: List[str] | None = None
    prompt_expression_candidates: List[dict[str, Any]] | None = None


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="批量对比旧注入、精细选择、planner 向量召回三种表达选取方式。")
    parser.add_argument(
        "--sample-source",
        choices=["maisaka_prompt", "app_log"],
        default="maisaka_prompt",
        help="样本来源；默认读取 logs/maisaka_prompt/replyer。",
    )
    parser.add_argument("--log-dir", default="logs/maisaka_prompt", help="日志目录。")
    parser.add_argument("--log-file", default="", help="指定单个日志文件；为空时自动扫描 log-dir。")
    parser.add_argument("--limit", type=int, default=10, help="最多处理多少个 reply 样本。")
    parser.add_argument("--scan-limit", type=int, default=200, help="从日志中最多先收集多少个候选样本。")
    parser.add_argument("--seed", type=int, default=20260621, help="独立随机采样使用的基础随机种子。")
    parser.add_argument(
        "--sample-mode",
        choices=["recent", "time_spread"],
        default="recent",
        help="样本选择方式；time_spread 会在收集到的候选中按时间顺序尽量拉开间隔。",
    )
    parser.add_argument("--session-id", default="", help="只处理指定 session_id 的样本。")
    parser.add_argument("--max-per-session", type=int, default=0, help="每个 session_id 最多取多少条；0 表示不限制。")
    parser.add_argument("--strict-max-per-session", action="store_true", help="严格执行每个 session_id 的数量上限，不用重复会话补足 limit。")
    parser.add_argument(
        "--exclude-sample-json",
        default="",
        help="排除指定批量对比 JSON 中已经出现过的 sample_id；多份文件用逗号或分号分隔。",
    )
    parser.add_argument("--analysis-json", default="", help="表达向量候选来源；为空时使用 data/analysis 下最新分析结果。")
    parser.add_argument("--top-k", type=int, default=12, help="向量召回输出 top-k。")
    parser.add_argument(
        "--items-per-method",
        type=int,
        default=0,
        help="评分用固定展示数量；大于 0 时三种方案都输出该数量，并让精细选择也按该数量挑选。",
    )
    parser.add_argument("--precise-candidate-pool-size", type=int, default=15, help="精细选择前独立随机抽取的候选池大小。")
    parser.add_argument("--max-concurrent", type=int, default=3, help="embedding 最大并发。")
    parser.add_argument("--skip-precise", action="store_true", help="跳过精细选择 LLM 模拟。")
    parser.add_argument(
        "--include-reply-guide-in-precise",
        action="store_true",
        help="精细选择模拟也附加 reply_guide；默认关闭以贴近现有正式 selector。",
    )
    parser.add_argument("--llm-task-name", default="utils", help="精细选择模拟使用的模型任务名。")
    parser.add_argument("--llm-max-tokens", type=int, default=256, help="精细选择模拟最大输出 token。")
    parser.add_argument("--output-dir", default="data/analysis", help="输出目录。")
    parser.add_argument("--embedding-cache", default="", help="候选表达 embedding 缓存路径；为空时自动生成。")
    return parser


def parse_args() -> Namespace:
    """解析命令行参数。"""

    return build_argument_parser().parse_args()


def normalize_text(value: Any) -> str:
    """压缩空白并去除首尾空白。"""

    return " ".join(str(value or "").split()).strip()


def resolve_path(raw_path: str) -> Path:
    """解析相对项目根目录的路径。"""

    path = Path(str(raw_path or "").strip()).expanduser()
    return path if path.is_absolute() else ROOT_PATH / path


def resolve_analysis_path(raw_path: str) -> Path:
    """解析表达分析 JSON 路径，默认使用最新结果。"""

    if normalize_text(raw_path):
        return resolve_path(raw_path)
    analysis_files = sorted(
        (ROOT_PATH / "data" / "analysis").glob("expression_situation_analysis_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not analysis_files:
        raise FileNotFoundError("未找到 expression_situation_analysis_*.json，请先运行向量分析脚本")
    return analysis_files[0]


def load_excluded_sample_ids(raw_path: str) -> set[str]:
    """从历史批量对比 JSON 中读取需要排除的 sample_id。"""

    if not normalize_text(raw_path):
        return set()
    excluded_sample_ids: set[str] = set()
    for path_text in re.split(r"[,;]", raw_path):
        if not normalize_text(path_text):
            continue
        payload = json.loads(resolve_path(path_text).read_text(encoding="utf-8"))
        excluded_sample_ids.update(
            str(sample.get("sample_id") or "")
            for sample in payload.get("samples") or []
            if isinstance(sample, dict) and sample.get("sample_id")
        )
    return excluded_sample_ids


def build_query_text(content: str, reply_guide: str) -> str:
    """构建 planner 向量召回 query。"""

    if normalize_text(content):
        return f"Planner 推理：\n{content.strip()}"
    if normalize_text(reply_guide):
        return f"回复指引：\n{reply_guide.strip()}"
    return ""


def extract_api_response_content(event: str) -> str:
    """从 APIResponse repr 中提取 content。"""

    match = re.search(r"APIResponse\(content=(?P<literal>'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")", event, re.DOTALL)
    if match is None:
        return ""
    try:
        return str(literal_eval(match.group("literal"))).strip()
    except (SyntaxError, ValueError):
        return ""


def extract_literal_after_key(event: str, key: str) -> str:
    """从 repr 或转义 JSON 片段中提取字符串字段。"""

    patterns = [
        rf"'{re.escape(key)}': (?P<literal>'(?:\\.|[^'])*')",
        rf'"{re.escape(key)}": (?P<double>"(?:\\.|[^"])*")',
        rf'\\"{re.escape(key)}\\": \\"(?P<escaped>.*?)(?<!\\)\\"',
    ]
    for pattern in patterns:
        match = re.search(pattern, event, re.DOTALL)
        if match is None:
            continue
        if match.groupdict().get("literal"):
            try:
                return str(literal_eval(match.group("literal"))).strip()
            except (SyntaxError, ValueError):
                continue
        if match.groupdict().get("double"):
            try:
                return str(literal_eval(match.group("double"))).strip()
            except (SyntaxError, ValueError):
                continue
        escaped = match.groupdict().get("escaped")
        if escaped:
            return escaped.replace(r"\\n", "\n").replace(r"\"", '"').strip()
    return ""


def extract_selected_ids(event: str) -> List[int]:
    """从 replyer 成功日志中提取已选表达 ID。"""

    match = re.search(r"已选表达=\[(?P<ids>[^\]]*)\]", event)
    if match is None:
        return []
    selected_ids: List[int] = []
    for raw_id in match.group("ids").split(","):
        raw_id = raw_id.strip()
        if not raw_id:
            continue
        try:
            selected_ids.append(int(raw_id))
        except ValueError:
            continue
    return selected_ids


def extract_reply_text(event: str) -> str:
    """从 replyer 成功日志中提取回复文本。"""

    match = re.search(r"文本=(?P<literal>'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")", event, re.DOTALL)
    if match is None:
        return ""
    try:
        return str(literal_eval(match.group("literal"))).strip()
    except (SyntaxError, ValueError):
        return ""


def row_event(row: dict[str, Any]) -> str:
    """兼容不同日志字段名，取出事件文本。"""

    return str(row.get("event") or row.get("message") or "")


def row_timestamp(row: dict[str, Any]) -> str:
    """兼容不同日志时间字段。"""

    return str(row.get("timestamp") or row.get("time") or "")


def is_planner_reply_event(event: str) -> bool:
    """判断日志事件是否是 planner 发起 reply。"""

    if "LLM生成内容: APIResponse(content=" not in event:
        return False
    if "func_name='reply'" in event or '"name": "reply"' in event or "reply_guide" in event:
        return bool(extract_literal_after_key(event, "msg_id"))
    return False


def is_replyer_success_event(event: str) -> bool:
    """判断日志事件是否是 replyer 生成成功。"""

    return "Maisaka 回复器生成成功" in event and "已选表达=[" in event


def read_log_rows(log_path: Path) -> List[dict[str, Any]]:
    """读取 JSONL 日志。"""

    rows: List[dict[str, Any]] = []
    for line_number, line in enumerate(log_path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            row["_line_number"] = line_number
            rows.append(row)
    return rows


def load_log_paths(args: Namespace) -> List[Path]:
    """加载待扫描日志路径。"""

    if normalize_text(args.log_file):
        return [resolve_path(args.log_file)]
    log_dir = resolve_path(args.log_dir)
    return sorted(log_dir.glob("app_*.log.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)


def load_prompt_paths(args: Namespace) -> List[Path]:
    """加载 Maisaka prompt replyer 快照路径。"""

    if normalize_text(args.log_file):
        return [resolve_path(args.log_file)]
    log_dir = resolve_path(args.log_dir)
    replyer_dir = log_dir if log_dir.name == "replyer" else log_dir / "replyer"
    return sorted(replyer_dir.rglob("*.txt"), key=lambda path: path.stat().st_mtime, reverse=True)


def extract_between(text: str, start_marker: str, end_markers: Sequence[str]) -> str:
    """提取两个标记之间的文本。"""

    start_index = text.find(start_marker)
    if start_index < 0:
        return ""
    content_start = start_index + len(start_marker)
    end_index = len(text)
    for marker in end_markers:
        marker_index = text.find(marker, content_start)
        if marker_index >= 0:
            end_index = min(end_index, marker_index)
    return text[content_start:end_index].strip()


def extract_prompt_output(text: str) -> str:
    """从 replyer prompt 快照中提取输出结果。"""

    return extract_between(text, "[输出结果]", ["================================================================================"])


def extract_prompt_latest_reason(text: str) -> str:
    """从 replyer prompt 快照中提取最新推理。"""

    return extract_between(
        text,
        "【最新推理】",
        ["\n\n请自然地回复", "\n\n请注意", "\n\n================================================================================"],
    )


def extract_prompt_target(text: str) -> dict[str, str]:
    """从 replyer prompt 快照中提取本次回复目标。"""

    block = extract_between(text, "【本次回复目标】", ["\n\n你这次要回复的就是这条目标消息"])
    result: dict[str, str] = {}
    field_map = {
        "msg_id": "message_id",
        "时间": "timestamp",
        "用户名": "speaker",
        "发言内容": "text",
    }
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if not line.startswith("- ") or "：" not in line:
            continue
        key, value = line[2:].split("：", 1)
        mapped_key = field_map.get(key.strip())
        if mapped_key:
            result[mapped_key] = value.strip()
    return result


def extract_prompt_history_lines(text: str, target_message_id: str, limit: int = 10) -> List[str]:
    """从 prompt 消息片段中提取目标前的最近聊天上下文。"""

    target_marker = "【本次回复目标】"
    prefix = text[: text.find(target_marker)] if target_marker in text else text
    pattern = re.compile(
        r"<message\s+[^>]*msg_id=\"(?P<msg_id>[^\"]+)\"[^>]*time=\"(?P<time>[^\"]*)\"[^>]*user=\"(?P<user>[^\"]*)\"[^>]*>\n(?P<content>.*?)(?=\n<message\s+|\n\n================================================================================|\Z)",
        re.DOTALL,
    )
    lines: List[str] = []
    for match in pattern.finditer(prefix):
        content = normalize_text(match.group("content"))
        if not content:
            continue
        if len(content) > 120:
            content = content[:120] + "..."
        lines.append(f"- {match.group('time')} {match.group('user')}: {content}".strip())
        if match.group("msg_id") == target_message_id:
            break
    return lines[-limit:]


def build_expression_lookup(candidates: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """按 situation/style 构建表达候选查找表。"""

    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in candidates:
        key = (normalize_text(candidate.get("situation")), normalize_text(candidate.get("style")))
        if key[0] and key[1] and key not in lookup:
            lookup[key] = dict(candidate)
    return lookup


def extract_prompt_expression_candidates(
    text: str,
    expression_lookup: dict[tuple[str, str], dict[str, Any]],
) -> List[dict[str, Any]]:
    """从 replyer prompt 中提取表达快照，并尽量匹配数据库 ID。"""

    block = extract_between(text, "【表达组表达集合】", ["\n\n================================================================================"])
    candidates: List[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for match in re.finditer(r"- 情景=(?P<situation>.*?)\s*\|\s*风格=(?P<style>.*)", block):
        situation = normalize_text(match.group("situation"))
        style = normalize_text(match.group("style"))
        key = (situation, style)
        if not situation or not style or key in seen_keys:
            continue
        seen_keys.add(key)
        matched = expression_lookup.get(key)
        if matched is not None:
            candidates.append(dict(matched))
            continue
        candidates.append(
            {
                "id": -len(candidates) - 1,
                "situation": situation,
                "style": style,
                "count": 0,
            }
        )
    return candidates


def collect_samples_from_logs(args: Namespace) -> List[PlannerReplySample]:
    """从日志中收集 planner reply 与实际表达选择结果。"""

    samples: List[PlannerReplySample] = []
    for log_path in load_log_paths(args):
        rows = read_log_rows(log_path)
        pending: dict[str, Any] | None = None
        for row in rows:
            event = row_event(row)
            if is_planner_reply_event(event):
                target_message_id = extract_literal_after_key(event, "msg_id")
                pending = {
                    "source_log": str(log_path),
                    "planner_line": int(row.get("_line_number") or 0),
                    "timestamp": row_timestamp(row),
                    "content": extract_api_response_content(event),
                    "reply_guide": extract_literal_after_key(event, "reply_guide"),
                    "target_message_id": target_message_id,
                }
                continue

            if pending is None or not is_replyer_success_event(event):
                continue

            old_selected_ids = extract_selected_ids(event)
            if old_selected_ids:
                target_message_id = str(pending["target_message_id"])
                sample_id = f"{Path(pending['source_log']).stem}:{pending['planner_line']}:{target_message_id}"
                samples.append(
                    PlannerReplySample(
                        sample_id=sample_id,
                        source_log=str(pending["source_log"]),
                        planner_line=int(pending["planner_line"]),
                        replyer_line=int(row.get("_line_number") or 0),
                        timestamp=str(pending["timestamp"]),
                        content=str(pending["content"]),
                        reply_guide=str(pending["reply_guide"]),
                        target_message_id=target_message_id,
                        actual_reply=extract_reply_text(event),
                        old_selected_ids=old_selected_ids,
                    )
                )
                pending = None
                if len(samples) >= max(1, int(args.scan_limit)):
                    return samples
    return samples


def collect_samples_from_prompts(
    args: Namespace,
    expression_lookup: dict[tuple[str, str], dict[str, Any]],
) -> List[PlannerReplySample]:
    """从 Maisaka replyer prompt 快照中收集样本。"""

    samples: List[PlannerReplySample] = []
    for prompt_path in load_prompt_paths(args):
        text = prompt_path.read_text(encoding="utf-8", errors="replace")
        target = extract_prompt_target(text)
        target_message_id = normalize_text(target.get("message_id"))
        latest_reason = extract_prompt_latest_reason(text)
        actual_reply = extract_prompt_output(text)
        if not target_message_id or not latest_reason or not actual_reply:
            continue
        prompt_candidates = extract_prompt_expression_candidates(text, expression_lookup)
        if not prompt_candidates:
            continue
        target_session_id = prompt_path.parent.name
        sample_id = f"{prompt_path.parent.parent.name}:{target_session_id}:{prompt_path.stem}:{target_message_id}"
        samples.append(
            PlannerReplySample(
                sample_id=sample_id,
                source_log=str(prompt_path),
                planner_line=0,
                replyer_line=0,
                timestamp=str(target.get("timestamp") or datetime.fromtimestamp(prompt_path.stat().st_mtime)),
                content=latest_reason,
                reply_guide="",
                target_message_id=target_message_id,
                actual_reply=actual_reply,
                old_selected_ids=[
                    int(candidate["id"])
                    for candidate in prompt_candidates
                    if isinstance(candidate.get("id"), int) and int(candidate["id"]) > 0
                ],
                target_session_id=target_session_id,
                target_timestamp=str(target.get("timestamp") or ""),
                target_speaker=str(target.get("speaker") or ""),
                target_text=str(target.get("text") or ""),
                prompt_history_lines=extract_prompt_history_lines(text, target_message_id),
                prompt_expression_candidates=prompt_candidates,
            )
        )
        if len(samples) >= max(1, int(args.scan_limit)):
            return samples
    return samples


def connect_database() -> sqlite3.Connection:
    """连接当前 SQLite 数据库。"""

    database_prefix = "sqlite:///"
    if not DATABASE_URL.startswith(database_prefix):
        raise ValueError(f"当前脚本只支持 sqlite 数据库: {DATABASE_URL}")
    database_path = Path(DATABASE_URL[len(database_prefix) :])
    if not database_path.is_absolute():
        database_path = ROOT_PATH / database_path
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    return connection


def load_expression_details(connection: sqlite3.Connection, expression_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
    """按 ID 读取表达详情。"""

    unique_ids = list(dict.fromkeys(int(expression_id) for expression_id in expression_ids if int(expression_id) > 0))
    if not unique_ids:
        return {}
    placeholders = ",".join("?" for _ in unique_ids)
    rows = connection.execute(
        f"SELECT id, situation, style, count FROM expressions WHERE id IN ({placeholders})",
        unique_ids,
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def load_all_expression_candidates(connection: sqlite3.Connection) -> List[dict[str, Any]]:
    """读取可用于独立随机采样的全局表达候选。"""

    rows = connection.execute(
        """
        SELECT id, situation, style, count
        FROM expressions
        WHERE situation IS NOT NULL
          AND style IS NOT NULL
          AND length(situation) > 0
          AND length(style) > 0
        ORDER BY id ASC
        """
    ).fetchall()
    candidates: List[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for row in rows:
        expression_id = int(row["id"] or 0)
        situation = normalize_text(row["situation"])
        style = normalize_text(row["style"])
        if expression_id <= 0 or not situation or not style or expression_id in seen_ids:
            continue
        seen_ids.add(expression_id)
        candidates.append(
            {
                "id": expression_id,
                "situation": situation,
                "style": style,
                "count": int(row["count"] or 0),
            }
        )
    return candidates


def build_sample_seed(base_seed: int, sample: PlannerReplySample, namespace: str) -> int:
    """为单个样本生成可复现的随机种子。"""

    seed_text = f"{base_seed}:{namespace}:{sample.sample_id}:{sample.target_message_id}"
    return int.from_bytes(sha256(seed_text.encode("utf-8")).digest()[:8], "big")


def sample_independent_precise_candidates(
    *,
    all_candidates: Sequence[dict[str, Any]],
    sample: PlannerReplySample,
    pool_size: int,
    base_seed: int,
    excluded_ids: set[int],
) -> List[dict[str, Any]]:
    """为精细选择独立随机抽取候选池。"""

    available_candidates = [
        candidate
        for candidate in all_candidates
        if int(candidate.get("id") or 0) not in excluded_ids
    ]
    if not available_candidates:
        return []

    randomizer = Random(build_sample_seed(base_seed, sample, "precise_candidate_pool"))
    shuffled_candidates = list(available_candidates)
    randomizer.shuffle(shuffled_candidates)
    return shuffled_candidates[: max(1, int(pool_size))]


def load_target_message(connection: sqlite3.Connection, message_id: str) -> dict[str, Any]:
    """读取目标消息。"""

    row = connection.execute(
        """
        SELECT message_id, timestamp, session_id, user_nickname, user_cardname, processed_plain_text
        FROM mai_messages
        WHERE message_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (message_id,),
    ).fetchone()
    return dict(row) if row is not None else {}


def load_history_lines(
    connection: sqlite3.Connection,
    *,
    session_id: str,
    until_timestamp: str,
    limit: int = 10,
) -> List[str]:
    """按正式 selector 近似格式构造最近上下文。"""

    if not session_id or not until_timestamp:
        return []
    rows = connection.execute(
        """
        SELECT timestamp, user_nickname, user_cardname, processed_plain_text
        FROM mai_messages
        WHERE session_id=? AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (session_id, until_timestamp, limit),
    ).fetchall()
    history_lines: List[str] = []
    for row in reversed(rows):
        content = normalize_text(row["processed_plain_text"])
        if not content:
            continue
        if len(content) > 120:
            content = content[:120] + "..."
        timestamp = str(row["timestamp"] or "").split(" ")[-1].split(".")[0]
        role = str(row["user_cardname"] or row["user_nickname"] or "unknown")
        history_lines.append(f"- {timestamp} {role}: {content}".strip())
    return history_lines


def build_selector_prompt(
    *,
    history_lines: Sequence[str],
    target_text: str,
    reply_reason: str,
    candidates: Sequence[dict[str, Any]],
    exact_count: int = 0,
) -> str:
    """构建与正式精细表达选择近似一致的 prompt。"""

    history_block = "\n".join(history_lines) if history_lines else "- 无可用上下文"
    candidate_lines = [
        f"{candidate['id']}: 情景={candidate['situation']} | 风格={candidate['style']} | count={candidate['count']}"
        for candidate in candidates
    ]
    if exact_count > 0:
        selection_instruction = f"请只从下面候选中选择正好 {exact_count} 条最适合当前语境的表达方式。"
        fallback_instruction = f"如果候选里没有 {exact_count} 条都明显合适，也请按相对合适程度补足 {exact_count} 条。"
        output_example = '{"selected_ids":[123,456,789,101,112]}'
    else:
        selection_instruction = "请只从下面候选中选择 0 到 3 条最适合当前语境的表达方式。"
        fallback_instruction = "如果没有明显合适的，就返回空数组。"
        output_example = '{"selected_ids":[123,456]}'
    return (
        "你是 Maisaka 的表达方式选择子代理。\n"
        "你只负责根据最近聊天上下文，为这一次可见回复挑选最合适的表达方式。\n"
        f"{selection_instruction}\n"
        "优先考虑自然、贴合上下文、不生硬、不模板化。\n"
        f"{fallback_instruction}\n"
        f"严格只输出 JSON，对象格式为 {output_example}。\n\n"
        f"最近上下文：\n{history_block}\n\n"
        f"目标消息：{target_text or '无'}\n"
        f"回复理由：{reply_reason.strip() or '无'}\n\n"
        f"候选表达方式：\n{chr(10).join(candidate_lines)}"
    )


def parse_precise_selected_ids(raw_response: str, candidate_ids: set[int]) -> List[int]:
    """解析精细选择模型输出。"""

    if not raw_response.strip():
        return []
    try:
        parsed_result = json.loads(repair_json(raw_response))
    except Exception:
        return []
    raw_selected_ids = parsed_result.get("selected_ids", []) if isinstance(parsed_result, dict) else []
    if not isinstance(raw_selected_ids, list):
        return []
    selected_ids: List[int] = []
    for raw_id in raw_selected_ids:
        if not isinstance(raw_id, int):
            continue
        if raw_id not in candidate_ids or raw_id in selected_ids:
            continue
        selected_ids.append(raw_id)
    return selected_ids


async def run_precise_selection(
    *,
    args: Namespace,
    sample: PlannerReplySample,
    history_lines: Sequence[str],
    target_text: str,
    old_candidates: Sequence[dict[str, Any]],
    session_id: str,
) -> dict[str, Any]:
    """运行精细表达选择模拟。"""

    if args.skip_precise or not old_candidates:
        return {
            "skipped": True,
            "selected_ids": [],
            "selected_expressions": [],
            "raw_response": "",
            "model_name": "",
        }

    reply_reason = sample.content
    if args.include_reply_guide_in_precise and normalize_text(sample.reply_guide):
        reply_reason = f"{reply_reason}\n\n回复指引：\n{sample.reply_guide}".strip()
    prompt = build_selector_prompt(
        history_lines=history_lines,
        target_text=target_text,
        reply_reason=reply_reason,
        candidates=old_candidates,
        exact_count=max(0, int(args.items_per_method)),
    )
    llm_client = LLMServiceClient(
        task_name=str(args.llm_task_name or "utils"),
        request_type="expression.precise_selection_batch_compare",
        session_id=session_id,
    )
    response = await llm_client.generate_response(
        prompt,
        LLMGenerationOptions(
            temperature=0,
            max_tokens=max(1, int(args.llm_max_tokens)),
        ),
        session_id=session_id,
    )
    candidate_ids = {int(candidate["id"]) for candidate in old_candidates if isinstance(candidate.get("id"), int)}
    selected_ids = parse_precise_selected_ids(response.response.strip(), candidate_ids)
    fixed_count = max(0, int(args.items_per_method))
    if fixed_count > 0:
        for candidate in old_candidates:
            candidate_id = int(candidate["id"])
            if candidate_id not in selected_ids:
                selected_ids.append(candidate_id)
            if len(selected_ids) >= fixed_count:
                break
        selected_ids = selected_ids[:fixed_count]
    else:
        selected_ids = selected_ids[:3]
    candidate_by_id = {int(candidate["id"]): candidate for candidate in old_candidates if isinstance(candidate.get("id"), int)}
    return {
        "skipped": False,
        "selected_ids": selected_ids,
        "selected_expressions": [candidate_by_id[expression_id] for expression_id in selected_ids],
        "raw_response": response.response.strip(),
        "model_name": response.model_name,
        "prompt_tokens": response.prompt_tokens,
        "completion_tokens": response.completion_tokens,
        "uses_reply_guide": bool(args.include_reply_guide_in_precise),
    }


def load_vector_candidates(analysis_path: Path) -> List[ExpressionCandidate]:
    """从分析 JSON 读取向量召回候选。"""

    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    cluster_by_expression_id: dict[int, int] = {}
    for cluster in payload.get("clusters") or []:
        if not isinstance(cluster, dict):
            continue
        cluster_id = int(cluster.get("cluster_id"))
        for member in cluster.get("members") or []:
            if isinstance(member, dict) and isinstance(member.get("id"), int):
                cluster_by_expression_id[int(member["id"])] = cluster_id

    candidates: List[ExpressionCandidate] = []
    seen_ids: set[int] = set()
    for sample in payload.get("samples") or []:
        if not isinstance(sample, dict):
            continue
        expression_id = int(sample.get("id") or 0)
        situation = normalize_text(sample.get("situation"))
        style = normalize_text(sample.get("style"))
        if expression_id <= 0 or not situation or not style or expression_id in seen_ids:
            continue
        seen_ids.add(expression_id)
        candidates.append(
            ExpressionCandidate(
                id=expression_id,
                situation=situation,
                style=style,
                count=int(sample.get("count") or 0),
                cluster_id=cluster_by_expression_id.get(expression_id),
            )
        )
    return candidates


def candidate_text(candidate: ExpressionCandidate) -> str:
    """构建候选 embedding 文本。"""

    return f"情境：{candidate.situation}\n表达方式：{candidate.style}"


def l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """按行做 L2 归一化。"""

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= 0):
        raise ValueError("存在零向量，无法计算余弦相似度")
    return matrix / norms


def load_embedding_cache(cache_path: Path, candidate_ids: Sequence[int]) -> tuple[np.ndarray, str] | None:
    """读取候选 embedding 缓存。"""

    if not cache_path.exists():
        return None
    try:
        payload = np.load(cache_path, allow_pickle=False)
        cached_ids = payload["ids"].astype(np.int64).tolist()
        if cached_ids != list(candidate_ids):
            return None
        model_name = str(payload["model_name"].tolist())
        embeddings = payload["embeddings"].astype(np.float32)
        return embeddings, model_name
    except Exception:
        return None


def save_embedding_cache(cache_path: Path, candidate_ids: Sequence[int], embeddings: np.ndarray, model_name: str) -> None:
    """保存候选 embedding 缓存。"""

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        cache_path,
        ids=np.array(list(candidate_ids), dtype=np.int64),
        embeddings=embeddings.astype(np.float32),
        model_name=np.array(model_name),
    )


async def embed_texts(
    texts: Sequence[str],
    *,
    request_type: str,
    max_concurrent: int,
) -> tuple[np.ndarray, str]:
    """批量向量化文本。"""

    client = EmbeddingServiceClient(task_name="embedding", request_type=request_type)
    results = await client.embed_texts(list(texts), max_concurrent=max(1, int(max_concurrent)))
    embeddings = np.array([result.embedding for result in results], dtype=np.float32)
    if embeddings.ndim != 2 or embeddings.shape[0] != len(texts):
        raise ValueError(f"embedding 结果维度异常: shape={embeddings.shape}, texts={len(texts)}")
    return embeddings, results[0].model_name if results else ""


async def prepare_candidate_vectors(
    *,
    args: Namespace,
    analysis_path: Path,
    candidates: Sequence[ExpressionCandidate],
) -> tuple[np.ndarray, str, Path]:
    """准备候选表达向量。"""

    candidate_ids = [candidate.id for candidate in candidates]
    cache_path = resolve_path(args.embedding_cache) if normalize_text(args.embedding_cache) else (
        resolve_path(args.output_dir) / f"{analysis_path.stem}_candidate_embeddings.npz"
    )
    cached = load_embedding_cache(cache_path, candidate_ids)
    if cached is not None:
        return cached[0], cached[1], cache_path

    embeddings, model_name = await embed_texts(
        [candidate_text(candidate) for candidate in candidates],
        request_type="expression.selection_batch_candidates",
        max_concurrent=int(args.max_concurrent),
    )
    save_embedding_cache(cache_path, candidate_ids, embeddings, model_name)
    return embeddings, model_name, cache_path


def rank_vector_matches(
    *,
    candidates: Sequence[ExpressionCandidate],
    candidate_vectors: np.ndarray,
    query_vector: np.ndarray,
    top_k: int,
) -> List[dict[str, Any]]:
    """按余弦相似度排序表达候选。"""

    scores = candidate_vectors @ query_vector
    top_indices = np.argsort(-scores)[: max(1, int(top_k))]
    matches: List[dict[str, Any]] = []
    for index in top_indices:
        candidate = candidates[int(index)]
        matches.append(
            {
                "id": candidate.id,
                "similarity": round(float(scores[int(index)]), 4),
                "situation": candidate.situation,
                "style": candidate.style,
                "count": candidate.count,
                "cluster_id": candidate.cluster_id,
            }
        )
    return matches


def build_cluster_summaries(
    *,
    candidates: Sequence[ExpressionCandidate],
    candidate_vectors: np.ndarray,
) -> List[dict[str, Any]]:
    """基于候选向量构建聚类中心。"""

    indices_by_cluster: dict[int, List[int]] = {}
    for index, candidate in enumerate(candidates):
        cluster_id = candidate.cluster_id if candidate.cluster_id is not None else -1
        indices_by_cluster.setdefault(int(cluster_id), []).append(index)

    cluster_summaries: List[dict[str, Any]] = []
    for cluster_id, indices in indices_by_cluster.items():
        vectors = candidate_vectors[indices]
        center = vectors.mean(axis=0)
        norm = float(np.linalg.norm(center))
        if norm <= 0:
            continue
        cluster_summaries.append(
            {
                "cluster_id": cluster_id,
                "indices": indices,
                "center": center / norm,
            }
        )
    return cluster_summaries


def sample_from_nearest_clusters(
    *,
    candidates: Sequence[ExpressionCandidate],
    cluster_summaries: Sequence[dict[str, Any]],
    query_vector: np.ndarray,
    sample: PlannerReplySample,
    top_k: int,
    base_seed: int,
) -> List[dict[str, Any]]:
    """先找最接近的聚类簇，再从簇内稳定随机抽样。"""

    limit = max(1, int(top_k))
    scored_clusters: List[dict[str, Any]] = []
    for cluster_summary in cluster_summaries:
        center = cluster_summary["center"]
        cluster_score = float(center @ query_vector)
        scored_clusters.append(
            {
                **cluster_summary,
                "similarity": cluster_score,
            }
        )
    scored_clusters.sort(key=lambda item: item["similarity"], reverse=True)

    matches: List[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for cluster_rank, cluster_summary in enumerate(scored_clusters, 1):
        cluster_id = int(cluster_summary["cluster_id"])
        indices = list(cluster_summary["indices"])
        randomizer = Random(build_sample_seed(base_seed, sample, f"cluster_sample:{cluster_id}"))
        randomizer.shuffle(indices)
        for index in indices:
            candidate = candidates[int(index)]
            if candidate.id in seen_ids:
                continue
            seen_ids.add(candidate.id)
            matches.append(
                {
                    "id": candidate.id,
                    "similarity": round(float(cluster_summary["similarity"]), 4),
                    "cluster_similarity": round(float(cluster_summary["similarity"]), 4),
                    "cluster_rank": cluster_rank,
                    "sampled_from_cluster": True,
                    "situation": candidate.situation,
                    "style": candidate.style,
                    "count": candidate.count,
                    "cluster_id": None if cluster_id < 0 else cluster_id,
                }
            )
            if len(matches) >= limit:
                return matches
    return matches


def filter_samples_by_session(
    *,
    args: Namespace,
    connection: sqlite3.Connection,
    samples: Sequence[PlannerReplySample],
) -> List[PlannerReplySample]:
    """按 session_id 过滤样本。"""

    session_id = normalize_text(args.session_id)
    if not session_id:
        return list(samples)
    filtered: List[PlannerReplySample] = []
    for sample in samples:
        target_message = load_target_message(connection, sample.target_message_id)
        sample_session_id = str(target_message.get("session_id") or sample.target_session_id or "")
        if sample_session_id == session_id:
            filtered.append(sample)
    return filtered


def diversify_samples_by_session(
    *,
    args: Namespace,
    connection: sqlite3.Connection,
    samples: Sequence[PlannerReplySample],
) -> List[PlannerReplySample]:
    """按 session_id 分散选择样本。"""

    limit = max(1, int(args.limit))
    max_per_session = max(0, int(args.max_per_session))
    if max_per_session <= 0:
        return list(samples[:limit])

    selected_samples: List[PlannerReplySample] = []
    skipped_samples: List[PlannerReplySample] = []
    selected_counts: dict[str, int] = {}
    for sample in samples:
        target_message = load_target_message(connection, sample.target_message_id)
        session_id = str(target_message.get("session_id") or sample.target_session_id or "").strip()
        session_id = session_id or "unknown"
        if selected_counts.get(session_id, 0) >= max_per_session:
            skipped_samples.append(sample)
            continue
        selected_samples.append(sample)
        selected_counts[session_id] = selected_counts.get(session_id, 0) + 1
        if len(selected_samples) >= limit:
            return selected_samples

    if args.strict_max_per_session:
        return selected_samples

    # 会话数量不足时，用剩余样本补足 limit，避免一次实验样本数过少。
    for sample in skipped_samples:
        selected_samples.append(sample)
        if len(selected_samples) >= limit:
            break
    return selected_samples


def spread_samples_over_time(samples: Sequence[PlannerReplySample], limit: int) -> List[PlannerReplySample]:
    """在候选序列中均匀取样，尽量拉宽时间跨度。"""

    if limit <= 0 or not samples:
        return []
    if len(samples) <= limit:
        return list(samples)

    selected_indices: List[int] = []
    last_index = len(samples) - 1
    for offset in range(limit):
        index = round(offset * last_index / max(limit - 1, 1))
        if index not in selected_indices:
            selected_indices.append(index)

    # 四舍五入可能在极端情况下撞 index，用未选位置补足。
    if len(selected_indices) < limit:
        for index in range(len(samples)):
            if index not in selected_indices:
                selected_indices.append(index)
            if len(selected_indices) >= limit:
                break
    return [samples[index] for index in selected_indices[:limit]]


def build_expression_list(ids: Sequence[int], expression_by_id: dict[int, dict[str, Any]]) -> List[dict[str, Any]]:
    """按 ID 顺序构建表达详情列表。"""

    return [
        expression_by_id[expression_id]
        for expression_id in ids
        if expression_id in expression_by_id
    ]


def write_markdown_report(report_path: Path, payload: dict[str, Any]) -> None:
    """写出简短 Markdown 报告。"""

    lines = [
        "# 表达选择三种方式批量对比",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- 样本数：{len(payload['samples'])}",
        f"- 向量候选：{payload['analysis_json']}",
        f"- 向量模型：{payload.get('embedding_model') or ''}",
        "",
    ]
    for index, sample in enumerate(payload["samples"], 1):
        lines.extend(
            [
                f"## 样本 {index}: {sample['target_message'].get('text') or sample['target_message_id']}",
                "",
                f"- 实际回复：{sample.get('actual_reply') or ''}",
                f"- 旧流程 ID：{sample['old_direct']['selected_ids']}",
                f"- 精细模拟 ID：{sample['precise_selection']['selected_ids']}",
                f"- 向量召回 ID：{sample['vector_recall']['selected_ids']}",
                "",
                "### 旧流程",
            ]
        )
        for item in sample["old_direct"]["selected_expressions"]:
            lines.append(f"- id={item['id']} | {item['situation']} -> {item['style']} | count={item.get('count')}")
        lines.append("")
        lines.append("### 精细选择模拟")
        if sample["precise_selection"].get("skipped"):
            lines.append("- 已跳过")
        elif sample["precise_selection"]["selected_expressions"]:
            for item in sample["precise_selection"]["selected_expressions"]:
                lines.append(f"- id={item['id']} | {item['situation']} -> {item['style']} | count={item.get('count')}")
        else:
            lines.append("- 未选择任何表达")
        lines.append("")
        lines.append("### Planner 向量召回")
        for item in sample["vector_recall"]["matches"]:
            lines.append(
                f"- sim={item['similarity']:.4f} id={item['id']} cluster={item.get('cluster_id')} | "
                f"{item['situation']} -> {item['style']}"
            )
        lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")


async def build_batch_payload(args: Namespace) -> dict[str, Any]:
    """构建批量对比结果。"""

    analysis_path = resolve_analysis_path(args.analysis_json)
    vector_candidates = load_vector_candidates(analysis_path)
    if not vector_candidates:
        raise ValueError(f"分析文件中没有可用表达候选: {analysis_path}")

    connection = connect_database()
    all_expression_candidates = load_all_expression_candidates(connection)
    if args.sample_source == "app_log":
        raw_samples = collect_samples_from_logs(args)
    else:
        raw_samples = collect_samples_from_prompts(
            args,
            expression_lookup=build_expression_lookup(all_expression_candidates),
        )
    excluded_sample_ids = load_excluded_sample_ids(args.exclude_sample_json)
    if excluded_sample_ids:
        before_count = len(raw_samples)
        raw_samples = [sample for sample in raw_samples if sample.sample_id not in excluded_sample_ids]
        print(f"已排除历史样本: {before_count - len(raw_samples)}")
    samples = filter_samples_by_session(args=args, connection=connection, samples=raw_samples)
    if args.sample_mode == "time_spread":
        spread_pool_size = min(
            len(samples),
            max(max(1, int(args.limit)) * 8, max(1, int(args.limit))),
        )
        samples = spread_samples_over_time(samples, spread_pool_size)
    samples = diversify_samples_by_session(args=args, connection=connection, samples=samples)
    if not samples:
        raise ValueError("未找到可对比的 reply 样本")

    candidate_vectors, embedding_model, cache_path = await prepare_candidate_vectors(
        args=args,
        analysis_path=analysis_path,
        candidates=vector_candidates,
    )
    normalized_candidate_vectors = l2_normalize(candidate_vectors)
    cluster_summaries = build_cluster_summaries(
        candidates=vector_candidates,
        candidate_vectors=normalized_candidate_vectors,
    )
    query_vectors, query_model = await embed_texts(
        [build_query_text(sample.content, sample.reply_guide) for sample in samples],
        request_type="expression.selection_batch_queries",
        max_concurrent=int(args.max_concurrent),
    )
    normalized_query_vectors = l2_normalize(query_vectors)
    embedding_model = embedding_model or query_model

    output_samples: List[dict[str, Any]] = []
    for sample_index, sample in enumerate(samples):
        db_target_message = load_target_message(connection, sample.target_message_id)
        session_id = str(db_target_message.get("session_id") or sample.target_session_id or "")
        target_timestamp = str(db_target_message.get("timestamp") or sample.target_timestamp or "")
        target_text = str(db_target_message.get("processed_plain_text") or sample.target_text or "")
        target_speaker = str(
            db_target_message.get("user_cardname")
            or db_target_message.get("user_nickname")
            or sample.target_speaker
            or ""
        )
        history_lines = list(sample.prompt_history_lines or [])
        if not history_lines:
            history_lines = load_history_lines(
                connection,
                session_id=session_id,
                until_timestamp=target_timestamp,
            )
        fixed_count = max(0, int(args.items_per_method))
        if sample.prompt_expression_candidates:
            prompt_candidates = list(sample.prompt_expression_candidates)
            randomizer = Random(build_sample_seed(int(args.seed), sample, "prompt_direct_candidates"))
            randomizer.shuffle(prompt_candidates)
            old_candidates = prompt_candidates[:fixed_count] if fixed_count > 0 else prompt_candidates
            old_selected_ids = [int(candidate["id"]) for candidate in old_candidates if isinstance(candidate.get("id"), int)]
        else:
            old_expression_by_id = load_expression_details(connection, sample.old_selected_ids)
            old_selected_ids = list(sample.old_selected_ids)
            if fixed_count > 0:
                old_selected_ids = old_selected_ids[:fixed_count]
            old_candidates = build_expression_list(old_selected_ids, old_expression_by_id)
        precise_candidate_pool = sample_independent_precise_candidates(
            all_candidates=all_expression_candidates,
            sample=sample,
            pool_size=max(1, int(args.precise_candidate_pool_size)),
            base_seed=int(args.seed),
            excluded_ids={expression_id for expression_id in old_selected_ids if expression_id > 0},
        )
        precise_result = await run_precise_selection(
            args=args,
            sample=sample,
            history_lines=history_lines,
            target_text=target_text,
            old_candidates=precise_candidate_pool,
            session_id=session_id,
        )
        vector_matches = sample_from_nearest_clusters(
            candidates=vector_candidates,
            cluster_summaries=cluster_summaries,
            query_vector=normalized_query_vectors[sample_index],
            sample=sample,
            top_k=fixed_count if fixed_count > 0 else int(args.top_k),
            base_seed=int(args.seed),
        )
        output_samples.append(
            {
                **asdict(sample),
                "query_text": build_query_text(sample.content, sample.reply_guide),
                "target_message": {
                    "message_id": sample.target_message_id,
                    "session_id": session_id,
                    "timestamp": target_timestamp,
                    "speaker": target_speaker,
                    "text": target_text,
                },
                "history_lines": history_lines,
                "old_direct": {
                    "selected_ids": old_selected_ids,
                    "selected_expressions": old_candidates,
                },
                "precise_selection": precise_result,
                "vector_recall": {
                    "selected_ids": [match["id"] for match in vector_matches],
                    "matches": vector_matches,
                },
            }
        )

    connection.close()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "args": vars(args),
        "database_url": DATABASE_URL,
        "analysis_json": str(analysis_path),
        "embedding_model": embedding_model,
        "embedding_cache": str(cache_path),
        "method_labels": {
            "old_direct": "旧流程实际选中",
            "precise_selection": "精细选择模拟",
            "vector_recall": "Planner 最近簇抽样",
        },
        "samples": output_samples,
    }


def write_outputs(args: Namespace, payload: dict[str, Any]) -> tuple[Path, Path]:
    """写出 JSON 和 Markdown。"""

    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"expression_selection_batch_compare_{timestamp}.json"
    markdown_path = output_dir / f"expression_selection_batch_compare_{timestamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown_report(markdown_path, payload)
    return json_path, markdown_path


async def async_main() -> None:
    """脚本入口。"""

    args = parse_args()
    payload = await build_batch_payload(args)
    json_path, markdown_path = write_outputs(args, payload)
    print(f"样本数: {len(payload['samples'])}")
    print(f"向量模型: {payload.get('embedding_model') or ''}")
    print(f"JSON 输出: {json_path.resolve()}")
    print(f"Markdown 输出: {markdown_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(async_main())
