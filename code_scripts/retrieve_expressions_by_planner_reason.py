"""用 planner 推理/回复指引用向量召回表达方式候选。"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from ast import literal_eval
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from sys import path as sys_path
from typing import Any, Dict, List, Sequence

import argparse
import asyncio
import json
import re
import sys

import numpy as np

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from src.common.database.database import DATABASE_URL  # noqa: E402
from src.services.embedding_service import EmbeddingServiceClient  # noqa: E402


@dataclass(frozen=True)
class PlannerReasonSample:
    """从日志或参数获得的一段 planner 推理。"""

    source: str
    content: str
    reply_guide: str
    query_text: str


@dataclass(frozen=True)
class ExpressionCandidate:
    """表达方式候选。"""

    id: int
    situation: str
    style: str
    count: int
    cluster_id: int | None = None


@dataclass(frozen=True)
class ExpressionMatch:
    """planner 推理召回的表达方式。"""

    id: int
    similarity: float
    situation: str
    style: str
    count: int
    cluster_id: int | None


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="用 planner 推理/回复指引用向量召回表达方式候选。")
    parser.add_argument("--reply-reason", default="", help="直接指定 planner 推理/回复理由。")
    parser.add_argument("--reply-guide", default="", help="直接指定 reply 工具的 reply_guide。")
    parser.add_argument("--log-dir", default="logs", help="未指定 reply-reason 时，从该目录的 app_*.jsonl 抽最近 planner reply。")
    parser.add_argument(
        "--analysis-json",
        default="data/analysis/expression_situation_analysis_20260621_111603.json",
        help="表达候选来源，默认使用 500 条统一聚类结果。",
    )
    parser.add_argument("--top-k", type=int, default=12, help="输出 top-k 表达候选。")
    parser.add_argument("--max-concurrent", type=int, default=3, help="embedding 最大并发。")
    parser.add_argument("--output-dir", default="data/analysis", help="输出目录。")
    return parser


def parse_args() -> Namespace:
    """解析命令行参数。"""

    return build_argument_parser().parse_args()


def normalize_text(value: Any) -> str:
    """压缩空白并去除首尾空白。"""

    return " ".join(str(value or "").split()).strip()


def resolve_path(raw_path: str) -> Path:
    """解析相对项目根目录的路径。"""

    path = Path(normalize_text(raw_path)).expanduser()
    return path if path.is_absolute() else ROOT_PATH / path


def build_query_text(content: str, reply_guide: str) -> str:
    """构建用于向量召回的 planner query。"""

    parts = []
    if normalize_text(content):
        parts.append(f"Planner 推理：\n{content.strip()}")
    if normalize_text(reply_guide):
        parts.append(f"回复指引：\n{reply_guide.strip()}")
    return "\n\n".join(parts)


def load_planner_reason_sample(args: Namespace) -> PlannerReasonSample:
    """读取 planner 推理样本。"""

    reply_reason = str(args.reply_reason or "").strip()
    reply_guide = str(args.reply_guide or "").strip()
    if reply_reason or reply_guide:
        return PlannerReasonSample(
            source="arguments",
            content=reply_reason,
            reply_guide=reply_guide,
            query_text=build_query_text(reply_reason, reply_guide),
        )

    return extract_latest_planner_reply(resolve_path(args.log_dir))


def extract_latest_planner_reply(log_dir: Path) -> PlannerReasonSample:
    """从最近 app 日志中提取 planner reply 输出。"""

    log_files = sorted(log_dir.glob("app_*.log.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
    for log_path in log_files:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in reversed(lines):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            event = str(row.get("event") or "")
            if "LLM生成内容: APIResponse(content=" not in event:
                continue
            if "LLMOrchestrator[maisaka.planner]" in event:
                continue
            if "func_name='reply'" not in event and "\"name\": \"reply\"" not in event and "reply_guide" not in event:
                continue
            content = extract_api_response_content(event)
            reply_guide = extract_reply_guide(event)
            if content or reply_guide:
                return PlannerReasonSample(
                    source=str(log_path),
                    content=content,
                    reply_guide=reply_guide,
                    query_text=build_query_text(content, reply_guide),
                )
    raise ValueError(f"未能从 {log_dir} 找到 planner reply 推理日志")


def extract_api_response_content(event: str) -> str:
    """从 APIResponse repr 中提取 content。"""

    match = re.search(r"APIResponse\(content=(?P<literal>'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")", event, re.DOTALL)
    if match is None:
        return ""
    try:
        return str(literal_eval(match.group("literal"))).strip()
    except (SyntaxError, ValueError):
        return ""


def extract_reply_guide(event: str) -> str:
    """从 ToolCall repr 中提取 reply_guide。"""

    patterns = [
        r"'reply_guide': (?P<literal>'(?:\\.|[^'])*')",
        r'\\"reply_guide\\": \\"(?P<escaped>.*?)(?<!\\)\\"',
    ]
    for pattern in patterns:
        match = re.search(pattern, event, re.DOTALL)
        if match is None:
            continue
        if "literal" in match.groupdict() and match.group("literal"):
            try:
                return str(literal_eval(match.group("literal"))).strip()
            except (SyntaxError, ValueError):
                continue
        escaped = match.groupdict().get("escaped")
        if escaped:
            return escaped.replace(r"\\n", "\n").replace(r"\"", '"').strip()
    return ""


def load_expression_candidates(analysis_path: Path) -> List[ExpressionCandidate]:
    """从聚类分析 JSON 读取表达候选。"""

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
    """构建候选表达 embedding 文本。"""

    return f"情境：{candidate.situation}\n表达方式：{candidate.style}"


async def embed_texts(texts: Sequence[str], *, max_concurrent: int) -> tuple[np.ndarray, str]:
    """批量向量化文本。"""

    client = EmbeddingServiceClient(task_name="embedding", request_type="expression.planner_reason_retrieval")
    results = await client.embed_texts(list(texts), max_concurrent=max(1, int(max_concurrent)))
    embeddings = np.array([result.embedding for result in results], dtype=np.float32)
    if embeddings.ndim != 2 or embeddings.shape[0] != len(texts):
        raise ValueError(f"embedding 结果维度异常: shape={embeddings.shape}, texts={len(texts)}")
    return embeddings, results[0].model_name if results else ""


def l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """按行做 L2 归一化。"""

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= 0):
        raise ValueError("存在零向量，无法计算余弦相似度")
    return matrix / norms


def rank_candidates(
    *,
    candidates: Sequence[ExpressionCandidate],
    candidate_vectors: np.ndarray,
    query_vector: np.ndarray,
    top_k: int,
) -> List[ExpressionMatch]:
    """按余弦相似度排序候选表达。"""

    scores = candidate_vectors @ query_vector
    ranked_indices = np.argsort(scores)[::-1][: max(1, int(top_k))]
    return [
        ExpressionMatch(
            id=candidates[int(index)].id,
            similarity=round(float(scores[int(index)]), 4),
            situation=candidates[int(index)].situation,
            style=candidates[int(index)].style,
            count=candidates[int(index)].count,
            cluster_id=candidates[int(index)].cluster_id,
        )
        for index in ranked_indices
    ]


def write_outputs(
    *,
    args: Namespace,
    analysis_path: Path,
    planner_sample: PlannerReasonSample,
    matches: Sequence[ExpressionMatch],
    embedding_model: str,
) -> tuple[Path, Path]:
    """写出召回结果。"""

    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"planner_reason_expression_retrieval_{timestamp}.json"
    markdown_path = output_dir / f"planner_reason_expression_retrieval_{timestamp}.md"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_url": DATABASE_URL,
        "analysis_json": str(analysis_path),
        "embedding_model": embedding_model,
        "args": vars(args),
        "planner_sample": asdict(planner_sample),
        "matches": [asdict(match) for match in matches],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown_summary(payload), encoding="utf-8")
    return json_path, markdown_path


def render_markdown_summary(payload: Dict[str, Any]) -> str:
    """渲染 Markdown 摘要。"""

    sample = payload["planner_sample"]
    lines = [
        "# Planner 推理表达召回",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- Embedding 模型：{payload['embedding_model']}",
        f"- 来源：{sample['source']}",
        "",
        "## Planner Query",
        "",
        sample["query_text"],
        "",
        "## 召回表达",
        "",
    ]
    for index, match in enumerate(payload["matches"], start=1):
        lines.append(
            f"{index}. sim={match['similarity']} cluster={match['cluster_id']} "
            f"id={match['id']} count={match['count']} | "
            f"当「{match['situation']}」时，用「{match['style']}」"
        )
    lines.append("")
    return "\n".join(lines)


async def main() -> None:
    """脚本入口。"""

    args = parse_args()
    analysis_path = resolve_path(args.analysis_json)
    planner_sample = load_planner_reason_sample(args)
    candidates = load_expression_candidates(analysis_path)
    if not candidates:
        raise ValueError(f"没有从 {analysis_path} 读取到表达候选")
    if not normalize_text(planner_sample.query_text):
        raise ValueError("planner query 为空")

    texts = [candidate_text(candidate) for candidate in candidates]
    embeddings, embedding_model = await embed_texts(
        [*texts, planner_sample.query_text],
        max_concurrent=int(args.max_concurrent),
    )
    normalized_embeddings = l2_normalize(embeddings)
    matches = rank_candidates(
        candidates=candidates,
        candidate_vectors=normalized_embeddings[:-1],
        query_vector=normalized_embeddings[-1],
        top_k=int(args.top_k),
    )
    json_path, markdown_path = write_outputs(
        args=args,
        analysis_path=analysis_path,
        planner_sample=planner_sample,
        matches=matches,
        embedding_model=embedding_model,
    )
    print(f"分析文件: {analysis_path}")
    print(f"Planner 来源: {planner_sample.source}")
    print("召回表达:")
    for match in matches[: min(8, len(matches))]:
        print(
            f"- sim={match.similarity} cluster={match.cluster_id} id={match.id} "
            f"situation={match.situation} style={match.style}"
        )
    print(f"JSON 输出: {json_path}")
    print(f"Markdown 输出: {markdown_path}")


if __name__ == "__main__":
    asyncio.run(main())
