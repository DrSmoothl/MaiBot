"""对表达方式 situation 做向量化、聚类和探索性因子分析。

示例：
    uv run python code_scripts/analyze_expression_situations.py
    uv run python code_scripts/analyze_expression_situations.py --limit 50 --clusters 5 --factors 4
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from random import Random
from sys import path as sys_path
from typing import Any, Dict, List, Optional, Sequence

import argparse
import asyncio
import json
import math
import sys

import numpy as np
from sqlalchemy import func
from sqlmodel import select

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from src.common.database.database import DATABASE_URL, get_db_session  # noqa: E402
from src.common.database.database_model import Expression  # noqa: E402
from src.services.embedding_service import EmbeddingServiceClient  # noqa: E402


@dataclass(frozen=True)
class ExpressionSituationSample:
    """参与分析的一条表达方式情境样本。"""

    id: int
    situation: str
    style: str
    count: int
    session_id: Optional[str]
    checked: bool
    last_active_time: str


@dataclass(frozen=True)
class ClusterSummary:
    """聚类摘要。"""

    cluster_id: int
    size: int
    representative_id: int
    representative_situation: str
    average_similarity_to_center: float
    members: List[Dict[str, Any]]


@dataclass(frozen=True)
class FactorSummary:
    """探索性因子摘要。"""

    factor_id: int
    explained_variance_ratio: float
    positive_items: List[Dict[str, Any]]
    negative_items: List[Dict[str, Any]]


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="抽样分析 expressions.situation 的 embedding 聚类与 PCA 因子。")
    parser.add_argument("--limit", type=int, default=50, help="抽样数量，默认 50。")
    parser.add_argument("--clusters", type=int, default=0, help="聚类数量；0 表示按样本量自动选择。")
    parser.add_argument("--factors", type=int, default=4, help="输出的 PCA 因子数量，默认 4。")
    parser.add_argument("--sample", choices=["random", "recent", "count"], default="random", help="抽样方式。")
    parser.add_argument("--seed", type=int, default=42, help="随机种子，影响 random 抽样和 k-means 初始化。")
    parser.add_argument("--session-id", default="", help="只分析指定 session_id 的表达方式；为空则不限制。")
    parser.add_argument("--checked-only", action="store_true", help="只分析人工审核通过的表达方式。")
    parser.add_argument("--max-concurrent", type=int, default=3, help="embedding 最大并发数，默认 3。")
    parser.add_argument("--output-dir", default="data/analysis", help="输出目录，默认 data/analysis。")
    parser.add_argument("--baseline-json", default="", help="先纳入已有分析 JSON 中的 samples，再补足到 limit。")
    return parser


def parse_args() -> Namespace:
    """解析命令行参数。"""

    return build_argument_parser().parse_args()


def normalize_text(value: Any) -> str:
    """压缩空白并去除首尾空白。"""

    return " ".join(str(value or "").split()).strip()


def load_expression_samples(args: Namespace) -> List[ExpressionSituationSample]:
    """从 expressions 表读取待分析的 situation 样本。"""

    limit = max(1, int(args.limit))
    session_id = normalize_text(args.session_id)
    baseline_samples = load_baseline_samples(args.baseline_json)
    baseline_ids = {sample.id for sample in baseline_samples}
    with get_db_session(auto_commit=False) as session:
        statement = (
            select(
                Expression.id,
                Expression.situation,
                Expression.style,
                Expression.count,
                Expression.checked,
            )
            .where(Expression.situation.is_not(None))  # type: ignore[attr-defined]
            .where(func.length(Expression.situation) > 0)
        )
        if session_id:
            statement = statement.where(Expression.session_id == session_id)
        if args.checked_only:
            statement = statement.where(Expression.checked.is_(True))  # type: ignore[attr-defined]

        if args.sample == "recent":
            statement = statement.order_by(Expression.last_active_time.desc(), Expression.id.desc())
        elif args.sample == "count":
            statement = statement.order_by(Expression.count.desc(), Expression.last_active_time.desc())
        else:
            statement = statement.order_by(Expression.id.asc())

        if args.sample != "random" and not baseline_samples:
            statement = statement.limit(limit)
        rows = session.exec(statement).all()

    samples: List[ExpressionSituationSample] = list(baseline_samples)
    seen_situations = {sample.situation for sample in samples}
    candidate_samples: List[ExpressionSituationSample] = []
    for row in rows:
        row_id, raw_situation, raw_style, raw_count, raw_checked = row
        expression_id = int(row_id or 0)
        if expression_id in baseline_ids:
            continue
        situation = normalize_text(raw_situation)
        if not situation or situation in seen_situations:
            continue
        seen_situations.add(situation)
        candidate_samples.append(
            ExpressionSituationSample(
                id=expression_id,
                situation=situation,
                style=normalize_text(raw_style),
                count=int(raw_count or 0),
                session_id=None,
                checked=bool(raw_checked),
                last_active_time="",
            )
        )
    if args.sample == "random":
        Random(int(args.seed)).shuffle(candidate_samples)
    samples.extend(candidate_samples[: max(0, limit - len(samples))])
    return samples[:limit]


def load_baseline_samples(raw_path: str) -> List[ExpressionSituationSample]:
    """读取已有分析结果中的样本，作为扩展分析的固定基线。"""

    normalized_path = normalize_text(raw_path)
    if not normalized_path:
        return []

    baseline_path = Path(normalized_path).expanduser()
    if not baseline_path.is_absolute():
        baseline_path = ROOT_PATH / baseline_path
    payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    raw_samples = payload.get("samples")
    if not isinstance(raw_samples, list):
        raise ValueError(f"baseline-json 缺少 samples 列表: {baseline_path}")

    samples: List[ExpressionSituationSample] = []
    seen_ids: set[int] = set()
    seen_situations: set[str] = set()
    for raw_sample in raw_samples:
        if not isinstance(raw_sample, dict):
            continue
        expression_id = int(raw_sample.get("id") or 0)
        situation = normalize_text(raw_sample.get("situation"))
        if expression_id <= 0 or not situation:
            continue
        if expression_id in seen_ids or situation in seen_situations:
            continue
        seen_ids.add(expression_id)
        seen_situations.add(situation)
        samples.append(
            ExpressionSituationSample(
                id=expression_id,
                situation=situation,
                style=normalize_text(raw_sample.get("style")),
                count=int(raw_sample.get("count") or 0),
                session_id=None,
                checked=bool(raw_sample.get("checked")),
                last_active_time=normalize_text(raw_sample.get("last_active_time")),
            )
        )
    return samples


async def embed_situations(samples: Sequence[ExpressionSituationSample], *, max_concurrent: int) -> tuple[np.ndarray, str]:
    """使用项目统一 EmbeddingServiceClient 生成 situation 向量。"""

    client = EmbeddingServiceClient(task_name="embedding", request_type="expression.situation_analysis")
    results = await client.embed_texts(
        [sample.situation for sample in samples],
        max_concurrent=max(1, int(max_concurrent)),
    )
    embeddings = np.array([result.embedding for result in results], dtype=np.float32)
    if embeddings.ndim != 2 or embeddings.shape[0] != len(samples):
        raise ValueError(f"embedding 结果维度异常: shape={embeddings.shape}, samples={len(samples)}")
    model_name = results[0].model_name if results else ""
    return embeddings, model_name


def l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """按行做 L2 归一化。"""

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= 0):
        raise ValueError("存在零向量 embedding，无法进行余弦聚类")
    return matrix / norms


def choose_cluster_count(sample_count: int, requested_clusters: int) -> int:
    """解析聚类数量。"""

    if sample_count < 2:
        return 1
    if requested_clusters > 0:
        return max(1, min(int(requested_clusters), sample_count))
    return max(2, min(8, int(round(math.sqrt(sample_count / 2.0)))))


def run_kmeans(normalized_vectors: np.ndarray, *, cluster_count: int, seed: int, max_iter: int = 100) -> np.ndarray:
    """在归一化向量上执行确定性 cosine k-means。"""

    sample_count = normalized_vectors.shape[0]
    if cluster_count <= 1 or sample_count <= 1:
        return np.zeros(sample_count, dtype=np.int32)

    rng = np.random.default_rng(seed)
    centroid_indices = [int(rng.integers(0, sample_count))]
    while len(centroid_indices) < cluster_count:
        selected = normalized_vectors[centroid_indices]
        similarity = normalized_vectors @ selected.T
        distance = 1.0 - np.max(similarity, axis=1)
        distance[centroid_indices] = 0.0
        total_distance = float(distance.sum())
        if total_distance <= 0:
            remaining_indices = [index for index in range(sample_count) if index not in centroid_indices]
            centroid_indices.append(remaining_indices[0])
            continue
        probabilities = distance / total_distance
        centroid_indices.append(int(rng.choice(sample_count, p=probabilities)))

    centroids = normalized_vectors[centroid_indices].copy()
    labels = np.zeros(sample_count, dtype=np.int32)
    for _ in range(max_iter):
        next_labels = np.argmax(normalized_vectors @ centroids.T, axis=1).astype(np.int32)
        if np.array_equal(next_labels, labels):
            break
        labels = next_labels
        for cluster_index in range(cluster_count):
            member_vectors = normalized_vectors[labels == cluster_index]
            if len(member_vectors) == 0:
                farthest_index = int(np.argmin(np.max(normalized_vectors @ centroids.T, axis=1)))
                centroids[cluster_index] = normalized_vectors[farthest_index]
                labels[farthest_index] = cluster_index
                continue
            centroid = member_vectors.mean(axis=0)
            norm = float(np.linalg.norm(centroid))
            if norm <= 0:
                raise ValueError(f"聚类 {cluster_index} 中心向量为零")
            centroids[cluster_index] = centroid / norm
    return labels


def summarize_clusters(
    samples: Sequence[ExpressionSituationSample],
    normalized_vectors: np.ndarray,
    labels: np.ndarray,
) -> List[ClusterSummary]:
    """生成聚类摘要。"""

    summaries: List[ClusterSummary] = []
    for cluster_id in sorted(set(int(label) for label in labels)):
        member_indices = [index for index, label in enumerate(labels) if int(label) == cluster_id]
        member_vectors = normalized_vectors[member_indices]
        centroid = member_vectors.mean(axis=0)
        centroid = centroid / np.linalg.norm(centroid)
        similarities = member_vectors @ centroid
        representative_local_index = int(np.argmax(similarities))
        representative_index = member_indices[representative_local_index]
        ordered_members = sorted(
            [
                {
                    "id": samples[index].id,
                    "situation": samples[index].situation,
                    "style": samples[index].style,
                    "count": samples[index].count,
                    "similarity_to_center": round(float(similarities[member_indices.index(index)]), 4),
                }
                for index in member_indices
            ],
            key=lambda item: item["similarity_to_center"],
            reverse=True,
        )
        summaries.append(
            ClusterSummary(
                cluster_id=cluster_id,
                size=len(member_indices),
                representative_id=samples[representative_index].id,
                representative_situation=samples[representative_index].situation,
                average_similarity_to_center=round(float(np.mean(similarities)), 4),
                members=ordered_members,
            )
        )
    return sorted(summaries, key=lambda item: item.size, reverse=True)


def run_pca_factor_summary(
    samples: Sequence[ExpressionSituationSample],
    vectors: np.ndarray,
    *,
    factor_count: int,
    top_items: int = 5,
) -> tuple[List[FactorSummary], List[float]]:
    """用 PCA 做探索性因子摘要，输出每个因子的正负两端代表项。"""

    if vectors.shape[0] < 2:
        return [], []

    centered = vectors - vectors.mean(axis=0, keepdims=True)
    _, singular_values, vt = np.linalg.svd(centered, full_matrices=False)
    max_factors = min(max(1, int(factor_count)), vt.shape[0], vectors.shape[0] - 1)
    components = vt[:max_factors]
    scores = centered @ components.T
    variance = singular_values**2
    total_variance = float(variance.sum())
    ratios = [float(value / total_variance) if total_variance > 0 else 0.0 for value in variance[:max_factors]]

    summaries: List[FactorSummary] = []
    for factor_index in range(max_factors):
        factor_scores = scores[:, factor_index]
        positive_indices = np.argsort(factor_scores)[::-1][:top_items]
        negative_indices = np.argsort(factor_scores)[:top_items]
        summaries.append(
            FactorSummary(
                factor_id=factor_index + 1,
                explained_variance_ratio=round(ratios[factor_index], 4),
                positive_items=[factor_item(samples, factor_scores, int(index)) for index in positive_indices],
                negative_items=[factor_item(samples, factor_scores, int(index)) for index in negative_indices],
            )
        )
    return summaries, [round(ratio, 4) for ratio in ratios]


def factor_item(
    samples: Sequence[ExpressionSituationSample],
    scores: np.ndarray,
    index: int,
) -> Dict[str, Any]:
    """构建因子代表项。"""

    return {
        "id": samples[index].id,
        "score": round(float(scores[index]), 4),
        "situation": samples[index].situation,
        "style": samples[index].style,
        "count": samples[index].count,
    }


def write_outputs(
    *,
    args: Namespace,
    samples: Sequence[ExpressionSituationSample],
    embeddings: np.ndarray,
    embedding_model: str,
    clusters: Sequence[ClusterSummary],
    factors: Sequence[FactorSummary],
    variance_ratios: Sequence[float],
) -> tuple[Path, Path]:
    """写出 JSON 明细和 Markdown 摘要。"""

    output_dir = Path(str(args.output_dir)).expanduser()
    if not output_dir.is_absolute():
        output_dir = ROOT_PATH / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"expression_situation_analysis_{timestamp}.json"
    markdown_path = output_dir / f"expression_situation_analysis_{timestamp}.md"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_url": DATABASE_URL,
        "embedding_model": embedding_model,
        "sample_count": len(samples),
        "embedding_dimension": int(embeddings.shape[1]),
        "args": vars(args),
        "samples": [asdict(sample) for sample in samples],
        "clusters": [asdict(cluster) for cluster in clusters],
        "factors": [asdict(factor) for factor in factors],
        "factor_explained_variance_ratios": list(variance_ratios),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown_summary(payload), encoding="utf-8")
    return json_path, markdown_path


def render_markdown_summary(payload: Dict[str, Any]) -> str:
    """渲染 Markdown 摘要。"""

    lines = [
        "# 表达方式 situation 向量分析",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- 样本数：{payload['sample_count']}",
        f"- 向量维度：{payload['embedding_dimension']}",
        f"- Embedding 模型：{payload['embedding_model']}",
        f"- 抽样方式：{payload['args']['sample']}",
        "",
        "## 聚类结果",
    ]

    for cluster in payload["clusters"]:
        lines.extend(
            [
                "",
                (
                    f"### Cluster {cluster['cluster_id']} "
                    f"(n={cluster['size']}, avg_sim={cluster['average_similarity_to_center']})"
                ),
                f"代表情境：{cluster['representative_situation']}",
                "",
            ]
        )
        for member in cluster["members"][:8]:
            lines.append(
                f"- {member['situation']} | style={member['style']} | "
                f"count={member['count']} | sim={member['similarity_to_center']}"
            )

    lines.extend(["", "## PCA 因子摘要"])
    for factor in payload["factors"]:
        lines.extend(
            [
                "",
                f"### Factor {factor['factor_id']} (explained={factor['explained_variance_ratio']})",
                "",
                "正向代表：",
            ]
        )
        for item in factor["positive_items"]:
            lines.append(f"- {item['situation']} | style={item['style']} | score={item['score']}")
        lines.append("")
        lines.append("反向代表：")
        for item in factor["negative_items"]:
            lines.append(f"- {item['situation']} | style={item['style']} | score={item['score']}")

    lines.append("")
    return "\n".join(lines)


async def main() -> None:
    """脚本入口。"""

    args = parse_args()
    samples = load_expression_samples(args)
    if len(samples) < 2:
        raise SystemExit(f"可分析的表达方式不足：{len(samples)} 条。")

    print(f"数据库: {DATABASE_URL}")
    print(f"读取样本: {len(samples)} 条")
    embeddings, embedding_model = await embed_situations(samples, max_concurrent=args.max_concurrent)
    print(f"向量化完成: model={embedding_model}, shape={embeddings.shape}")

    normalized_vectors = l2_normalize(embeddings)
    cluster_count = choose_cluster_count(len(samples), int(args.clusters))
    labels = run_kmeans(normalized_vectors, cluster_count=cluster_count, seed=int(args.seed))
    clusters = summarize_clusters(samples, normalized_vectors, labels)
    factors, variance_ratios = run_pca_factor_summary(samples, embeddings, factor_count=int(args.factors))

    json_path, markdown_path = write_outputs(
        args=args,
        samples=samples,
        embeddings=embeddings,
        embedding_model=embedding_model,
        clusters=clusters,
        factors=factors,
        variance_ratios=variance_ratios,
    )
    print(f"聚类数: {cluster_count}")
    print(f"因子解释率: {variance_ratios}")
    print(f"JSON 输出: {json_path}")
    print(f"Markdown 输出: {markdown_path}")


if __name__ == "__main__":
    asyncio.run(main())
