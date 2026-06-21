"""构建 replyer 表达方式向量召回索引。

示例：
    uv run python code_scripts/build_expression_vector_index.py
    uv run python code_scripts/build_expression_vector_index.py --checked-only --clusters 80
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from sys import path as sys_path
from typing import Any, List, Optional, Sequence

import asyncio
import json
import math
import sys

import numpy as np

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

INDEX_VERSION = 1
DEFAULT_OUTPUT_JSON = "data/expression_selection/expression_vector_index.json"


@dataclass(frozen=True)
class ExpressionIndexSample:
    """参与向量索引的一条表达方式。"""

    id: int
    situation: str
    style: str
    count: int
    session_id: Optional[str]
    checked: bool
    modified_by: str


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = ArgumentParser(description="构建 replyer 表达方式向量召回索引。")
    parser.add_argument("--output-json", default=DEFAULT_OUTPUT_JSON, help="输出索引 JSON 路径。")
    parser.add_argument("--source-analysis-json", default="", help="复用已有表达分析 JSON 样本。")
    parser.add_argument("--embedding-cache", default="", help="复用候选 embedding npz；需要包含 ids/embeddings/model_name。")
    parser.add_argument("--limit", type=int, default=0, help="最多纳入多少条表达；0 表示全部。")
    parser.add_argument("--clusters", type=int, default=80, help="聚类数量；0 表示按样本量自动选择。")
    parser.add_argument("--seed", type=int, default=20260621, help="聚类初始化随机种子。")
    parser.add_argument("--session-id", default="", help="只构建指定 session_id 的表达索引；为空则不限制。")
    parser.add_argument("--checked-only", action="store_true", help="只纳入人工审核通过的表达。")
    parser.add_argument(
        "--user-modified-only",
        action="store_true",
        help="与 --checked-only 配合，只纳入用户人工修改/确认的表达。",
    )
    parser.add_argument("--max-concurrent", type=int, default=3, help="embedding 最大并发数。")
    return parser


def parse_args() -> Namespace:
    """解析命令行参数。"""

    return build_argument_parser().parse_args()


def normalize_text(value: Any) -> str:
    """压缩空白并去除首尾空白。"""

    return " ".join(str(value or "").split()).strip()


def resolve_output_path(raw_path: str) -> Path:
    """解析输出路径。"""

    output_path = Path(normalize_text(raw_path) or DEFAULT_OUTPUT_JSON).expanduser()
    if not output_path.is_absolute():
        output_path = ROOT_PATH / output_path
    return output_path.resolve()


def resolve_input_path(raw_path: str) -> Path:
    """解析输入路径。"""

    input_path = Path(normalize_text(raw_path)).expanduser()
    if not input_path.is_absolute():
        input_path = ROOT_PATH / input_path
    return input_path.resolve()


def expression_fingerprint(expression_id: int, situation: str, style: str) -> str:
    """生成用于判断索引是否仍匹配当前表达内容的指纹。"""

    raw_text = f"{int(expression_id)}\n{normalize_text(situation)}\n{normalize_text(style)}"
    return sha256(raw_text.encode("utf-8")).hexdigest()


def expression_embedding_text(situation: str, style: str) -> str:
    """构建表达方式候选的 embedding 文本。"""

    return f"情景：{normalize_text(situation)}\n风格：{normalize_text(style)}"


def load_analysis_samples(raw_path: str, *, limit: int) -> List[ExpressionIndexSample]:
    """从 situation analysis JSON 读取样本。"""

    analysis_path = resolve_input_path(raw_path)
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    raw_samples = payload.get("samples")
    if not isinstance(raw_samples, list):
        raise ValueError(f"analysis JSON 缺少 samples: {analysis_path}")

    samples: List[ExpressionIndexSample] = []
    seen_ids: set[int] = set()
    for raw_sample in raw_samples:
        if not isinstance(raw_sample, dict):
            continue
        expression_id = int(raw_sample.get("id") or 0)
        situation = normalize_text(raw_sample.get("situation"))
        style = normalize_text(raw_sample.get("style"))
        if expression_id <= 0 or not situation or not style or expression_id in seen_ids:
            continue
        seen_ids.add(expression_id)
        samples.append(
            ExpressionIndexSample(
                id=expression_id,
                situation=situation,
                style=style,
                count=int(raw_sample.get("count") or 0),
                session_id=str(raw_sample.get("session_id")) if raw_sample.get("session_id") is not None else None,
                checked=bool(raw_sample.get("checked")),
                modified_by=normalize_text(raw_sample.get("modified_by")),
            )
        )
        if limit > 0 and len(samples) >= limit:
            break
    return samples


def load_expression_samples(args: Namespace) -> List[ExpressionIndexSample]:
    """从 expressions 表读取待索引表达。"""

    if normalize_text(args.source_analysis_json):
        return load_analysis_samples(str(args.source_analysis_json), limit=max(0, int(args.limit)))

    from sqlalchemy import func
    from sqlmodel import select

    from src.common.database.database import get_db_session
    from src.common.database.database_model import Expression, ModifiedBy

    session_id = normalize_text(args.session_id)
    with get_db_session(auto_commit=False) as session:
        statement = (
            select(
                Expression.id,
                Expression.situation,
                Expression.style,
                Expression.count,
                Expression.session_id,
                Expression.checked,
                Expression.modified_by,
            )
            .where(Expression.situation.is_not(None))  # type: ignore[attr-defined]
            .where(Expression.style.is_not(None))  # type: ignore[attr-defined]
            .where(func.length(Expression.situation) > 0)
            .where(func.length(Expression.style) > 0)
            .order_by(Expression.id.asc())
        )
        if session_id:
            statement = statement.where(Expression.session_id == session_id)
        if args.checked_only:
            statement = statement.where(Expression.checked.is_(True))  # type: ignore[attr-defined]
        if args.user_modified_only:
            statement = statement.where(Expression.modified_by == ModifiedBy.USER)
        if int(args.limit) > 0:
            statement = statement.limit(int(args.limit))
        rows = session.exec(statement).all()

    samples: List[ExpressionIndexSample] = []
    seen_ids: set[int] = set()
    for row in rows:
        expression_id, raw_situation, raw_style, raw_count, raw_session_id, raw_checked, raw_modified_by = row
        item_id = int(expression_id or 0)
        situation = normalize_text(raw_situation)
        style = normalize_text(raw_style)
        if item_id <= 0 or not situation or not style or item_id in seen_ids:
            continue
        modified_by = raw_modified_by.value if isinstance(raw_modified_by, ModifiedBy) else str(raw_modified_by or "")
        seen_ids.add(item_id)
        samples.append(
            ExpressionIndexSample(
                id=item_id,
                situation=situation,
                style=style,
                count=int(raw_count or 0),
                session_id=str(raw_session_id) if raw_session_id is not None else None,
                checked=bool(raw_checked),
                modified_by=modified_by,
            )
        )
    return samples


async def embed_expressions(samples: Sequence[ExpressionIndexSample], *, max_concurrent: int) -> tuple[np.ndarray, str]:
    """批量生成表达方式向量。"""

    from src.services.embedding_service import EmbeddingServiceClient

    client = EmbeddingServiceClient(task_name="embedding", request_type="expression.selection.index")
    results = await client.embed_texts(
        [expression_embedding_text(sample.situation, sample.style) for sample in samples],
        max_concurrent=max(1, int(max_concurrent)),
    )
    embeddings = np.array([result.embedding for result in results], dtype=np.float32)
    if embeddings.ndim != 2 or embeddings.shape[0] != len(samples):
        raise ValueError(f"embedding 结果维度异常: shape={embeddings.shape}, samples={len(samples)}")
    model_name = results[0].model_name if results else ""
    return embeddings, model_name


def load_cached_embeddings(samples: Sequence[ExpressionIndexSample], raw_cache_path: str) -> tuple[np.ndarray, str]:
    """从候选 embedding 缓存读取并按 samples 顺序重排。"""

    cache_path = resolve_input_path(raw_cache_path)
    with np.load(cache_path) as payload:
        if "ids" not in payload or "embeddings" not in payload:
            raise ValueError(f"embedding cache 需要包含 ids/embeddings: {cache_path}")
        ids = np.array(payload["ids"], dtype=np.int64)
        embeddings = np.array(payload["embeddings"], dtype=np.float32)
        raw_model_name = payload["model_name"] if "model_name" in payload else ""
    if embeddings.ndim != 2 or embeddings.shape[0] != ids.shape[0]:
        raise ValueError(f"embedding cache 维度异常: ids={ids.shape}, embeddings={embeddings.shape}")

    embedding_by_id = {int(expression_id): embeddings[index] for index, expression_id in enumerate(ids)}
    ordered_embeddings: List[np.ndarray] = []
    missing_ids: List[int] = []
    for sample in samples:
        embedding = embedding_by_id.get(sample.id)
        if embedding is None:
            missing_ids.append(sample.id)
            continue
        ordered_embeddings.append(embedding)
    if missing_ids:
        raise ValueError(f"embedding cache 缺少表达 ID: {missing_ids[:10]}，缺失数={len(missing_ids)}")

    model_name = str(raw_model_name.item()) if hasattr(raw_model_name, "item") else str(raw_model_name or "")
    return np.vstack(ordered_embeddings).astype(np.float32), model_name


def l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """按行执行 L2 归一化。"""

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= 0):
        raise ValueError("存在零向量 embedding，无法构建表达索引")
    return matrix / norms


def choose_cluster_count(sample_count: int, requested_clusters: int) -> int:
    """解析聚类数量。"""

    if sample_count < 2:
        return 1
    if requested_clusters > 0:
        return max(1, min(int(requested_clusters), sample_count))
    return max(2, min(80, int(round(math.sqrt(sample_count * 2.0)))))


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


def build_cluster_centers(normalized_vectors: np.ndarray, labels: np.ndarray, cluster_count: int) -> np.ndarray:
    """根据聚类标签计算中心向量。"""

    centers: List[np.ndarray] = []
    for cluster_id in range(cluster_count):
        member_vectors = normalized_vectors[labels == cluster_id]
        if len(member_vectors) == 0:
            raise ValueError(f"聚类 {cluster_id} 没有成员")
        center = member_vectors.mean(axis=0)
        norm = float(np.linalg.norm(center))
        if norm <= 0:
            raise ValueError(f"聚类 {cluster_id} 中心向量为零")
        centers.append((center / norm).astype(np.float32))
    return np.vstack(centers).astype(np.float32)


def build_cluster_summaries(samples: Sequence[ExpressionIndexSample], labels: np.ndarray) -> List[dict[str, Any]]:
    """生成轻量聚类摘要。"""

    summaries: List[dict[str, Any]] = []
    for cluster_id in sorted(set(int(label) for label in labels)):
        member_indices = [index for index, label in enumerate(labels) if int(label) == cluster_id]
        top_members = [
            {
                "id": samples[index].id,
                "situation": samples[index].situation,
                "style": samples[index].style,
                "count": samples[index].count,
            }
            for index in member_indices[:8]
        ]
        summaries.append(
            {
                "cluster_id": cluster_id,
                "size": len(member_indices),
                "members": top_members,
            }
        )
    return sorted(summaries, key=lambda item: int(item["size"]), reverse=True)


def write_index(
    *,
    output_json: Path,
    samples: Sequence[ExpressionIndexSample],
    vectors: np.ndarray,
    cluster_centers: np.ndarray,
    labels: np.ndarray,
    embedding_model: str,
    args: Namespace,
) -> None:
    """写入索引 JSON 与向量 npz。"""

    from src.common.database.database import DATABASE_URL

    output_json.parent.mkdir(parents=True, exist_ok=True)
    vectors_path = output_json.with_suffix(".npz")
    np.savez_compressed(
        vectors_path,
        vectors=vectors.astype(np.float32),
        cluster_centers=cluster_centers.astype(np.float32),
    )
    expressions: List[dict[str, Any]] = []
    for index, sample in enumerate(samples):
        item = asdict(sample)
        item["fingerprint"] = expression_fingerprint(sample.id, sample.situation, sample.style)
        item["cluster_id"] = int(labels[index])
        expressions.append(item)

    payload = {
        "version": INDEX_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_url": DATABASE_URL,
        "embedding_model": embedding_model,
        "embedding_dimension": int(vectors.shape[1]),
        "sample_count": len(samples),
        "clusters": build_cluster_summaries(samples, labels),
        "vectors_file": vectors_path.name,
        "args": {
            "limit": int(args.limit),
            "clusters": int(args.clusters),
            "seed": int(args.seed),
            "session_id": normalize_text(args.session_id),
            "checked_only": bool(args.checked_only),
            "user_modified_only": bool(args.user_modified_only),
            "max_concurrent": int(args.max_concurrent),
            "source_analysis_json": normalize_text(args.source_analysis_json),
            "embedding_cache": normalize_text(args.embedding_cache),
        },
        "expressions": expressions,
    }
    output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main_async() -> None:
    """执行索引构建。"""

    args = parse_args()
    output_json = resolve_output_path(str(args.output_json))
    samples = load_expression_samples(args)
    if len(samples) < 10:
        raise ValueError(f"可索引表达数量不足: {len(samples)}")

    if normalize_text(args.embedding_cache):
        embeddings, model_name = load_cached_embeddings(samples, str(args.embedding_cache))
    else:
        embeddings, model_name = await embed_expressions(samples, max_concurrent=int(args.max_concurrent))
    vectors = l2_normalize(embeddings).astype(np.float32)
    cluster_count = choose_cluster_count(len(samples), int(args.clusters))
    labels = run_kmeans(vectors, cluster_count=cluster_count, seed=int(args.seed))
    cluster_centers = build_cluster_centers(vectors, labels, cluster_count)
    write_index(
        output_json=output_json,
        samples=samples,
        vectors=vectors,
        cluster_centers=cluster_centers,
        labels=labels,
        embedding_model=model_name,
        args=args,
    )
    print(f"已构建表达向量索引: {output_json}")
    print(f"表达数: {len(samples)}, 聚类数: {cluster_count}, embedding模型: {model_name}")


def main() -> None:
    """脚本入口。"""

    asyncio.run(main_async())


if __name__ == "__main__":
    main()
