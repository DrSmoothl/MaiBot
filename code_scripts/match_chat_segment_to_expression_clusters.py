"""随机抽取聊天片段，并匹配到已有表达情境聚类。"""

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
import sys

import numpy as np
from sqlalchemy import func
from sqlmodel import col, select

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from src.common.database.database import DATABASE_URL, get_db_session  # noqa: E402
from src.common.database.database_model import ChatSession, Messages  # noqa: E402
from src.services.embedding_service import EmbeddingServiceClient  # noqa: E402


@dataclass(frozen=True)
class CandidateSession:
    """可抽样聊天流。"""

    session_id: str
    display_name: str
    message_count: int


@dataclass(frozen=True)
class ChatSegmentMessage:
    """聊天片段中的一条消息。"""

    id: int
    timestamp: str
    speaker: str
    text: str


@dataclass(frozen=True)
class ChatSegment:
    """随机抽取的一段连续聊天记录。"""

    session_id: str
    display_name: str
    start_index: int
    messages: List[ChatSegmentMessage]


@dataclass(frozen=True)
class ClusterMatch:
    """聊天片段与表达情境簇的匹配结果。"""

    cluster_id: int
    similarity: float
    size: int
    representative_situation: str
    average_similarity_to_center: float
    closest_members: List[Dict[str, Any]]


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="随机抽取聊天片段，并匹配到已有表达情境聚类。")
    parser.add_argument(
        "--analysis-json",
        default="data/analysis/expression_situation_analysis_20260621_111009.json",
        help="已有表达情境聚类 JSON。",
    )
    parser.add_argument("--window-size", type=int, default=12, help="随机聊天片段长度，默认 12 条消息。")
    parser.add_argument("--min-text-length", type=int, default=2, help="参与抽样的消息最短文本长度。")
    parser.add_argument("--seed", type=int, default=42, help="随机种子。")
    parser.add_argument("--top-k", type=int, default=5, help="输出最接近的簇数量。")
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


def load_analysis_payload(path: Path) -> Dict[str, Any]:
    """读取已有聚类分析结果。"""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("clusters"), list):
        raise ValueError(f"分析文件缺少 clusters: {path}")
    return payload


def find_candidate_sessions(*, min_text_length: int, window_size: int) -> List[CandidateSession]:
    """读取有足够连续文本消息的聊天流候选。"""

    with get_db_session(auto_commit=False) as session:
        rows = session.exec(
            select(Messages.session_id, func.count(Messages.id))
            .where(Messages.processed_plain_text.is_not(None))
            .where(func.length(Messages.processed_plain_text) >= min_text_length)
            .group_by(Messages.session_id)
            .having(func.count(Messages.id) >= window_size)
        ).all()
        session_ids = [str(row[0]) for row in rows if normalize_text(row[0])]
        chat_sessions = {
            item.session_id: item
            for item in session.exec(
                select(ChatSession).where(col(ChatSession.session_id).in_(session_ids))
            ).all()
        }

    return [
        CandidateSession(
            session_id=str(session_id),
            display_name=build_session_display_name(chat_sessions.get(str(session_id)), str(session_id)),
            message_count=int(message_count),
        )
        for session_id, message_count in rows
        if normalize_text(session_id)
    ]


def build_session_display_name(chat_session: Optional[ChatSession], fallback_session_id: str) -> str:
    """构建聊天流展示名。"""

    if chat_session is None:
        return fallback_session_id
    if chat_session.group_id:
        return chat_session.group_name or f"群聊{chat_session.group_id}"
    if chat_session.user_id:
        private_name = chat_session.user_cardname or chat_session.user_nickname or f"用户{chat_session.user_id}"
        return f"{private_name}的私聊"
    return fallback_session_id


def sample_chat_segment(
    *,
    seed: int,
    window_size: int,
    min_text_length: int,
) -> ChatSegment:
    """随机抽取一段连续聊天记录。"""

    rng = Random(seed)
    candidates = find_candidate_sessions(min_text_length=min_text_length, window_size=window_size)
    if not candidates:
        raise ValueError(f"没有找到至少 {window_size} 条文本消息的聊天流")

    target_session = rng.choice(candidates)
    with get_db_session(auto_commit=False) as session:
        rows = session.exec(
            select(
                Messages.id,
                Messages.timestamp,
                Messages.user_nickname,
                Messages.user_cardname,
                Messages.processed_plain_text,
            )
            .where(Messages.session_id == target_session.session_id)
            .where(Messages.processed_plain_text.is_not(None))
            .where(func.length(Messages.processed_plain_text) >= min_text_length)
            .order_by(Messages.timestamp.asc(), Messages.id.asc())
        ).all()

    if len(rows) < window_size:
        raise ValueError(f"聊天流消息不足：{target_session.session_id} count={len(rows)}")

    start_index = rng.randint(0, len(rows) - window_size)
    window_rows = rows[start_index : start_index + window_size]
    messages: List[ChatSegmentMessage] = []
    for row in window_rows:
        message_id, timestamp, user_nickname, user_cardname, processed_plain_text = row
        speaker = normalize_text(user_cardname) or normalize_text(user_nickname) or "未知用户"
        messages.append(
            ChatSegmentMessage(
                id=int(message_id or 0),
                timestamp=timestamp.isoformat(timespec="seconds") if timestamp else "",
                speaker=speaker,
                text=normalize_text(processed_plain_text),
            )
        )

    return ChatSegment(
        session_id=target_session.session_id,
        display_name=target_session.display_name,
        start_index=start_index,
        messages=messages,
    )


def render_segment_text(segment: ChatSegment) -> str:
    """将聊天片段渲染为 embedding 输入文本。"""

    lines = [f"聊天流：{segment.display_name}", "聊天记录："]
    for index, message in enumerate(segment.messages, start=1):
        lines.append(f"{index}. {message.speaker}: {message.text}")
    return "\n".join(lines)


def collect_cluster_member_texts(payload: Dict[str, Any]) -> tuple[List[str], Dict[str, Dict[str, Any]], Dict[int, List[str]]]:
    """收集聚类成员 situation 文本。"""

    text_to_member: Dict[str, Dict[str, Any]] = {}
    cluster_texts: Dict[int, List[str]] = {}
    for cluster in payload["clusters"]:
        cluster_id = int(cluster["cluster_id"])
        cluster_texts[cluster_id] = []
        members = cluster.get("members") or []
        for member in members:
            if not isinstance(member, dict):
                continue
            situation = normalize_text(member.get("situation"))
            if not situation:
                continue
            text_to_member.setdefault(situation, member)
            cluster_texts[cluster_id].append(situation)
    return list(text_to_member), text_to_member, cluster_texts


async def embed_texts(texts: Sequence[str], *, max_concurrent: int) -> tuple[np.ndarray, str]:
    """批量向量化文本。"""

    client = EmbeddingServiceClient(task_name="embedding", request_type="expression.cluster_match")
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


def build_cluster_centers(
    *,
    payload: Dict[str, Any],
    texts: Sequence[str],
    text_vectors: np.ndarray,
    cluster_texts: Dict[int, List[str]],
) -> Dict[int, np.ndarray]:
    """根据聚类成员向量构建簇中心。"""

    vector_by_text = {text: text_vectors[index] for index, text in enumerate(texts)}
    centers: Dict[int, np.ndarray] = {}
    for cluster in payload["clusters"]:
        cluster_id = int(cluster["cluster_id"])
        member_vectors = [vector_by_text[text] for text in cluster_texts.get(cluster_id, []) if text in vector_by_text]
        if not member_vectors:
            continue
        center = np.array(member_vectors, dtype=np.float32).mean(axis=0)
        center_norm = float(np.linalg.norm(center))
        if center_norm <= 0:
            continue
        centers[cluster_id] = center / center_norm
    return centers


def match_segment_to_clusters(
    *,
    payload: Dict[str, Any],
    segment_vector: np.ndarray,
    texts: Sequence[str],
    text_vectors: np.ndarray,
    text_to_member: Dict[str, Dict[str, Any]],
    cluster_texts: Dict[int, List[str]],
    centers: Dict[int, np.ndarray],
    top_k: int,
) -> List[ClusterMatch]:
    """计算聊天片段最接近的表达情境簇。"""

    vector_by_text = {text: text_vectors[index] for index, text in enumerate(texts)}
    cluster_by_id = {int(cluster["cluster_id"]): cluster for cluster in payload["clusters"]}
    scored_clusters = sorted(
        [
            (cluster_id, float(segment_vector @ center))
            for cluster_id, center in centers.items()
        ],
        key=lambda item: item[1],
        reverse=True,
    )[: max(1, int(top_k))]

    matches: List[ClusterMatch] = []
    for cluster_id, similarity in scored_clusters:
        cluster = cluster_by_id[cluster_id]
        member_scores = []
        for text in cluster_texts.get(cluster_id, []):
            vector = vector_by_text.get(text)
            if vector is None:
                continue
            member = text_to_member[text]
            member_scores.append(
                {
                    "id": member.get("id"),
                    "situation": member.get("situation"),
                    "style": member.get("style"),
                    "similarity": round(float(segment_vector @ vector), 4),
                }
            )
        member_scores.sort(key=lambda item: item["similarity"], reverse=True)
        matches.append(
            ClusterMatch(
                cluster_id=cluster_id,
                similarity=round(similarity, 4),
                size=int(cluster.get("size") or 0),
                representative_situation=normalize_text(cluster.get("representative_situation")),
                average_similarity_to_center=float(cluster.get("average_similarity_to_center") or 0.0),
                closest_members=member_scores[:5],
            )
        )
    return matches


def write_outputs(
    *,
    args: Namespace,
    analysis_path: Path,
    segment: ChatSegment,
    segment_text: str,
    matches: Sequence[ClusterMatch],
    embedding_model: str,
) -> tuple[Path, Path]:
    """写出匹配结果。"""

    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"chat_segment_expression_cluster_match_{timestamp}.json"
    markdown_path = output_dir / f"chat_segment_expression_cluster_match_{timestamp}.md"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_url": DATABASE_URL,
        "analysis_json": str(analysis_path),
        "embedding_model": embedding_model,
        "args": vars(args),
        "segment": asdict(segment),
        "segment_text": segment_text,
        "matches": [asdict(match) for match in matches],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown_summary(payload), encoding="utf-8")
    return json_path, markdown_path


def render_markdown_summary(payload: Dict[str, Any]) -> str:
    """渲染 Markdown 摘要。"""

    segment = payload["segment"]
    lines = [
        "# 聊天片段表达情境簇匹配",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- Embedding 模型：{payload['embedding_model']}",
        f"- 聊天流：{segment['display_name']}",
        f"- session_id：{segment['session_id']}",
        "",
        "## 随机聊天片段",
        "",
    ]
    for index, message in enumerate(segment["messages"], start=1):
        lines.append(f"{index}. {message['speaker']}: {message['text']}")

    lines.extend(["", "## 最接近的表达情境簇"])
    for match in payload["matches"]:
        lines.extend(
            [
                "",
                (
                    f"### Cluster {match['cluster_id']} "
                    f"(sim={match['similarity']}, n={match['size']})"
                ),
                f"代表情境：{match['representative_situation']}",
                "",
            ]
        )
        for member in match["closest_members"]:
            lines.append(
                f"- {member['situation']} | style={member['style']} | sim={member['similarity']}"
            )
    lines.append("")
    return "\n".join(lines)


async def main() -> None:
    """脚本入口。"""

    args = parse_args()
    analysis_path = resolve_path(args.analysis_json)
    payload = load_analysis_payload(analysis_path)
    segment = sample_chat_segment(
        seed=int(args.seed),
        window_size=max(2, int(args.window_size)),
        min_text_length=max(1, int(args.min_text_length)),
    )
    segment_text = render_segment_text(segment)
    texts, text_to_member, cluster_texts = collect_cluster_member_texts(payload)
    if not texts:
        raise ValueError("没有可用于构建簇中心的聚类成员")

    all_texts = [*texts, segment_text]
    embeddings, embedding_model = await embed_texts(all_texts, max_concurrent=int(args.max_concurrent))
    normalized_embeddings = l2_normalize(embeddings)
    text_vectors = normalized_embeddings[:-1]
    segment_vector = normalized_embeddings[-1]
    centers = build_cluster_centers(
        payload=payload,
        texts=texts,
        text_vectors=text_vectors,
        cluster_texts=cluster_texts,
    )
    matches = match_segment_to_clusters(
        payload=payload,
        segment_vector=segment_vector,
        texts=texts,
        text_vectors=text_vectors,
        text_to_member=text_to_member,
        cluster_texts=cluster_texts,
        centers=centers,
        top_k=int(args.top_k),
    )
    json_path, markdown_path = write_outputs(
        args=args,
        analysis_path=analysis_path,
        segment=segment,
        segment_text=segment_text,
        matches=matches,
        embedding_model=embedding_model,
    )

    print(f"数据库: {DATABASE_URL}")
    print(f"分析文件: {analysis_path}")
    print(f"随机聊天流: {segment.display_name} ({segment.session_id})")
    print(f"片段消息数: {len(segment.messages)}")
    print("最接近的簇:")
    for match in matches:
        print(
            f"- Cluster {match.cluster_id}: sim={match.similarity} "
            f"n={match.size} representative={match.representative_situation}"
        )
    print(f"JSON 输出: {json_path}")
    print(f"Markdown 输出: {markdown_path}")


if __name__ == "__main__":
    asyncio.run(main())
