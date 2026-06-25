"""LLM 表达选择盲评器。

读取离线运行器产出的 batch JSON，把三种表达选择方案稳定随机映射为
A/B/C，只把盲化后的候选交给 LLM 评审，并在结果中回填真实方法。
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from sys import path as sys_path
from typing import Any, List, Sequence

import argparse
import asyncio
import json
import sys

from json_repair import repair_json

ROOT_PATH = Path(__file__).resolve().parents[2]
if str(ROOT_PATH) not in sys_path:
    sys_path.insert(0, str(ROOT_PATH))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from src.common.data_models.llm_service_data_models import LLMGenerationOptions  # noqa: E402
from src.services.llm_service import LLMServiceClient  # noqa: E402

DEFAULT_METHODS = ["old_direct", "precise_selection", "vector_recall"]
BLIND_LABELS = ["A", "B", "C"]


@dataclass(frozen=True)
class AutoJudgeResult:
    """单条样本的自动盲评结果。"""

    sample_id: str
    target_text: str
    actual_reply: str
    label_to_method: dict[str, str]
    method_to_label: dict[str, str]
    raw_response: str
    ranking_labels: List[str]
    ranking_methods: List[str]
    special: str
    scores_by_label: dict[str, int]
    scores_by_method: dict[str, int]
    reason: str
    model_name: str
    prompt_tokens: int
    completion_tokens: int


def build_argument_parser() -> ArgumentParser:
    """构建命令行参数解析器。"""

    parser = argparse.ArgumentParser(description="自动盲评表达选择三方案。")
    parser.add_argument("--input-json", default="", help="batch compare JSON；为空时使用最新一份。")
    parser.add_argument("--limit", type=int, default=0, help="最多评审多少条；0 表示全部。")
    parser.add_argument("--llm-task-name", default="utils", help="评审使用的模型任务名。")
    parser.add_argument("--model-name", default="", help="指定评审模型名称；为空时使用任务默认模型。")
    parser.add_argument("--max-tokens", type=int, default=512, help="评审最大输出 token。")
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

    path = Path(str(raw_path or "").strip()).expanduser()
    return path if path.is_absolute() else ROOT_PATH / path


def resolve_input_path(raw_path: str) -> Path:
    """解析输入 JSON 路径，默认使用最新批量对比文件。"""

    if normalize_text(raw_path):
        return resolve_path(raw_path)
    candidates = sorted(
        (ROOT_PATH / "data" / "analysis").glob("expression_selection_batch_compare_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("未找到 expression_selection_batch_compare_*.json")
    return candidates[0]


def stable_hash(value: str) -> int:
    """生成稳定 hash。"""

    return int.from_bytes(sha256(value.encode("utf-8")).digest()[:8], "big")


def get_method_keys(source_payload: dict[str, Any]) -> list[str]:
    """读取本次盲评要比较的方法名。"""

    raw_method_keys = source_payload.get("method_keys")
    if not isinstance(raw_method_keys, list):
        return list(DEFAULT_METHODS)

    method_keys = [
        str(method_key or "").strip()
        for method_key in raw_method_keys
        if str(method_key or "").strip()
    ]
    if len(method_keys) != len(BLIND_LABELS):
        raise ValueError(f"盲评方法数量必须是 {len(BLIND_LABELS)} 个，当前是 {len(method_keys)} 个")
    return method_keys


def build_blind_mapping(sample: dict[str, Any], method_keys: Sequence[str]) -> tuple[dict[str, str], dict[str, str]]:
    """为单个样本生成稳定随机的 A/B/C 到真实方法映射。"""

    sample_id = str(sample.get("sample_id") or sample.get("target_message_id") or "")
    ordered_methods = sorted(method_keys, key=lambda method: stable_hash(f"{sample_id}:{method}"))
    label_to_method = {label: method for label, method in zip(BLIND_LABELS, ordered_methods, strict=True)}
    method_to_label = {method: label for label, method in label_to_method.items()}
    return label_to_method, method_to_label


def build_blind_label_order(sample: dict[str, Any]) -> list[str]:
    """为单个样本生成稳定随机的方案展示顺序。"""

    sample_id = str(sample.get("sample_id") or sample.get("target_message_id") or "")
    return sorted(BLIND_LABELS, key=lambda label: stable_hash(f"{sample_id}:display:{label}"))


def method_items(sample: dict[str, Any], method: str) -> list[dict[str, Any]]:
    """读取某个方案的候选表达。"""

    method_payload = sample.get(method)
    if isinstance(method_payload, list):
        return list(method_payload)
    if not isinstance(method_payload, dict):
        return []
    if isinstance(method_payload.get("matches"), list):
        return list(method_payload.get("matches") or [])
    if isinstance(method_payload.get("selected_expressions"), list):
        return list(method_payload.get("selected_expressions") or [])
    return []


def render_blind_scheme(sample: dict[str, Any], label: str, method: str) -> str:
    """渲染盲化方案内容。"""

    items = method_items(sample, method)
    if not items:
        return f"方案 {label}：无候选表达"
    lines = [f"方案 {label}："]
    for index, item in enumerate(items, 1):
        situation = normalize_text(item.get("situation"))
        style = normalize_text(item.get("style"))
        lines.append(f"{index}. 情境：{situation}\n   表达方式：{style}")
    return "\n".join(lines)


def build_judge_prompt(sample: dict[str, Any], label_to_method: dict[str, str]) -> str:
    """构建自动盲评 prompt。"""

    target_message = sample.get("target_message") or {}
    target_text = normalize_text(target_message.get("text"))
    history_lines = sample.get("history_lines") or []
    history_block = "\n".join(str(line) for line in history_lines[-10:]) or "无"
    query_text = str(sample.get("query_text") or "").strip() or "无"
    label_order = build_blind_label_order(sample)
    scheme_blocks = [
        render_blind_scheme(sample, label, label_to_method[label])
        for label in label_order
    ]

    return (
        "请判断哪个方案提供的表达方式候选最适合本次回复。\n"
        "评价标准：贴近 planner 意图、贴近目标消息、能自然辅助生成合适回复、不生硬、不跑题。\n"
        "如果三个方案都明显不合适，special 填 none；如果难分高下或各有优劣，special 填 tie。\n"
        "否则 special 填空字符串，并给出 ranking，按从好到差排列 A/B/C。\n"
        "scores 是每个方案 1 到 5 分，5 表示最贴合。\n"
        "严格只输出 JSON，不要 Markdown。格式：\n"
        '{"ranking":["A","B","C"],"special":"","scores":{"A":5,"B":3,"C":2},"reason":"简短理由"}\n\n'
        f"目标消息：{target_text or '无'}\n"
        "\n"
        f"最近上下文：\n{history_block}\n\n"
        f"Planner Query：\n{query_text}\n\n"
        f"候选方案：\n\n{chr(10).join(scheme_blocks)}"
    )


def parse_judge_response(raw_response: str) -> dict[str, Any]:
    """解析自动评审输出。"""

    try:
        parsed = json.loads(repair_json(raw_response))
    except Exception:
        return {
            "ranking": [],
            "special": "",
            "scores": {},
            "reason": "解析失败",
        }
    if not isinstance(parsed, dict):
        return {
            "ranking": [],
            "special": "",
            "scores": {},
            "reason": "解析结果不是对象",
        }
    return parsed


def normalize_ranking(raw_ranking: Any) -> List[str]:
    """规范化 A/B/C 排序。"""

    if not isinstance(raw_ranking, list):
        return []
    ranking: List[str] = []
    for raw_label in raw_ranking:
        label = str(raw_label or "").strip().upper()
        if label in BLIND_LABELS and label not in ranking:
            ranking.append(label)
    for label in BLIND_LABELS:
        if label not in ranking:
            ranking.append(label)
    return ranking[: len(BLIND_LABELS)]


def normalize_scores(raw_scores: Any) -> dict[str, int]:
    """规范化评分。"""

    scores: dict[str, int] = {}
    raw_score_map = raw_scores if isinstance(raw_scores, dict) else {}
    for label in BLIND_LABELS:
        try:
            score = int(raw_score_map.get(label, 0))
        except (TypeError, ValueError):
            score = 0
        scores[label] = max(1, min(score, 5)) if score else 0
    return scores


async def judge_sample(
    *,
    sample: dict[str, Any],
    method_keys: Sequence[str],
    llm_client: LLMServiceClient,
    args: Namespace,
) -> AutoJudgeResult:
    """评审单条样本。"""

    label_to_method, method_to_label = build_blind_mapping(sample, method_keys)
    prompt = build_judge_prompt(sample, label_to_method)
    response = await llm_client.generate_response(
        prompt,
        LLMGenerationOptions(
            temperature=0,
            max_tokens=max(1, int(args.max_tokens)),
            model_name=str(args.model_name or "").strip() or None,
        ),
    )
    parsed = parse_judge_response(response.response.strip())
    special = str(parsed.get("special") or "").strip().lower()
    if special not in {"", "tie", "none"}:
        special = ""
    ranking_labels = [] if special else normalize_ranking(parsed.get("ranking"))
    scores_by_label = normalize_scores(parsed.get("scores"))
    ranking_methods = [label_to_method[label] for label in ranking_labels if label in label_to_method]
    scores_by_method = {
        label_to_method[label]: score
        for label, score in scores_by_label.items()
        if label in label_to_method
    }
    target_message = sample.get("target_message") or {}
    return AutoJudgeResult(
        sample_id=str(sample.get("sample_id") or ""),
        target_text=str(target_message.get("text") or ""),
        actual_reply=str(sample.get("actual_reply") or ""),
        label_to_method=label_to_method,
        method_to_label=method_to_label,
        raw_response=response.response.strip(),
        ranking_labels=ranking_labels,
        ranking_methods=ranking_methods,
        special=special,
        scores_by_label=scores_by_label,
        scores_by_method=scores_by_method,
        reason=str(parsed.get("reason") or "").strip(),
        model_name=response.model_name,
        prompt_tokens=response.prompt_tokens,
        completion_tokens=response.completion_tokens,
    )


def build_summary(results: Sequence[AutoJudgeResult], method_keys: Sequence[str]) -> dict[str, Any]:
    """汇总自动评审结果。"""

    first_place_counts = {method: 0 for method in method_keys}
    point_totals = {method: 0 for method in method_keys}
    score_totals = {method: 0 for method in method_keys}
    score_counts = {method: 0 for method in method_keys}
    special_counts = {"tie": 0, "none": 0}
    for result in results:
        if result.special in special_counts:
            special_counts[result.special] += 1
            continue
        for rank_index, method in enumerate(result.ranking_methods):
            if method not in first_place_counts:
                continue
            if rank_index == 0:
                first_place_counts[method] += 1
            point_totals[method] += max(3 - rank_index, 0)
        for method, score in result.scores_by_method.items():
            if method in score_totals and score > 0:
                score_totals[method] += score
                score_counts[method] += 1

    average_scores = {
        method: round(score_totals[method] / score_counts[method], 3) if score_counts[method] else 0
        for method in method_keys
    }
    return {
        "sample_count": len(results),
        "first_place_counts": first_place_counts,
        "point_totals": point_totals,
        "average_scores": average_scores,
        "special_counts": special_counts,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    """写出 Markdown 摘要。"""

    lines = [
        "# 表达选择自动盲评",
        "",
        f"- 生成时间：{payload['generated_at']}",
        f"- 输入文件：{payload['input_json']}",
        f"- 样本数：{payload['summary']['sample_count']}",
        "",
        "## 汇总",
        "",
        f"- 第一名次数：{payload['summary']['first_place_counts']}",
        f"- 排名积分：{payload['summary']['point_totals']}",
        f"- 平均分：{payload['summary']['average_scores']}",
        f"- 特殊判断：{payload['summary']['special_counts']}",
        "",
        "## 明细",
        "",
    ]
    for index, result in enumerate(payload["results"], 1):
        ranking = result.get("ranking_methods") or []
        lines.extend(
            [
                f"### 样本 {index}",
                "",
                f"- 目标消息：{result.get('target_text') or ''}",
                f"- 实际回复：{result.get('actual_reply') or ''}",
                f"- 特殊判断：{result.get('special') or '无'}",
                f"- 排序：{ranking}",
                f"- 分数：{result.get('scores_by_method')}",
                f"- 理由：{result.get('reason') or ''}",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


async def async_main() -> None:
    """脚本入口。"""

    args = parse_args()
    input_path = resolve_input_path(args.input_json)
    source_payload = json.loads(input_path.read_text(encoding="utf-8"))
    method_keys = get_method_keys(source_payload)
    samples = list(source_payload.get("samples") or [])
    if int(args.limit) > 0:
        samples = samples[: int(args.limit)]
    if not samples:
        raise ValueError(f"输入文件没有 samples: {input_path}")

    llm_client = LLMServiceClient(
        task_name=str(args.llm_task_name or "utils"),
        request_type="expression.auto_blind_judge",
    )
    results: List[AutoJudgeResult] = []
    for sample in samples:
        result = await judge_sample(sample=sample, method_keys=method_keys, llm_client=llm_client, args=args)
        results.append(result)
        ranking_text = result.special or " > ".join(result.ranking_labels)
        print(f"已评审 {len(results)}/{len(samples)}: {ranking_text} | {result.reason}")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "input_json": str(input_path),
        "args": vars(args),
        "method_keys": list(method_keys),
        "summary": build_summary(results, method_keys),
        "results": [asdict(result) for result in results],
    }
    output_dir = resolve_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"expression_selection_auto_blind_judge_{timestamp}.json"
    markdown_path = output_dir / f"expression_selection_auto_blind_judge_{timestamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(markdown_path, payload)
    print(f"JSON 输出: {json_path.resolve()}")
    print(f"Markdown 输出: {markdown_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(async_main())
