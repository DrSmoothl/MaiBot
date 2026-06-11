from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import json
import sys

from sqlmodel import Session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.database.database import get_db_session  # noqa: E402
from src.common.database.database_model import (  # noqa: E402
    BehaviorActionNode,
    BehaviorExperiencePath,
    BehaviorOutcomeNode,
    BehaviorSceneCluster,
    BehaviorSceneNode,
)
from src.learners.behavior_scenario import BehaviorScenarioProfile, parse_behavior_scenario_response  # noqa: E402
from src.learners.behavior_scene_graph_store import (  # noqa: E402
    _expand_scene_scores,
    _load_cluster_distribution,
    _load_scene_node_map,
    _score_behavior_clusters,
    _score_behavior_links,
    _score_behavior_paths,
    _score_scene_clusters,
    _score_scene_nodes_by_tag_clusters,
    build_scene_cluster_distribution,
    build_scene_descriptors,
    format_scene_cluster_distribution,
)


def _split_values(raw_value: str) -> List[str]:
    values: List[str] = []
    for item in str(raw_value or "").replace("，", ",").split(","):
        value = " ".join(item.split()).strip()
        if value and value not in values:
            values.append(value)
    return values


def _build_profile(args: Namespace) -> BehaviorScenarioProfile:
    if not args.profile_json:
        raise ValueError("请提供 --profile-json，且其中必须包含 tag_clusters")
    raw_profile = json.loads(args.profile_json)
    if not isinstance(raw_profile, dict):
        raise ValueError("--profile-json 必须是 JSON 对象")
    profile = parse_behavior_scenario_response(json.dumps(raw_profile, ensure_ascii=False))
    if not profile.tag_clusters:
        raise ValueError("--profile-json 必须包含非空 tag_clusters")
    return profile


def _session_ids(args: Namespace) -> Set[str]:
    values: Set[str] = set()
    for raw_item in args.session_id:
        values.update(_split_values(raw_item))
    return values


def _combine_scores(*score_maps: Dict[int, float]) -> Dict[int, float]:
    combined_scores: Dict[int, float] = {}
    for score_map in score_maps:
        for behavior_id, score in score_map.items():
            combined_scores[behavior_id] = combined_scores.get(behavior_id, 0.0) + float(score or 0.0)
    return dict(sorted(combined_scores.items(), key=lambda item: item[1], reverse=True))


def _path_payload(session: Session, path_id: int, score: float) -> Dict[str, Any]:
    path = session.get(BehaviorExperiencePath, path_id)
    if path is None:
        return {"id": path_id, "score": round(score, 4), "missing": True}

    cluster = session.get(BehaviorSceneCluster, path.scene_cluster_id)
    action_node = session.get(BehaviorActionNode, path.action_node_id)
    outcome_node = session.get(BehaviorOutcomeNode, path.outcome_node_id)
    return {
        "id": path.id,
        "score": round(score, 4),
        "session_id": path.session_id,
        "cluster_id": path.scene_cluster_id,
        "cluster": format_scene_cluster_distribution(_load_cluster_distribution(cluster.tag_distribution))
        if cluster is not None
        else "",
        "action": action_node.action if action_node is not None else "",
        "outcome": outcome_node.outcome if outcome_node is not None else "",
        "count": path.count,
        "success_count": path.success_count,
        "failure_count": path.failure_count,
        "enabled": path.enabled,
    }


def _paths_payload(session: Session, scores: Dict[int, float], max_count: int) -> List[Dict[str, Any]]:
    return [_path_payload(session, path_id, score) for path_id, score in list(scores.items())[:max_count]]


def _node_payload(node: Optional[BehaviorSceneNode], score: float) -> Dict[str, Any]:
    if node is None:
        return {"id": None, "score": round(score, 4), "missing": True}
    return {
        "id": node.id,
        "score": round(score, 4),
        "session_id": node.session_id,
        "kind": node.node_kind,
        "name": node.name,
        "source_count": node.source_count,
        "node_score": node.score,
    }


def _cluster_payload(session: Session, cluster_id: int, score: float) -> Dict[str, Any]:
    cluster = session.get(BehaviorSceneCluster, cluster_id)
    if cluster is None:
        return {"id": cluster_id, "score": round(score, 4), "missing": True}
    return {
        "id": cluster.id,
        "score": round(score, 4),
        "session_id": cluster.session_id,
        "name": format_scene_cluster_distribution(_load_cluster_distribution(cluster.tag_distribution)),
        "tags": _load_cluster_distribution(cluster.tag_distribution),
        "source_count": cluster.source_count,
        "cluster_score": cluster.score,
    }


def _compare_sets(graph_scores: Dict[int, float], cluster_scores: Dict[int, float]) -> Dict[str, List[int]]:
    graph_ids = set(graph_scores)
    cluster_ids = set(cluster_scores)
    return {
        "both": sorted(graph_ids & cluster_ids),
        "graph_only": sorted(graph_ids - cluster_ids),
        "cluster_only": sorted(cluster_ids - graph_ids),
    }


def compare_matching(args: Namespace) -> Dict[str, Any]:
    profile = _build_profile(args)
    if not profile.has_signal:
        raise ValueError("请提供包含 tag_clusters 的场景画像")

    session_ids = _session_ids(args)
    descriptors = build_scene_descriptors(profile)
    target_distribution = build_scene_cluster_distribution(profile)

    with get_db_session(auto_commit=False) as session:
        active_node_scores = _score_scene_nodes_by_tag_clusters(
            session,
            profile=profile,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        node_by_id = _load_scene_node_map(session, set(active_node_scores))
        expanded_node_scores = _expand_scene_scores(
            session,
            active_node_scores=active_node_scores,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        missing_node_ids = set(expanded_node_scores) - set(node_by_id)
        if missing_node_ids:
            node_by_id.update(_load_scene_node_map(session, missing_node_ids))
        graph_link_scores = _score_behavior_links(
            session,
            node_scores=expanded_node_scores,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        graph_path_scores = _score_behavior_paths(
            session,
            node_scores=expanded_node_scores,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        graph_behavior_scores = _combine_scores(graph_link_scores, graph_path_scores)

        cluster_scores = _score_scene_clusters(
            session,
            profile=profile,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        cluster_behavior_scores = _score_behavior_clusters(
            session,
            cluster_scores=cluster_scores,
            session_ids=session_ids,
            include_global=args.include_global,
        )
        combined_behavior_scores = _combine_scores(graph_behavior_scores, cluster_behavior_scores)

        active_nodes = [
            _node_payload(node_by_id.get(node_id), score)
            for node_id, score in list(active_node_scores.items())[: args.max_count]
        ]
        expanded_nodes = [
            _node_payload(node_by_id.get(node_id), score)
            for node_id, score in list(
                sorted(expanded_node_scores.items(), key=lambda item: item[1], reverse=True)
            )[: args.max_count]
        ]
        matched_clusters = [
            _cluster_payload(session, cluster_id, score)
            for cluster_id, score in list(cluster_scores.items())[: args.max_count]
        ]
        graph_paths = _paths_payload(session, graph_behavior_scores, args.max_count)
        cluster_paths = _paths_payload(session, cluster_behavior_scores, args.max_count)
        combined_paths = _paths_payload(session, combined_behavior_scores, args.max_count)

    return {
        "scope": {
            "session_ids": sorted(session_ids),
            "include_global": args.include_global,
            "include_global_note": "当前实现中 include_global=True 会跳过 session_id 过滤，用于观察现有行为。",
        },
        "profile": {
            "summary": profile.summary,
            "tag_clusters": profile.domain_prompt_payloads(),
            "need": profile.need_prompt_payload(),
            "other_traits": profile.other_traits_prompt_payloads(),
            "confidence": profile.confidence,
            "tag_key": profile.tag_cluster_text(),
        },
        "input_descriptors": [
            {"kind": descriptor.node_kind, "name": descriptor.name, "weight": descriptor.weight}
            for descriptor in descriptors
        ],
        "input_cluster_distribution": target_distribution,
        "scene_graph": {
            "active_nodes": active_nodes,
            "expanded_nodes": expanded_nodes,
            "behavior_candidates": graph_paths,
        },
        "scene_cluster": {
            "matched_clusters": matched_clusters,
            "behavior_candidates": cluster_paths,
        },
        "candidate_diff": _compare_sets(graph_behavior_scores, cluster_behavior_scores),
        "combined_behavior_candidates": combined_paths,
    }


def _print_path_table(title: str, paths: List[Dict[str, Any]]) -> None:
    print(f"\n{title}")
    if not paths:
        print("  无命中")
        return
    for path in paths:
        print(
            "  "
            f"#{path.get('id')} score={path.get('score')} session={path.get('session_id') or '__global__'} "
            f"cluster=#{path.get('cluster_id')} count={path.get('count')} "
            f"success={path.get('success_count')} failure={path.get('failure_count')}"
        )
        print(f"    场景簇: {path.get('cluster')}")
        print(f"    行为: {path.get('action')}")
        print(f"    结果: {path.get('outcome')}")


def print_report(result: Dict[str, Any]) -> None:
    profile = result["profile"]
    print("行为场景匹配对比")
    print(f"scope session_ids={result['scope']['session_ids']} include_global={result['scope']['include_global']}")
    print(f"scene_start: {profile['scene_start']}")
    print("\n输入场景图节点:")
    for descriptor in result["input_descriptors"]:
        print(f"  {descriptor['kind']}: {descriptor['name']} (weight={descriptor['weight']})")
    print("\n输入场景簇分布:")
    for tag in result["input_cluster_distribution"]:
        print(f"  {tag['tag']} = {tag['probability']}")

    print("\n场景图直接命中节点:")
    for node in result["scene_graph"]["active_nodes"]:
        print(f"  #{node.get('id')} {node.get('kind')} score={node.get('score')} name={node.get('name')}")

    print("\n场景簇命中:")
    for cluster in result["scene_cluster"]["matched_clusters"]:
        print(
            f"  #{cluster.get('id')} score={cluster.get('score')} "
            f"session={cluster.get('session_id') or '__global__'} name={cluster.get('name')}"
        )

    _print_path_table("场景图候选行为", result["scene_graph"]["behavior_candidates"])
    _print_path_table("场景簇候选行为", result["scene_cluster"]["behavior_candidates"])
    _print_path_table("合并后候选行为", result["combined_behavior_candidates"])

    diff = result["candidate_diff"]
    print("\n候选差异")
    print(f"  两边都命中: {diff['both']}")
    print(f"  仅场景图命中: {diff['graph_only']}")
    print(f"  仅场景簇命中: {diff['cluster_only']}")


def parse_args() -> Namespace:
    parser = ArgumentParser(description="对比行为学习的场景图匹配与场景簇匹配。")
    parser.add_argument("--session-id", action="append", default=[], help="限定聊天流 session_id，可重复或用逗号分隔。")
    parser.add_argument(
        "--include-global",
        action="store_true",
        help="按现有实现开启 include_global；注意这会跳过 session_id 过滤。",
    )
    parser.add_argument("--profile-json", required=True, help="直接传入包含 tag_clusters 的场景画像 JSON 对象。")
    parser.add_argument("--max-count", type=int, default=10, help="每类输出的最大数量。")
    parser.add_argument("--json", action="store_true", help="输出完整 JSON。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = compare_matching(args)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    print_report(result)


if __name__ == "__main__":
    main()
