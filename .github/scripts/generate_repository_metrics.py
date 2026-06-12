from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import argparse
import json
import os
import re


API_ROOT = "https://api.github.com"
LINK_LAST_PAGE_RE = re.compile(r"[?&]page=(\d+)>;\s*rel=\"last\"")


def request_json(path: str, token: Optional[str]) -> Tuple[Any, Dict[str, str]]:
    request = Request(f"{API_ROOT}{path}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        request.add_header("Authorization", f"Bearer {token}")

    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body), dict(response.headers)
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API 请求失败: {error.code} {error.reason}: {body}") from error


def count_paginated(path: str, token: Optional[str]) -> int:
    data, headers = request_json(path, token)
    if not isinstance(data, list):
        raise RuntimeError(f"GitHub API 返回了非列表数据: {path}")

    link = headers.get("Link", "")
    match = LINK_LAST_PAGE_RE.search(link)
    if match:
        return int(match.group(1))
    return len(data)


def search_count(query: str, token: Optional[str]) -> int:
    data, _ = request_json(f"/search/issues?q={quote_plus(query)}&per_page=1", token)
    return int(data["total_count"])


def format_number(value: int) -> str:
    return f"{value:,}"


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def format_relative_time(value: str, now: datetime) -> str:
    delta = now - parse_time(value)
    if delta.days >= 1:
        return f"{delta.days} 天前"
    hours = max(1, delta.seconds // 3600)
    return f"{hours} 小时前"


def collect_metrics(repo: str, token: Optional[str]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=30)
    since_iso = since.isoformat(timespec="seconds").replace("+00:00", "Z")

    repository, _ = request_json(f"/repos/{repo}", token)
    default_branch = repository["default_branch"]
    latest_commit, _ = request_json(f"/repos/{repo}/commits/{default_branch}", token)

    return {
        "repo": repo,
        "description": repository.get("description") or "",
        "stars": int(repository["stargazers_count"]),
        "forks": int(repository["forks_count"]),
        "watchers": int(repository["subscribers_count"]),
        "open_issues": search_count(f"repo:{repo} type:issue state:open", token),
        "open_prs": search_count(f"repo:{repo} type:pr state:open", token),
        "closed_issues_30d": search_count(f"repo:{repo} type:issue state:closed closed:>={since.date()}", token),
        "merged_prs_30d": search_count(f"repo:{repo} type:pr is:merged merged:>={since.date()}", token),
        "commits_30d": count_paginated(
            f"/repos/{repo}/commits?sha={default_branch}&since={since_iso}&per_page=1",
            token,
        ),
        "contributors": count_paginated(f"/repos/{repo}/contributors?anon=true&per_page=1", token),
        "default_branch": default_branch,
        "latest_sha": latest_commit["sha"][:7],
        "latest_message": latest_commit["commit"]["message"].splitlines()[0],
        "latest_commit_at": latest_commit["commit"]["author"]["date"],
        "generated_at": now.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M UTC+8"),
    }


def metric_card(x: int, y: int, title: str, value: str, detail: str, color: str) -> str:
    return f"""
  <g transform="translate({x} {y})">
    <rect width="206" height="82" rx="10" fill="#ffffff" stroke="#d8dee4"/>
    <rect x="0" y="0" width="5" height="82" rx="2.5" fill="{color}"/>
    <text x="20" y="27" fill="#57606a" font-size="13">{escape(title)}</text>
    <text x="20" y="55" fill="#24292f" font-size="25" font-weight="700">{escape(value)}</text>
    <text x="110" y="55" fill="#6e7781" font-size="12">{escape(detail)}</text>
  </g>"""


def render_svg(metrics: Dict[str, Any]) -> str:
    cards: List[Tuple[str, str, str, str]] = [
        ("Stars", format_number(metrics["stars"]), "累计收藏", "#f59f00"),
        ("Forks", format_number(metrics["forks"]), "派生仓库", "#2f9e44"),
        ("Open Issues", format_number(metrics["open_issues"]), "待处理问题", "#e8590c"),
        ("Open PRs", format_number(metrics["open_prs"]), "待合并请求", "#66a80f"),
        ("Commits", format_number(metrics["commits_30d"]), "最近 30 天", "#099268"),
        ("Merged PRs", format_number(metrics["merged_prs_30d"]), "最近 30 天", "#f08c00"),
        ("Closed Issues", format_number(metrics["closed_issues_30d"]), "最近 30 天", "#d9480f"),
        ("Contributors", format_number(metrics["contributors"]), "贡献者", "#37b24d"),
    ]

    card_svgs: List[str] = []
    for index, (title, value, detail, color) in enumerate(cards):
        x = 28 + (index % 4) * 222
        y = 126 + (index // 4) * 98
        card_svgs.append(metric_card(x, y, title, value, detail, color))

    latest_commit_time = format_relative_time(metrics["latest_commit_at"], datetime.now(timezone.utc))
    latest_message = escape(metrics["latest_message"][:78])

    return f"""<svg width="920" height="360" viewBox="0 0 920 360" xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="title desc">
  <title id="title">{escape(metrics["repo"])} 仓库动态</title>
  <desc id="desc">自动生成的仓库动态统计图，包含 PR、Issue、Commit、Star 和 Fork 数据。</desc>
  <defs>
    <linearGradient id="header" x1="0" x2="1" y1="0" y2="1">
      <stop offset="0%" stop-color="#2f9e44"/>
      <stop offset="52%" stop-color="#66a80f"/>
      <stop offset="100%" stop-color="#f08c00"/>
    </linearGradient>
    <filter id="shadow" x="-5%" y="-5%" width="110%" height="110%">
      <feDropShadow dx="0" dy="8" stdDeviation="10" flood-color="#2b2318" flood-opacity="0.14"/>
    </filter>
  </defs>
  <rect width="920" height="360" rx="16" fill="#f7fbf2"/>
  <rect x="16" y="16" width="888" height="328" rx="14" fill="#ffffff" filter="url(#shadow)"/>
  <rect x="16" y="16" width="888" height="86" rx="14" fill="url(#header)"/>
  <path d="M16 88 H904 V102 H16 Z" fill="#ffffff"/>
  <text x="38" y="54" fill="#ffffff" font-size="26" font-weight="700">{escape(metrics["repo"])}</text>
  <text x="38" y="80" fill="#fff4d6" font-size="13">{escape(metrics["description"][:105])}</text>
  <text x="760" y="47" fill="#ffffff" font-size="13" text-anchor="end">默认分支</text>
  <text x="882" y="47" fill="#ffffff" font-size="18" font-weight="700" text-anchor="end">{escape(metrics["default_branch"])}</text>
  <text x="882" y="75" fill="#fff4d6" font-size="12" text-anchor="end">更新于 {escape(metrics["generated_at"])}</text>
{"".join(card_svgs)}
  <g transform="translate(28 318)">
    <circle cx="8" cy="8" r="5" fill="#1f883d"/>
    <text x="22" y="12" fill="#57606a" font-size="13">最后提交</text>
    <text x="94" y="12" fill="#24292f" font-size="13" font-weight="700">{escape(metrics["latest_sha"])}</text>
    <text x="156" y="12" fill="#57606a" font-size="13">{latest_commit_time}</text>
    <text x="246" y="12" fill="#24292f" font-size="13">{latest_message}</text>
  </g>
</svg>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="生成轻量仓库动态 SVG。")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"), help="owner/repo")
    parser.add_argument("--output", default="depends-data/repository-metrics.svg", help="SVG 输出路径")
    args = parser.parse_args()

    if not args.repo:
        raise RuntimeError("缺少仓库名称，请传入 --repo 或设置 GITHUB_REPOSITORY。")

    token = os.environ.get("METRICS_TOKEN") or os.environ.get("GITHUB_TOKEN")
    metrics = collect_metrics(args.repo, token)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_svg(metrics), encoding="utf-8", newline="\n")
    print(f"已生成 {output}")


if __name__ == "__main__":
    main()
