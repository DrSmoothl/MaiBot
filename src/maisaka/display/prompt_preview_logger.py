"""Maisaka Prompt 预览落盘器。"""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Dict
from urllib.parse import quote

import re
import time

from .preview_path_utils import build_preview_chat_dir_name, normalize_preview_name

HTML_NAVIGATION_START = "<!-- maibot-reasoning-html-navigation:start -->"
HTML_NAVIGATION_END = "<!-- maibot-reasoning-html-navigation:end -->"
HTML_NAVIGATION_PATTERN = re.compile(
    rf"\s*{re.escape(HTML_NAVIGATION_START)}.*?{re.escape(HTML_NAVIGATION_END)}\s*",
    re.DOTALL,
)
HTML_BODY_OPEN_PATTERN = re.compile(r"(<body\b[^>]*>)", re.IGNORECASE)
HTML_STEM_TIMESTAMP_PATTERN = re.compile(r"^\d+")


class PromptPreviewLogger:
    """负责保存 Maisaka Prompt 预览文件并控制目录容量。"""

    _BASE_DIR = Path("logs") / "maisaka_prompt"
    _DEFAULT_MAX_PREVIEW_GROUPS_PER_CHAT = 256
    _TRIM_COUNT = 100

    @classmethod
    def _build_file_stem(cls, chat_dir: Path) -> str:
        base_stem = str(int(time.time() * 1000))
        candidate_stem = base_stem
        suffix_index = 1
        while any((chat_dir / f"{candidate_stem}{suffix}").exists() for suffix in (".html", ".json", ".txt")):
            candidate_stem = f"{base_stem}_{suffix_index}"
            suffix_index += 1
        return candidate_stem

    @staticmethod
    def _html_sort_key(file_path: Path) -> tuple[int, str]:
        match = HTML_STEM_TIMESTAMP_PATTERN.match(file_path.stem)
        timestamp = int(match.group(0)) if match is not None else 0
        return timestamp, file_path.name

    @staticmethod
    def _strip_html_navigation(content: str) -> str:
        return HTML_NAVIGATION_PATTERN.sub("\n", content).lstrip()

    @staticmethod
    def _build_navigation_link(file_path: Path | None, label: str) -> str:
        if file_path is None:
            return (
                "<span class='maibot-html-nav-button maibot-html-nav-button-disabled' "
                f"aria-disabled='true'>{escape(label)}</span>"
            )

        return (
            "<a class='maibot-html-nav-button' "
            f"href='{escape(quote(file_path.name), quote=True)}'>{escape(label)}</a>"
        )

    @classmethod
    def _build_html_navigation(cls, file_path: Path, previous_path: Path | None, next_path: Path | None) -> str:
        previous_link = cls._build_navigation_link(previous_path, "上一份")
        next_link = cls._build_navigation_link(next_path, "下一份")
        current_label = escape(file_path.stem)
        return f"""{HTML_NAVIGATION_START}
<style>
  .maibot-html-nav {{
    position: fixed;
    top: 12px;
    right: 12px;
    z-index: 2147483647;
    display: flex;
    align-items: center;
    gap: 8px;
    max-width: calc(100vw - 24px);
    padding: 8px;
    border: 1px solid rgba(148, 163, 184, 0.36);
    border-radius: 8px;
    background: rgba(255, 255, 255, 0.92);
    box-shadow: 0 12px 28px rgba(15, 23, 42, 0.16);
    color: #0f172a;
    font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    backdrop-filter: blur(8px);
  }}
  .maibot-html-nav-current {{
    min-width: 0;
    max-width: 220px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: #475569;
    font-size: 12px;
    font-weight: 700;
  }}
  .maibot-html-nav-button {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 30px;
    padding: 0 10px;
    border: 1px solid rgba(15, 23, 42, 0.14);
    border-radius: 6px;
    background: #0f172a;
    color: #fff;
    font-size: 13px;
    font-weight: 700;
    line-height: 1;
    text-decoration: none;
    white-space: nowrap;
  }}
  .maibot-html-nav-button:hover {{
    background: #1e293b;
  }}
  .maibot-html-nav-button-disabled {{
    background: #e2e8f0;
    color: #94a3b8;
    cursor: not-allowed;
  }}
  .maibot-html-nav-spacer {{
    height: 54px;
  }}
  @media (max-width: 640px) {{
    .maibot-html-nav {{
      left: 8px;
      right: 8px;
      top: 8px;
    }}
    .maibot-html-nav-current {{
      flex: 1;
      max-width: none;
    }}
  }}
</style>
<nav class="maibot-html-nav" aria-label="推理过程 HTML 导航">
  {previous_link}
  <span class="maibot-html-nav-current" title="{current_label}">{current_label}</span>
  {next_link}
</nav>
<div class="maibot-html-nav-spacer" aria-hidden="true"></div>
{HTML_NAVIGATION_END}
"""

    @classmethod
    def _inject_html_navigation(
        cls,
        content: str,
        file_path: Path,
        previous_path: Path | None,
        next_path: Path | None,
    ) -> str:
        clean_content = cls._strip_html_navigation(content)
        navigation_html = cls._build_html_navigation(file_path, previous_path, next_path)
        if HTML_BODY_OPEN_PATTERN.search(clean_content):
            return HTML_BODY_OPEN_PATTERN.sub(
                lambda match: f"{match.group(1)}\n{navigation_html}",
                clean_content,
                count=1,
            )
        return f"{navigation_html}{clean_content}"

    @classmethod
    def _refresh_html_navigation(cls, chat_dir: Path) -> None:
        html_files = sorted(
            [
                file_path
                for file_path in chat_dir.iterdir()
                if file_path.is_file() and file_path.suffix.lower() == ".html"
            ],
            key=cls._html_sort_key,
        )
        for index, file_path in enumerate(html_files):
            previous_path = html_files[index - 1] if index > 0 else None
            next_path = html_files[index + 1] if index + 1 < len(html_files) else None
            try:
                content = file_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

            updated_content = cls._inject_html_navigation(content, file_path, previous_path, next_path)
            if updated_content == content:
                continue
            try:
                file_path.write_text(updated_content, encoding="utf-8")
            except OSError:
                continue

    @classmethod
    def save_preview_files(
        cls,
        chat_id: str,
        category: str,
        files: Dict[str, str],
    ) -> Dict[str, Path]:
        """保存同一份 Prompt 预览的多个文件并执行超量清理。"""

        normalized_category = normalize_preview_name(category)
        chat_dir = (cls._BASE_DIR / normalized_category / build_preview_chat_dir_name(chat_id)).resolve()
        chat_dir.mkdir(parents=True, exist_ok=True)
        stem = cls._build_file_stem(chat_dir)
        saved_paths: Dict[str, Path] = {}
        saved_html = False
        try:
            for suffix, content in files.items():
                normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
                file_path = chat_dir / f"{stem}{normalized_suffix}"
                file_content = cls._strip_html_navigation(content) if normalized_suffix == ".html" else content
                file_path.write_text(file_content, encoding="utf-8")
                saved_paths[normalized_suffix] = file_path
                saved_html = saved_html or normalized_suffix == ".html"
        finally:
            cls._trim_overflow(chat_dir)
        if saved_html:
            cls._refresh_html_navigation(chat_dir)
        return saved_paths

    @classmethod
    def _trim_overflow(cls, chat_dir: Path) -> None:
        """超过阈值时按批次删除最老的若干组预览文件。"""

        max_preview_groups = cls._get_max_preview_groups_per_chat()
        grouped_files: dict[str, list[Path]] = {}
        for file_path in chat_dir.iterdir():
            if not file_path.is_file():
                continue
            grouped_files.setdefault(file_path.stem, []).append(file_path)

        if len(grouped_files) <= max_preview_groups:
            return

        sorted_groups = sorted(
            grouped_files.items(),
            key=lambda item: min(path.stat().st_mtime for path in item[1]),
        )
        overflow_count = len(grouped_files) - max_preview_groups
        trim_count = min(len(sorted_groups), max(cls._TRIM_COUNT, overflow_count))
        for _, file_group in sorted_groups[:trim_count]:
            for old_file in file_group:
                try:
                    old_file.unlink()
                except FileNotFoundError:
                    continue

    @classmethod
    def _get_max_preview_groups_per_chat(cls) -> int:
        try:
            from src.config.config import global_config

            configured_limit = global_config.log.maisaka_prompt_preview_limit
            return max(1, int(configured_limit or cls._DEFAULT_MAX_PREVIEW_GROUPS_PER_CHAT))
        except Exception:
            return cls._DEFAULT_MAX_PREVIEW_GROUPS_PER_CHAT
