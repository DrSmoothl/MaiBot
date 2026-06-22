"""Maisaka Prompt 预览落盘器。"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import time

from .preview_path_utils import build_preview_chat_dir_name, normalize_preview_name


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
        try:
            for suffix, content in files.items():
                normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
                file_path = chat_dir / f"{stem}{normalized_suffix}"
                file_path.write_text(content, encoding="utf-8")
                saved_paths[normalized_suffix] = file_path
        finally:
            cls._trim_overflow(chat_dir)
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
