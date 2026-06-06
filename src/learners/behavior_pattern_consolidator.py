from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from json_repair import repair_json
from sqlmodel import select

import json

from src.common.data_models.llm_service_data_models import LLMGenerationOptions
from src.common.database.database import get_db_session
from src.common.database.database_model import BehaviorPattern
from src.common.logger import get_logger
from src.common.prompt_i18n import load_prompt
from src.config.config import global_config
from src.llm_models.payload_content.message import Message, MessageBuilder, RoleType
from src.maisaka.display.prompt_cli_renderer import PromptCLIVisualizer
from src.services.llm_service import LLMServiceClient

from .behavior_pattern_maintenance import BehaviorPatternMergeGroup, behavior_pattern_maintenance
from .behavior_pattern_store import behavior_pattern_to_dict

logger = get_logger("behavior_pattern_consolidator")

behavior_consolidate_model = LLMServiceClient(task_name="learner", request_type="behavior.consolidator")

MAX_CONSOLIDATION_PATTERNS = 60
MAX_MERGE_GROUPS_PER_BATCH = 8


@dataclass(frozen=True)
class BehaviorConsolidationSuggestion:
    """LLM 语义整合器输出的一组行为表现合并建议。"""

    keeper_id: int
    merge_ids: list[int] = field(default_factory=list)
    trigger: str = ""
    action: str = ""
    outcome: str = ""
    reason: str = ""


@dataclass(frozen=True)
class BehaviorConsolidationResult:
    """行为表现语义整合结果摘要。"""

    session_id: str
    scanned_count: int = 0
    suggestion_count: int = 0
    merged_count: int = 0
    skipped_reason: str = ""

    @property
    def changed(self) -> bool:
        return self.merged_count > 0


def _strip_json_code_fence(raw_response: str) -> str:
    normalized_response = raw_response.strip()
    if not normalized_response.startswith("```"):
        return normalized_response

    lines = normalized_response.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return normalized_response


def _coerce_int(raw_value: Any) -> int:
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return 0


def _coerce_merge_ids(raw_value: Any) -> list[int]:
    if isinstance(raw_value, list):
        raw_items = raw_value
    elif raw_value is None:
        raw_items = []
    else:
        raw_items = [raw_value]

    merge_ids: list[int] = []
    for raw_item in raw_items:
        merge_id = _coerce_int(raw_item)
        if merge_id > 0 and merge_id not in merge_ids:
            merge_ids.append(merge_id)
    return merge_ids


def _parse_suggestion(raw_item: Any) -> Optional[BehaviorConsolidationSuggestion]:
    if not isinstance(raw_item, dict):
        return None

    keeper_id = _coerce_int(raw_item.get("keeper_id") or raw_item.get("target_id"))
    merge_ids = _coerce_merge_ids(
        raw_item.get("merge_ids")
        or raw_item.get("merged_ids")
        or raw_item.get("duplicate_ids")
        or raw_item.get("source_ids")
    )
    merge_ids = [merge_id for merge_id in merge_ids if merge_id != keeper_id]
    if keeper_id <= 0 or not merge_ids:
        return None

    return BehaviorConsolidationSuggestion(
        keeper_id=keeper_id,
        merge_ids=merge_ids,
        trigger=str(raw_item.get("trigger") or "").strip(),
        action=str(raw_item.get("action") or "").strip(),
        outcome=str(raw_item.get("outcome") or "").strip(),
        reason=str(raw_item.get("reason") or "").strip(),
    )


def parse_behavior_consolidation_response(response: str) -> list[BehaviorConsolidationSuggestion]:
    """解析行为表现语义整合器返回的 JSON。"""

    normalized_response = _strip_json_code_fence(response or "")
    if not normalized_response:
        return []

    try:
        parsed_response = json.loads(repair_json(normalized_response))
    except Exception:
        logger.warning(f"行为表现语义整合结果解析失败: {normalized_response!r}")
        return []

    if isinstance(parsed_response, dict):
        raw_items = (
            parsed_response.get("merge_groups")
            or parsed_response.get("merges")
            or parsed_response.get("items")
            or []
        )
    else:
        raw_items = parsed_response

    if isinstance(raw_items, dict):
        raw_items = [raw_items]
    if not isinstance(raw_items, list):
        return []

    suggestions: list[BehaviorConsolidationSuggestion] = []
    used_pattern_ids: set[int] = set()
    for raw_item in raw_items:
        suggestion = _parse_suggestion(raw_item)
        if suggestion is None:
            continue
        group_ids = {suggestion.keeper_id, *suggestion.merge_ids}
        if group_ids & used_pattern_ids:
            continue
        used_pattern_ids.update(group_ids)
        suggestions.append(suggestion)
        if len(suggestions) >= MAX_MERGE_GROUPS_PER_BATCH:
            break
    return suggestions


class BehaviorPatternConsolidator:
    """使用 LLM 对当前聊天流内的行为表现做语义级合并。"""

    async def consolidate_after_learning(self, session_id: str) -> BehaviorConsolidationResult:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return BehaviorConsolidationResult(session_id="", skipped_reason="empty_session_id")

        patterns = self._load_session_patterns(normalized_session_id)
        if len(patterns) < 2:
            return BehaviorConsolidationResult(
                session_id=normalized_session_id,
                scanned_count=len(patterns),
                skipped_reason="not_enough_patterns",
            )

        messages = self._build_consolidation_messages(patterns)
        try:
            generation_result = await behavior_consolidate_model.generate_response_with_messages(
                lambda _client: messages,
                options=LLMGenerationOptions(temperature=0.1),
            )
            response = generation_result.response or ""
            self._log_consolidation_preview(
                messages,
                session_id=normalized_session_id,
                pattern_count=len(patterns),
                output_content=response,
            )
        except Exception as exc:
            logger.error(f"行为表现语义整合请求失败: session_id={normalized_session_id} error={exc}")
            return BehaviorConsolidationResult(
                session_id=normalized_session_id,
                scanned_count=len(patterns),
                skipped_reason="llm_error",
            )

        suggestions = self._filter_suggestions_for_patterns(
            parse_behavior_consolidation_response(response),
            patterns,
        )
        if not suggestions:
            return BehaviorConsolidationResult(
                session_id=normalized_session_id,
                scanned_count=len(patterns),
                skipped_reason="no_suggestions",
            )

        merge_result = behavior_pattern_maintenance.apply_merge_groups(
            session_id=normalized_session_id,
            merge_groups=[
                BehaviorPatternMergeGroup(
                    keeper_id=suggestion.keeper_id,
                    merge_ids=suggestion.merge_ids,
                    trigger=suggestion.trigger,
                    action=suggestion.action,
                    outcome=suggestion.outcome,
                    reason=suggestion.reason,
                )
                for suggestion in suggestions
            ],
        )
        return BehaviorConsolidationResult(
            session_id=normalized_session_id,
            scanned_count=len(patterns),
            suggestion_count=len(suggestions),
            merged_count=merge_result.merged_count,
            skipped_reason=merge_result.skipped_reason,
        )

    @staticmethod
    def _load_session_patterns(session_id: str) -> list[BehaviorPattern]:
        try:
            with get_db_session(auto_commit=False) as session:
                statement = (
                    select(BehaviorPattern)
                    .where(BehaviorPattern.session_id == session_id)
                    .where(BehaviorPattern.enabled.is_(True))  # type: ignore[attr-defined]
                    .order_by(BehaviorPattern.update_time.desc())  # type: ignore[attr-defined]
                    .limit(MAX_CONSOLIDATION_PATTERNS)
                )
                patterns = list(session.exec(statement).all())
                for pattern in patterns:
                    session.expunge(pattern)
                return patterns
        except Exception as exc:
            logger.error(f"读取行为表现语义整合候选失败: session_id={session_id} error={exc}")
            return []

    @staticmethod
    def _build_pattern_payload(patterns: Sequence[BehaviorPattern]) -> list[dict[str, Any]]:
        return [
            behavior_pattern_to_dict(pattern)
            for pattern in patterns
            if pattern.id is not None and pattern.enabled and pattern.trigger and pattern.action and pattern.outcome
        ]

    def _build_consolidation_messages(self, patterns: Sequence[BehaviorPattern]) -> list[Message]:
        behavior_patterns = json.dumps(
            self._build_pattern_payload(patterns),
            ensure_ascii=False,
            indent=2,
        )
        prompt = load_prompt(
            "consolidate_behavior",
            bot_name=global_config.bot.nickname,
            behavior_patterns=behavior_patterns,
        )
        return [
            MessageBuilder()
            .set_role(RoleType.System)
            .add_text_content(prompt)
            .build()
        ]

    @staticmethod
    def _filter_suggestions_for_patterns(
        suggestions: list[BehaviorConsolidationSuggestion],
        patterns: Sequence[BehaviorPattern],
    ) -> list[BehaviorConsolidationSuggestion]:
        valid_ids = {int(pattern.id) for pattern in patterns if pattern.id is not None and pattern.enabled}
        filtered_suggestions: list[BehaviorConsolidationSuggestion] = []
        used_ids: set[int] = set()
        for suggestion in suggestions:
            group_ids = {suggestion.keeper_id, *suggestion.merge_ids}
            if len(group_ids) < 2:
                continue
            if not group_ids.issubset(valid_ids):
                continue
            if group_ids & used_ids:
                continue
            used_ids.update(group_ids)
            filtered_suggestions.append(suggestion)
        return filtered_suggestions

    @staticmethod
    def _log_consolidation_preview(
        messages: list[Message],
        *,
        session_id: str,
        pattern_count: int,
        output_content: str,
    ) -> None:
        try:
            preview_access = PromptCLIVisualizer.build_prompt_preview_access(
                messages,
                category="behavior_consolidator",
                chat_id=session_id,
                request_kind="behavior_consolidator",
                selection_reason=(
                    f"会话ID: {session_id}\n"
                    f"候选行为表现数: {pattern_count}\n"
                    "来源: behavior_learning_after_write"
                ),
                output_content=output_content,
            )
        except Exception as exc:
            logger.warning(f"{session_id} 行为表现语义整合预览保存失败: {exc}")
            return

        logger.info(
            f"{session_id} 行为表现语义整合预览已生成: "
            f"WebUI={preview_access.viewer_web_uri} "
            f"HTML={preview_access.viewer_path} "
            f"TXT={preview_access.dump_path}"
        )


behavior_pattern_consolidator = BehaviorPatternConsolidator()
