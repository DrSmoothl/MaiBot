from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Sequence

from json_repair import repair_json

import asyncio
import json

from src.chat.utils.utils import is_bot_self
from src.common.data_models.llm_service_data_models import LLMGenerationOptions
from src.common.logger import get_logger
from src.common.prompt_i18n import load_prompt
from src.config.config import global_config
from src.llm_models.payload_content.message import Message, MessageBuilder, RoleType
from src.maisaka.display.prompt_cli_renderer import PromptCLIVisualizer
from src.services.llm_service import LLMServiceClient

from .behavior_pattern_consolidator import behavior_pattern_consolidator
from .behavior_pattern_maintenance import behavior_pattern_maintenance
from .behavior_pattern_store import upsert_behavior_pattern
from .behavior_scenario import BehaviorScenarioProfile, BehaviorScenarioSegment, behavior_scenario_analyzer

if TYPE_CHECKING:
    from src.chat.message_receive.message import SessionMessage
    from src.maisaka.context.messages import LLMContextMessage


logger = get_logger("behavior_learner")

behavior_learn_model = LLMServiceClient(task_name="learner", request_type="behavior.learner")
behavior_scene_model = LLMServiceClient(task_name="learner", request_type="behavior.scene_analyzer")


@dataclass(frozen=True)
class BehaviorCandidate:
    """从聊天历史中抽取出的场景-行为-结果候选。"""

    trigger: str
    action: str
    outcome: str
    source_ids: list[str]
    segment_id: str = ""


@dataclass(frozen=True)
class BehaviorParseDiagnostics:
    """行为学习输出解析诊断信息。"""

    normalized_response: str
    parsed_item_count: int = 0
    accepted_item_count: int = 0
    invalid_item_count: int = 0
    empty_output: bool = False
    missing_scene_start: bool = False
    parse_error: str = ""
    non_list_output: bool = False


@dataclass(frozen=True)
class BehaviorParseResult:
    """行为学习输出解析结果。"""

    candidates: list[BehaviorCandidate]
    diagnostics: BehaviorParseDiagnostics


@dataclass(frozen=True)
class BehaviorFilterResult:
    """行为学习候选过滤结果。"""

    candidates: list[BehaviorCandidate]
    skipped_reasons: dict[str, int]


@dataclass(frozen=True)
class BehaviorLearningAcquireResult:
    """行为学习批次并发闸门的申请结果。"""

    acquired: bool
    reason: str = ""
    active_count: int = 0
    max_count: int = 0


class BehaviorLearningBatchGate:
    """控制行为学习批次的聊天流互斥与全局并发上限。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._active_session_ids: set[str] = set()

    async def acquire(self, session_id: str) -> BehaviorLearningAcquireResult:
        max_count = int(global_config.expression.max_expression_learner)
        if max_count <= 0:
            return BehaviorLearningAcquireResult(False, "max_expression_learner <= 0", 0, max_count)

        async with self._lock:
            active_count = len(self._active_session_ids)
            if session_id in self._active_session_ids:
                return BehaviorLearningAcquireResult(False, "session_busy", active_count, max_count)
            if active_count >= max_count:
                return BehaviorLearningAcquireResult(False, "global_limit", active_count, max_count)

            self._active_session_ids.add(session_id)
            return BehaviorLearningAcquireResult(True, active_count=active_count + 1, max_count=max_count)

    async def release(self, session_id: str) -> None:
        async with self._lock:
            self._active_session_ids.discard(session_id)


behavior_learning_batch_gate = BehaviorLearningBatchGate()


def _strip_json_code_fence(raw_response: str) -> str:
    normalized_response = raw_response.strip()
    if not normalized_response.startswith("```"):
        return normalized_response

    lines = normalized_response.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return normalized_response


def _coerce_source_ids(raw_value: Any) -> list[str]:
    if isinstance(raw_value, list):
        raw_items = raw_value
    elif raw_value is None:
        raw_items = []
    else:
        raw_items = [raw_value]

    source_ids: list[str] = []
    for raw_item in raw_items:
        if isinstance(raw_item, str) and "," in raw_item:
            split_items = raw_item.split(",")
        else:
            split_items = [raw_item]
        for split_item in split_items:
            source_id = str(split_item or "").strip()
            if source_id and source_id not in source_ids:
                source_ids.append(source_id)
    return source_ids


def _compact_log_text(text: str, *, max_length: int = 1200) -> str:
    """压缩长文本到适合日志展示的长度。"""

    compacted_text = " ".join((text or "").split()).strip()
    if len(compacted_text) <= max_length:
        return compacted_text
    return compacted_text[:max_length].rstrip() + "..."


def _parse_behavior_item(
    raw_item: Any,
    *,
    scene_start: str,
    scene_start_by_segment_id: Optional[dict[str, str]] = None,
) -> Optional[BehaviorCandidate]:
    if not isinstance(raw_item, dict):
        return None

    action = str(raw_item.get("action") or "").strip()
    outcome = str(raw_item.get("outcome") or "").strip()
    source_ids = _coerce_source_ids(raw_item.get("source_ids"))
    segment_id = str(raw_item.get("segment_id") or raw_item.get("scene_id") or "").strip()
    trigger = scene_start.strip()
    if segment_id and scene_start_by_segment_id:
        trigger = scene_start_by_segment_id.get(segment_id, trigger).strip()
    if not trigger or not action or not outcome:
        return None
    return BehaviorCandidate(
        trigger=trigger,
        action=action,
        outcome=outcome,
        source_ids=source_ids,
        segment_id=segment_id,
    )


def parse_behavior_response_with_diagnostics(
    response: str,
    *,
    scene_start: str,
    scene_start_by_segment_id: Optional[dict[str, str]] = None,
) -> BehaviorParseResult:
    """解析行为学习模型返回的 JSON，并保留诊断信息供日志输出。"""

    normalized_response = _strip_json_code_fence(response or "")
    normalized_scene_start = scene_start.strip()
    if not normalized_response:
        return BehaviorParseResult(
            candidates=[],
            diagnostics=BehaviorParseDiagnostics(normalized_response="", empty_output=True),
        )
    if not normalized_scene_start:
        return BehaviorParseResult(
            candidates=[],
            diagnostics=BehaviorParseDiagnostics(
                normalized_response=normalized_response,
                missing_scene_start=True,
            ),
        )

    try:
        parsed_response = json.loads(repair_json(normalized_response))
    except Exception as exc:
        return BehaviorParseResult(
            candidates=[],
            diagnostics=BehaviorParseDiagnostics(
                normalized_response=normalized_response,
                parse_error=str(exc),
            ),
        )

    if not isinstance(parsed_response, list):
        return BehaviorParseResult(
            candidates=[],
            diagnostics=BehaviorParseDiagnostics(
                normalized_response=normalized_response,
                non_list_output=True,
            ),
        )

    candidates: list[BehaviorCandidate] = []
    invalid_item_count = 0
    for raw_item in parsed_response:
        candidate = _parse_behavior_item(
            raw_item,
            scene_start=normalized_scene_start,
            scene_start_by_segment_id=scene_start_by_segment_id,
        )
        if candidate is not None:
            candidates.append(candidate)
        else:
            invalid_item_count += 1

    return BehaviorParseResult(
        candidates=candidates,
        diagnostics=BehaviorParseDiagnostics(
            normalized_response=normalized_response,
            parsed_item_count=len(parsed_response),
            accepted_item_count=len(candidates),
            invalid_item_count=invalid_item_count,
        ),
    )


def parse_behavior_response(response: str, *, scene_start: str) -> list[BehaviorCandidate]:
    """解析行为学习模型返回的 JSON。"""

    parse_result = parse_behavior_response_with_diagnostics(response, scene_start=scene_start)
    if parse_result.diagnostics.parse_error:
        logger.warning(f"行为学习结果解析失败: {parse_result.diagnostics.normalized_response!r}")
    return parse_result.candidates


class BehaviorLearner:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.min_messages_for_extraction = 10

    async def learn_from_context_messages(
        self,
        context_messages: Sequence["LLMContextMessage"],
    ) -> bool:
        """从 Maisaka 被裁切的上下文消息中学习行为表现模式。"""

        source_messages = self._extract_session_messages_from_context(context_messages)
        if not source_messages:
            logger.debug("裁切历史中没有可用于行为学习的真实聊天消息")
            return False
        if len(source_messages) < self.min_messages_for_extraction:
            logger.debug(
                f"裁切历史可学习行为消息不足: 可学习={len(source_messages)} 阈值={self.min_messages_for_extraction}"
            )
            return False

        return await self._learn_from_session_messages(source_messages)

    @staticmethod
    def _extract_session_messages_from_context(
        context_messages: Sequence["LLMContextMessage"],
    ) -> list["SessionMessage"]:
        """从上下文消息中过滤出真实聊天消息。"""

        from src.maisaka.context.messages import SessionBackedMessage

        source_messages: list["SessionMessage"] = []
        seen_message_ids: set[str] = set()
        seen_object_ids: set[int] = set()

        for context_message in context_messages:
            if not isinstance(context_message, SessionBackedMessage):
                continue
            if context_message.source_kind not in {"user", "guided_reply", "outbound_send"}:
                continue

            original_message = context_message.original_message
            if original_message is None:
                continue

            message_id = str(original_message.message_id or "").strip()
            if message_id:
                if message_id in seen_message_ids:
                    continue
                seen_message_ids.add(message_id)
            else:
                object_id = id(original_message)
                if object_id in seen_object_ids:
                    continue
                seen_object_ids.add(object_id)

            source_messages.append(original_message)

        return source_messages

    async def _learn_from_session_messages(self, pending_messages: list["SessionMessage"]) -> bool:
        learning_session_id = self._resolve_learning_session_id(pending_messages)
        if learning_session_id is None:
            logger.warning(f"行为学习已跳过：无法解析到有效聊天流，learner_session_id={self.session_id}")
            return False
        if learning_session_id != self.session_id:
            logger.info(
                f"行为学习会话 ID 已按真实消息修正: learner_session_id={self.session_id} "
                f"learning_session_id={learning_session_id}"
            )

        acquire_result = await behavior_learning_batch_gate.acquire(learning_session_id)
        if not acquire_result.acquired:
            if acquire_result.reason == "session_busy":
                logger.info(f"{learning_session_id} 已有行为学习批次正在运行，放弃新的批次")
            elif acquire_result.reason == "global_limit":
                logger.info(
                    f"行为学习全局并发已满，放弃新的批次: "
                    f"active={acquire_result.active_count}, max={acquire_result.max_count}, "
                    f"session_id={learning_session_id}"
                )
            else:
                logger.warning(
                    f"行为学习并发配置无效，放弃新的批次: "
                    f"max_expression_learner={acquire_result.max_count}, session_id={learning_session_id}"
                )
            return False

        try:
            return await self._run_learning_batch(
                pending_messages,
                learning_session_id=learning_session_id,
            )
        finally:
            await behavior_learning_batch_gate.release(learning_session_id)

    def _resolve_learning_session_id(self, messages: list["SessionMessage"]) -> Optional[str]:
        """根据真实消息解析本轮行为学习应该归属的会话 ID。"""

        from src.chat.message_receive.chat_manager import chat_manager

        candidates = [
            str(getattr(message, "session_id", "") or "").strip()
            for message in messages
            if str(getattr(message, "session_id", "") or "").strip()
        ]

        def session_exists(session_id: str) -> bool:
            if not session_id:
                return False
            return chat_manager.get_existing_session_by_session_id(session_id) is not None

        for session_id, _ in Counter(candidates).most_common():
            if session_exists(session_id):
                return session_id

        if session_exists(self.session_id):
            return self.session_id

        logger.warning(
            f"行为学习无法从真实消息中找到已注册聊天流，也无法确认 learner_session_id; "
            f"learner_session_id={self.session_id} "
            f"候选 session_id={dict(Counter(candidates))}"
        )
        return None

    async def _run_learning_batch(
        self,
        pending_messages: list["SessionMessage"],
        *,
        learning_session_id: str,
    ) -> bool:
        """执行已经获得并发闸门的行为学习批次。"""

        scene_segments = await self._analyze_learning_scene_segments(
            pending_messages,
            learning_session_id=learning_session_id,
        )
        if not scene_segments:
            logger.debug(f"{learning_session_id} 行为学习未形成可用场景片段，跳过本批次")
            return False

        scene_start_by_segment_id = {
            segment.segment_id: segment.profile.to_learning_start_text()
            for segment in scene_segments
            if segment.profile.to_learning_start_text()
        }
        primary_segment = scene_segments[0]
        scene_start = scene_start_by_segment_id.get(primary_segment.segment_id, "")
        if not scene_start:
            logger.debug(f"{learning_session_id} 行为学习未形成可用 start 场景，跳过本批次")
            return False

        prompt = load_prompt(
            "learn_behavior",
            bot_name=global_config.bot.nickname,
            chat_str="聊天记录将在后续多条 user message 中给出；请以每条消息中的 source_id 作为来源行编号。",
            scene_profile=self._format_scene_segments_for_prompt(scene_segments),
            scene_start="；".join(
                f"{segment.segment_id}: {scene_start_by_segment_id.get(segment.segment_id, '')}"
                for segment in scene_segments
                if scene_start_by_segment_id.get(segment.segment_id)
            ),
        )

        try:
            learning_messages = await self._build_multi_learning_messages(pending_messages, prompt)
            generation_result = await behavior_learn_model.generate_response_with_messages(
                lambda _client: learning_messages,
                options=LLMGenerationOptions(temperature=0.25),
            )
            response = generation_result.response or ""
            self._log_learning_context_preview(
                learning_messages,
                session_id=learning_session_id,
                source_message_count=len(pending_messages),
                output_content=response,
            )
        except Exception as exc:
            logger.error(f"学习行为表现失败: {exc}")
            return False

        parse_result = parse_behavior_response_with_diagnostics(
            response,
            scene_start=scene_start,
            scene_start_by_segment_id=scene_start_by_segment_id,
        )
        self._log_parse_diagnostics(
            learning_session_id=learning_session_id,
            response=response,
            parse_result=parse_result,
        )

        filter_result = self._filter_behavior_candidates(parse_result.candidates, pending_messages)
        behavior_candidates = filter_result.candidates
        logger.info(
            f"{learning_session_id} 行为学习过滤概览: "
            f"解析候选={len(parse_result.candidates)} "
            f"有效候选={len(behavior_candidates)} "
            f"跳过原因={filter_result.skipped_reasons}"
        )
        if not behavior_candidates:
            logger.info(
                f"{learning_session_id} 行为学习未抽取到有效候选: "
                f"模型输出预览={_compact_log_text(parse_result.diagnostics.normalized_response, max_length=1600)!r}"
            )
            return False

        wrote_pattern = False
        write_success_count = 0
        write_failed_count = 0
        for candidate in behavior_candidates[:12]:
            matched_segment = self._select_segment_for_candidate(candidate, scene_segments)
            candidate_scene_start = scene_start_by_segment_id.get(matched_segment.segment_id, scene_start)
            logger.info(
                f"{learning_session_id} 准备写入行为经验路径: "
                f"segment_id={matched_segment.segment_id} action={candidate.action} "
                f"outcome={candidate.outcome} source_ids={candidate.source_ids}"
            )
            path = upsert_behavior_pattern(
                trigger=candidate_scene_start,
                action=candidate.action,
                outcome=candidate.outcome,
                source_ids=candidate.source_ids,
                session_id=learning_session_id,
                scenario_profile=matched_segment.profile,
                scene_start=candidate_scene_start,
            )
            if path is None:
                write_failed_count += 1
                logger.warning(
                    f"{learning_session_id} 行为经验路径写入未成功: "
                    f"segment_id={matched_segment.segment_id} action={candidate.action} "
                    f"outcome={candidate.outcome} source_ids={candidate.source_ids}"
                )
                continue
            wrote_pattern = True
            write_success_count += 1
            logger.info(
                f"学习到行为经验路径 [ID: {path.id}]: "
                f"场景片段={matched_segment.segment_id} 场景={candidate_scene_start} "
                f"行为={candidate.action} 结果={candidate.outcome}"
            )

        logger.info(
            f"{learning_session_id} 行为学习写入概览: "
            f"有效候选={len(behavior_candidates)} "
            f"尝试写入={min(len(behavior_candidates), 12)} "
            f"成功={write_success_count} "
            f"失败={write_failed_count}"
        )

        if wrote_pattern:
            maintenance_result = behavior_pattern_maintenance.maybe_maintain_session(
                session_id=learning_session_id,
                force=True,
            )
            if maintenance_result.changed:
                logger.info(
                    f"{learning_session_id} 行为表现已完成学习后维护: "
                    f"衰减={maintenance_result.decayed_count} "
                    f"禁用={maintenance_result.disabled_count} "
                    f"合并={maintenance_result.merged_count}"
                )

            consolidation_result = await behavior_pattern_consolidator.consolidate_after_learning(learning_session_id)
            if consolidation_result.changed:
                logger.info(
                    f"{learning_session_id} 行为表现语义整合完成: "
                    f"建议={consolidation_result.suggestion_count} "
                    f"合并={consolidation_result.merged_count}"
                )

        return wrote_pattern

    def _log_parse_diagnostics(
        self,
        *,
        learning_session_id: str,
        response: str,
        parse_result: BehaviorParseResult,
    ) -> None:
        """输出行为学习结果解析阶段的详细诊断日志。"""

        diagnostics = parse_result.diagnostics
        response_preview = _compact_log_text(diagnostics.normalized_response or response, max_length=1600)
        logger.info(
            f"{learning_session_id} 行为学习解析概览: "
            f"原始长度={len(response or '')} "
            f"规范化长度={len(diagnostics.normalized_response)} "
            f"数组项={diagnostics.parsed_item_count} "
            f"解析候选={diagnostics.accepted_item_count} "
            f"无效项={diagnostics.invalid_item_count} "
            f"空输出={diagnostics.empty_output} "
            f"缺少场景={diagnostics.missing_scene_start} "
            f"非数组={diagnostics.non_list_output} "
            f"解析错误={diagnostics.parse_error or '无'} "
            f"输出预览={response_preview!r}"
        )
        for index, candidate in enumerate(parse_result.candidates[:12], start=1):
            logger.info(
                f"{learning_session_id} 行为学习解析候选[{index}]: "
                f"segment_id={candidate.segment_id or 'auto'} "
                f"action={candidate.action} outcome={candidate.outcome} source_ids={candidate.source_ids}"
            )

    @staticmethod
    def _format_scene_segments_for_prompt(segments: Sequence[BehaviorScenarioSegment]) -> str:
        """把场景片段压成学习 prompt 中稳定可引用的 JSON。"""

        return json.dumps(
            {"segments": [segment.to_prompt_payload() for segment in segments]},
            ensure_ascii=False,
            indent=2,
        )

    @staticmethod
    def _select_segment_for_candidate(
        candidate: BehaviorCandidate,
        segments: Sequence[BehaviorScenarioSegment],
    ) -> BehaviorScenarioSegment:
        """根据候选的 segment_id 或 source_ids 选择最贴近的场景片段。"""

        if not segments:
            return BehaviorScenarioSegment(segment_id="s1", title="主场景", profile=BehaviorScenarioProfile())

        if candidate.segment_id:
            for segment in segments:
                if segment.segment_id == candidate.segment_id:
                    return segment

        candidate_source_ids = set(candidate.source_ids)
        best_segment = segments[0]
        best_overlap = -1
        for segment in segments:
            segment_source_ids = set(segment.source_ids)
            overlap = len(candidate_source_ids & segment_source_ids)
            if overlap > best_overlap:
                best_overlap = overlap
                best_segment = segment
        return best_segment

    async def _analyze_learning_scene(
        self,
        messages: list["SessionMessage"],
        *,
        learning_session_id: str,
    ) -> BehaviorScenarioProfile:
        """在行为学习前，用同一套场景画像语言确定本批次的 start。"""

        context_text = await self._build_learning_context_text(messages)
        if not context_text:
            return BehaviorScenarioProfile()

        async def run_scene_prompt(prompt: str) -> str:
            generation_result = await behavior_scene_model.generate_response(
                prompt,
                options=LLMGenerationOptions(temperature=0.2),
            )
            response = generation_result.response or ""
            self._log_learning_scene_preview(
                prompt,
                session_id=learning_session_id,
                source_message_count=len(messages),
                output_content=response,
            )
            return response

        return await behavior_scenario_analyzer.analyze(
            context_text=context_text,
            sub_agent_runner=run_scene_prompt,
        )

    async def _analyze_learning_scene_segments(
        self,
        messages: list["SessionMessage"],
        *,
        learning_session_id: str,
    ) -> list[BehaviorScenarioSegment]:
        """在行为学习前，将同一学习窗口拆成 1~3 个可独立学习的场景片段。"""

        context_text = await self._build_learning_context_text(messages)
        if not context_text:
            return []

        async def run_scene_prompt(prompt: str) -> str:
            generation_result = await behavior_scene_model.generate_response(
                prompt,
                options=LLMGenerationOptions(temperature=0.2),
            )
            response = generation_result.response or ""
            self._log_learning_scene_preview(
                prompt,
                session_id=learning_session_id,
                source_message_count=len(messages),
                output_content=response,
            )
            return response

        segments = await behavior_scenario_analyzer.analyze_segments(
            context_text=context_text,
            sub_agent_runner=run_scene_prompt,
        )
        if segments:
            logger.info(
                f"{learning_session_id} 行为学习场景片段分析完成: "
                f"片段数={len(segments)} "
                f"片段={[{'id': segment.segment_id, 'sources': segment.source_ids, 'title': segment.title} for segment in segments]}"
            )
            return segments

        profile = await behavior_scenario_analyzer.analyze(
            context_text=context_text,
            sub_agent_runner=run_scene_prompt,
        )
        if not profile.has_signal:
            return []
        return [
            BehaviorScenarioSegment(
                segment_id="s1",
                title=profile.summary or "主场景",
                source_ids=[str(index) for index in range(1, len(messages) + 1)],
                profile=profile,
            )
        ]

    async def _build_learning_context_text(self, messages: list["SessionMessage"]) -> str:
        """构建场景分析用的紧凑学习窗口文本。"""

        context_lines: list[str] = []
        for index, message in enumerate(messages, start=1):
            await message.process()
            user_info = message.message_info.user_info
            speaker_kind = "SELF" if is_bot_self(message.platform, user_info.user_id) else "USER"
            content = " ".join((message.processed_plain_text or "").split()).strip()
            if not content:
                content = "[空消息]"
            if len(content) > 300:
                content = content[:300].rstrip() + "..."
            context_lines.append(
                "\n".join(
                    [
                        f"[source_id:{index}]",
                        f"[speaker:{speaker_kind}]",
                        f"[time:{message.timestamp.strftime('%H:%M:%S')}]",
                        "[content]",
                        content,
                    ]
                )
            )
        return "\n\n".join(context_lines).strip()

    async def _build_multi_learning_messages(
        self,
        messages: list["SessionMessage"],
        system_prompt: str,
    ) -> list[Message]:
        """构造行为学习使用的多 message 请求。"""

        learning_messages = [
            MessageBuilder()
            .set_role(RoleType.System)
            .add_text_content(
                f"{system_prompt}\n\n"
                "注意：聊天记录会在后续多条 user message 中给出。每条消息内的 source_id "
                "是本轮学习的来源编号；speaker=SELF 的消息可以作为行为链的一部分，"
                "但输出的行为表现不要直接写 SELF 或具体昵称。"
            )
            .build()
        ]

        for index, message in enumerate(messages, start=1):
            await message.process()
            user_info = message.message_info.user_info
            speaker_name = user_info.user_cardname or user_info.user_nickname or "未知用户"
            speaker_kind = "SELF" if is_bot_self(message.platform, user_info.user_id) else "USER"
            content = (message.processed_plain_text or "").strip()
            if not content:
                content = "[空消息]"
            learning_messages.append(
                MessageBuilder()
                .set_role(RoleType.User)
                .add_text_content(
                    "\n".join(
                        [
                            f"[source_id:{index}]",
                            f"[speaker:{speaker_kind}]",
                            f"[name:{speaker_name}]",
                            f"[time:{message.timestamp.strftime('%H:%M:%S')}]",
                            "[content]",
                            content,
                        ]
                    )
                )
                .build()
            )

        learning_messages.append(
            MessageBuilder()
            .set_role(RoleType.User)
            .add_text_content("请根据以上聊天消息输出 JSON。")
            .build()
        )
        return learning_messages

    def _log_learning_scene_preview(
        self,
        prompt: str,
        *,
        session_id: str,
        source_message_count: int,
        output_content: str,
    ) -> None:
        """保存行为学习前的场景画像请求预览。"""

        try:
            preview_access = PromptCLIVisualizer.build_prompt_preview_access(
                [
                    MessageBuilder()
                    .set_role(RoleType.User)
                    .add_text_content(prompt)
                    .build()
                ],
                category="behavior_scenario_analyzer",
                chat_id=session_id,
                request_kind="behavior_scenario_analyzer",
                selection_reason=(
                    f"会话ID: {session_id}\n"
                    f"Learner会话ID: {self.session_id}\n"
                    f"来源: behavior_learning_scene\n"
                    f"真实聊天消息数: {source_message_count}"
                ),
                output_content=output_content,
            )
        except Exception as exc:
            logger.warning(f"{self.session_id} 行为学习场景画像预览保存失败: {exc}")
            return

        logger.info(
            f"{self.session_id} 行为学习场景画像预览已生成: "
            f"WebUI={preview_access.viewer_web_uri} "
            f"HTML={preview_access.viewer_path} "
            f"JSON={preview_access.dump_path}"
        )

    def _log_learning_context_preview(
        self,
        messages: list[Message],
        *,
        session_id: str,
        source_message_count: int,
        output_content: str,
    ) -> None:
        """保存行为学习上下文预览，并在日志中输出查看入口。"""

        try:
            preview_access = PromptCLIVisualizer.build_prompt_preview_access(
                messages,
                category="behavior_learner",
                chat_id=session_id,
                request_kind="behavior_learner",
                selection_reason=(
                    f"会话ID: {session_id}\n"
                    f"Learner会话ID: {self.session_id}\n"
                    f"来源: trimmed_history\n"
                    f"真实聊天消息数: {source_message_count}\n"
                    f"构建消息数: {len(messages)}"
                ),
                output_content=output_content,
            )
        except Exception as exc:
            logger.warning(f"{self.session_id} 行为学习上下文预览保存失败: {exc}")
            return

        logger.info(
            f"{self.session_id} 行为学习上下文预览已生成: "
            f"WebUI={preview_access.viewer_web_uri} "
            f"HTML={preview_access.viewer_path} "
            f"TXT={preview_access.dump_path}"
        )

    def _filter_behavior_candidates(
        self,
        candidates: list[BehaviorCandidate],
        messages: list["SessionMessage"],
    ) -> BehaviorFilterResult:
        """过滤行为表现候选，确保来源行有效且内容可复用。"""

        filtered_candidates: list[BehaviorCandidate] = []
        skipped_reasons: Counter[str] = Counter()
        for candidate in candidates:
            if "SELF" in candidate.trigger or "SELF" in candidate.action or "SELF" in candidate.outcome:
                skipped_reasons["contains_self_literal"] += 1
                logger.info(
                    f"跳过包含 SELF 字面量的行为表现："
                    f"trigger={candidate.trigger}, action={candidate.action}, outcome={candidate.outcome}"
                )
                continue

            valid_source_ids: list[str] = []
            for source_id in candidate.source_ids:
                source_id_str = source_id.strip()
                if not source_id_str.isdigit():
                    continue
                line_index = int(source_id_str) - 1
                if line_index < 0 or line_index >= len(messages):
                    continue
                if source_id_str not in valid_source_ids:
                    valid_source_ids.append(source_id_str)
            if not valid_source_ids:
                skipped_reasons["invalid_source_ids"] += 1
                logger.info(
                    f"跳过来源无效的行为表现: "
                    f"action={candidate.action} outcome={candidate.outcome} source_ids={candidate.source_ids}"
                )
                continue

            has_source_text = any(
                (messages[int(source_id) - 1].processed_plain_text or "").strip()
                for source_id in valid_source_ids
            )
            if not has_source_text:
                skipped_reasons["empty_source_text"] += 1
                logger.info(
                    f"跳过来源为空的行为表现: "
                    f"action={candidate.action} outcome={candidate.outcome} source_ids={valid_source_ids}"
                )
                continue

            filtered_candidates.append(
                BehaviorCandidate(
                    trigger=candidate.trigger.strip(),
                    action=candidate.action.strip(),
                    outcome=candidate.outcome.strip(),
                    source_ids=valid_source_ids,
                    segment_id=candidate.segment_id.strip(),
                )
            )

        return BehaviorFilterResult(
            candidates=filtered_candidates,
            skipped_reasons=dict(skipped_reasons),
        )
