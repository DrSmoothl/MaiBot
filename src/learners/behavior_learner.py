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
from .behavior_scenario import BehaviorScenarioProfile, behavior_scenario_analyzer

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


def _parse_behavior_item(raw_item: Any, *, scene_start: str) -> Optional[BehaviorCandidate]:
    if not isinstance(raw_item, dict):
        return None

    action = str(raw_item.get("action") or "").strip()
    outcome = str(raw_item.get("outcome") or "").strip()
    source_ids = _coerce_source_ids(raw_item.get("source_ids"))
    trigger = scene_start.strip()
    if not trigger or not action or not outcome:
        return None
    return BehaviorCandidate(trigger=trigger, action=action, outcome=outcome, source_ids=source_ids)


def parse_behavior_response(response: str, *, scene_start: str) -> list[BehaviorCandidate]:
    """解析行为学习模型返回的 JSON。"""

    normalized_response = _strip_json_code_fence(response or "")
    normalized_scene_start = scene_start.strip()
    if not normalized_response:
        return []
    if not normalized_scene_start:
        return []

    try:
        parsed_response = json.loads(repair_json(normalized_response))
    except Exception:
        logger.warning(f"行为学习结果解析失败: {normalized_response!r}")
        return []

    if not isinstance(parsed_response, list):
        return []

    candidates: list[BehaviorCandidate] = []
    for raw_item in parsed_response:
        candidate = _parse_behavior_item(raw_item, scene_start=normalized_scene_start)
        if candidate is not None:
            candidates.append(candidate)
    return candidates


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

        scene_profile = await self._analyze_learning_scene(
            pending_messages,
            learning_session_id=learning_session_id,
        )
        scene_start = scene_profile.to_learning_start_text()
        if not scene_start:
            logger.debug(f"{learning_session_id} 行为学习未形成可用场景画像，跳过本批次")
            return False

        prompt = load_prompt(
            "learn_behavior",
            bot_name=global_config.bot.nickname,
            chat_str="聊天记录将在后续多条 user message 中给出；请以每条消息中的 source_id 作为来源行编号。",
            scene_profile=scene_profile.to_prompt_text(),
            scene_start=scene_start,
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

        candidates = parse_behavior_response(response, scene_start=scene_start)
        behavior_candidates = self._filter_behavior_candidates(candidates, pending_messages)
        if not behavior_candidates:
            logger.debug(f"{learning_session_id} 行为学习未抽取到有效候选")
            return False

        wrote_pattern = False
        for candidate in behavior_candidates[:12]:
            pattern = upsert_behavior_pattern(
                trigger=candidate.trigger,
                action=candidate.action,
                outcome=candidate.outcome,
                source_ids=candidate.source_ids,
                session_id=learning_session_id,
            )
            if pattern is None:
                continue
            wrote_pattern = True
            logger.info(
                f"学习到行为表现 [ID: {pattern.id}]: "
                f"场景={candidate.trigger} 行为={candidate.action} 结果={candidate.outcome}"
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
    ) -> list[BehaviorCandidate]:
        """过滤行为表现候选，确保来源行有效且内容可复用。"""

        filtered_candidates: list[BehaviorCandidate] = []
        for candidate in candidates:
            if "SELF" in candidate.trigger or "SELF" in candidate.action or "SELF" in candidate.outcome:
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
                logger.debug(f"跳过来源无效的行为表现：{candidate}")
                continue

            has_source_text = any(
                (messages[int(source_id) - 1].processed_plain_text or "").strip()
                for source_id in valid_source_ids
            )
            if not has_source_text:
                logger.debug(f"跳过来源为空的行为表现：{candidate}")
                continue

            filtered_candidates.append(
                BehaviorCandidate(
                    trigger=candidate.trigger.strip(),
                    action=candidate.action.strip(),
                    outcome=candidate.outcome.strip(),
                    source_ids=valid_source_ids,
                )
            )

        return filtered_candidates
