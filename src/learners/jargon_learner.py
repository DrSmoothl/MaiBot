from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

import asyncio
import re

from src.chat.utils.utils import is_bot_self
from src.common.data_models.llm_service_data_models import LLMGenerationOptions
from src.common.logger import get_logger
from src.config.config import global_config
from src.llm_models.payload_content.message import Message, MessageBuilder, RoleType
from src.maisaka.display.prompt_cli_renderer import PromptCLIVisualizer
from src.prompt.prompt_manager import prompt_manager
from src.services.llm_service import LLMServiceClient

from .expression_utils import parse_jargon_response
from .jargon_miner import JargonEntry, JargonMiner

if TYPE_CHECKING:
    from src.chat.message_receive.message import SessionMessage
    from src.maisaka.context.messages import LLMContextMessage


logger = get_logger("jargon_learner")

jargon_learn_model = LLMServiceClient(task_name="learner", request_type="jargon.learner")


@dataclass(frozen=True)
class JargonLearningAcquireResult:
    """黑话学习批次并发闸门的申请结果。"""

    acquired: bool
    reason: str = ""
    active_count: int = 0
    max_count: int = 0


class JargonLearningBatchGate:
    """控制黑话学习批次的聊天流互斥与全局并发上限。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._active_session_ids: set[str] = set()

    async def acquire(self, session_id: str) -> JargonLearningAcquireResult:
        max_count = int(global_config.expression.max_expression_learner)
        if max_count <= 0:
            return JargonLearningAcquireResult(False, "max_expression_learner <= 0", 0, max_count)

        async with self._lock:
            active_count = len(self._active_session_ids)
            if session_id in self._active_session_ids:
                return JargonLearningAcquireResult(False, "session_busy", active_count, max_count)
            if active_count >= max_count:
                return JargonLearningAcquireResult(False, "global_limit", active_count, max_count)

            self._active_session_ids.add(session_id)
            return JargonLearningAcquireResult(True, active_count=active_count + 1, max_count=max_count)

    async def release(self, session_id: str) -> None:
        async with self._lock:
            self._active_session_ids.discard(session_id)


jargon_learning_batch_gate = JargonLearningBatchGate()


class JargonLearner:
    def __init__(self, session_id: str) -> None:
        """初始化黑话学习器。

        Args:
            session_id: 当前会话 ID。
        """

        self.session_id = session_id
        self.min_messages_for_extraction = 10

    @staticmethod
    def _get_session_display_name(session_id: str) -> str:
        """获取聊天流展示名称，无法解析时回退到 session_id。"""

        from src.chat.message_receive.chat_manager import chat_manager

        session_name = chat_manager.get_session_name(session_id)
        if session_name:
            return session_name

        chat_manager.get_existing_session_by_session_id(session_id)
        return chat_manager.get_session_name(session_id) or session_id

    async def learn_from_context_messages(
        self,
        context_messages: Sequence["LLMContextMessage"],
        jargon_miner: JargonMiner,
    ) -> bool:
        """从 Maisaka 被裁切的上下文消息中学习黑话候选。"""

        source_messages = self._extract_session_messages_from_context(context_messages)
        if not source_messages:
            logger.debug("裁切历史中没有可用于黑话学习的真实聊天消息")
            return False
        if len(source_messages) < self.min_messages_for_extraction:
            logger.debug(
                f"裁切历史可学习消息不足: 可学习={len(source_messages)} 阈值={self.min_messages_for_extraction}"
            )
            return False

        return await self._learn_from_session_messages(source_messages, jargon_miner=jargon_miner)

    @staticmethod
    def _extract_session_messages_from_context(
        context_messages: Sequence["LLMContextMessage"],
    ) -> List["SessionMessage"]:
        """从上下文消息中过滤出真实聊天消息。"""

        from src.maisaka.context.messages import SessionBackedMessage

        source_messages: List["SessionMessage"] = []
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

    async def _learn_from_session_messages(
        self,
        pending_messages: List["SessionMessage"],
        *,
        jargon_miner: JargonMiner,
    ) -> bool:
        """对一批真实会话消息执行黑话学习。"""

        learning_session_id = self._resolve_learning_session_id(pending_messages)
        if learning_session_id is None:
            logger.warning(f"黑话学习已跳过：无法解析到有效聊天流，learner_session_id={self.session_id}")
            return False
        if learning_session_id != self.session_id:
            logger.info(
                f"黑话学习会话 ID 已按真实消息修正: learner_session_id={self.session_id} "
                f"learning_session_id={learning_session_id}"
            )

        acquire_result = await jargon_learning_batch_gate.acquire(learning_session_id)
        if not acquire_result.acquired:
            if acquire_result.reason == "session_busy":
                logger.info(f"{learning_session_id} 已有黑话学习批次正在运行，放弃新的批次")
            elif acquire_result.reason == "global_limit":
                logger.info(
                    f"黑话学习全局并发已满，放弃新的批次: "
                    f"active={acquire_result.active_count}, max={acquire_result.max_count}, "
                    f"session_id={learning_session_id}"
                )
            else:
                logger.warning(
                    f"黑话学习并发配置无效，放弃新的批次: "
                    f"max_expression_learner={acquire_result.max_count}, session_id={learning_session_id}"
                )
            return False

        try:
            return await self._run_learning_batch(
                pending_messages,
                learning_session_id=learning_session_id,
                jargon_miner=jargon_miner,
            )
        finally:
            await jargon_learning_batch_gate.release(learning_session_id)

    async def _run_learning_batch(
        self,
        pending_messages: List["SessionMessage"],
        *,
        learning_session_id: str,
        jargon_miner: JargonMiner,
    ) -> bool:
        """执行已经获得并发闸门的黑话学习批次。"""

        readable_message = "聊天记录将在后续多条 user message 中给出；请以每条消息中的 source_id 作为来源行编号。"
        prompt_template = prompt_manager.get_prompt("learn_jargon")
        prompt_template.add_context("bot_name", global_config.bot.nickname)
        prompt_template.add_context("chat_str", readable_message)
        prompt = await prompt_manager.render_prompt(prompt_template)

        try:
            learning_messages = await self._build_multi_learning_messages(pending_messages, prompt)
            generation_result = await jargon_learn_model.generate_response_with_messages(
                lambda _client: learning_messages,
                options=LLMGenerationOptions(temperature=0.3),
                session_id=learning_session_id,
            )
            self._log_learning_context_preview(
                learning_messages,
                session_id=learning_session_id,
                source_message_count=len(pending_messages),
                source_type="trimmed_history",
                output_content=generation_result.response or "",
            )
            response = generation_result.response
        except Exception as e:
            logger.error(f"学习黑话失败: {e}")
            return False

        jargon_entries = parse_jargon_response(response)
        cached_jargon_entries = self._check_cached_jargons_in_messages(pending_messages, jargon_miner)
        if cached_jargon_entries:
            existing_contents = {content for content, _ in jargon_entries}
            for content, source_id in cached_jargon_entries:
                if content in existing_contents:
                    continue
                jargon_entries.append((content, source_id))
                existing_contents.add(content)
                logger.info(f"从缓存中找到黑话: {content}")

        if not jargon_entries:
            logger.info("没有可学习的黑话")
            return False

        original_jargon_session_id = jargon_miner.session_id
        original_jargon_session_name = jargon_miner.session_name
        if learning_session_id != original_jargon_session_id:
            jargon_miner.session_id = learning_session_id
            jargon_miner.session_name = self._get_session_display_name(learning_session_id)
        try:
            return await self._process_jargon_entries(jargon_entries, pending_messages, jargon_miner)
        finally:
            jargon_miner.session_id = original_jargon_session_id
            jargon_miner.session_name = original_jargon_session_name

    def _resolve_learning_session_id(self, messages: List["SessionMessage"]) -> Optional[str]:
        """根据真实消息解析本轮黑话学习应该归属的会话 ID。"""

        from src.chat.message_receive.chat_manager import chat_manager

        candidates = [
            str(message.session_id or "").strip()
            for message in messages
            if str(message.session_id or "").strip()
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
            f"黑话学习无法从真实消息中找到已注册聊天流，也无法确认 learner_session_id; "
            f"learner_session_id={self.session_id} "
            f"候选 session_id={dict(Counter(candidates))}"
        )
        return None

    async def _build_multi_learning_messages(
        self,
        messages: List["SessionMessage"],
        system_prompt: str,
    ) -> List[Message]:
        """构造黑话学习使用的多 message 请求。"""

        learning_messages = [
            MessageBuilder()
            .set_role(RoleType.System)
            .add_text_content(
                f"{system_prompt}\n\n"
                "注意：聊天记录会在后续多条 user message 中给出。每条消息内的 source_id "
                "是本轮学习的来源编号；speaker=SELF 的消息只作为上下文，不要从 SELF 的发言中学习。"
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

    def _log_learning_context_preview(
        self,
        messages: List[Message],
        *,
        session_id: str,
        source_message_count: int,
        source_type: str,
        output_content: str,
    ) -> None:
        """保存黑话学习上下文预览，并在日志中输出查看入口。"""

        try:
            preview_access = PromptCLIVisualizer.build_prompt_preview_access(
                messages,
                category="jargon_learner",
                chat_id=session_id,
                request_kind="jargon_learner",
                selection_reason=(
                    f"会话ID: {session_id}\n"
                    f"Learner会话ID: {self.session_id}\n"
                    f"来源: {source_type}\n"
                    f"真实聊天消息数: {source_message_count}\n"
                    f"构建消息数: {len(messages)}"
                ),
                output_content=output_content,
            )
        except Exception as exc:
            logger.warning(f"{self.session_id} 黑话学习上下文预览保存失败: {exc}")
            return

        logger.info(
            f"{self.session_id} 黑话学习上下文预览已生成: "
            f"WebUI={preview_access.preview_web_uri} "
            f"JSON={preview_access.record_path}"
        )

    def _check_cached_jargons_in_messages(
        self,
        messages: List["SessionMessage"],
        jargon_miner: JargonMiner,
    ) -> List[Tuple[str, str]]:
        """检查缓存中的黑话是否出现在 messages 中。"""

        cached_jargons = jargon_miner.get_cached_jargons()
        if not cached_jargons:
            return []

        matched_entries: List[Tuple[str, str]] = []

        for i, msg in enumerate(messages):
            # 跳过机器人自己的消息
            if is_bot_self(msg.platform, msg.message_info.user_info.user_id):
                continue

            msg_text = (msg.processed_plain_text or "").strip()
            if not msg_text:
                continue

            for jargon in cached_jargons:
                if not jargon or not jargon.strip():
                    continue

                jargon_content = jargon.strip()
                pattern = re.escape(jargon_content)
                if re.search(r"[\u4e00-\u9fff]", jargon_content):
                    search_pattern = pattern
                else:
                    search_pattern = r"\b" + pattern + r"\b"

                if re.search(search_pattern, msg_text, re.IGNORECASE):
                    matched_entries.append((jargon_content, str(i + 1)))

        return matched_entries

    async def _process_jargon_entries(
        self,
        jargon_entries: List[Tuple[str, str]],
        messages: List["SessionMessage"],
        jargon_miner: JargonMiner,
    ) -> bool:
        """处理黑话条目，并路由到 JargonMiner。"""

        if not jargon_entries or not messages:
            return False

        entries: List[JargonEntry] = []

        for content, source_id in jargon_entries:
            content = content.strip()
            if not content:
                continue

            if "SELF" in content:
                logger.info(f"跳过包含 SELF 的黑话：{content}")
                continue

            # TODO: 多平台兼容
            bot_nickname = global_config.bot.nickname
            if bot_nickname and bot_nickname in content:
                logger.info(f"跳过包含机器人昵称的黑话：{content}")
                continue

            if not source_id.isdigit():
                logger.warning(f"黑话条目 source_id 无效：content={content}, source_id={source_id}")
                continue

            line_index = int(source_id) - 1
            if line_index < 0 or line_index >= len(messages):
                logger.warning(f"黑话条目 source_id 超出范围：content={content}, source_id={source_id}")
                continue

            target_msg = messages[line_index]
            if is_bot_self(target_msg.platform, target_msg.message_info.user_info.user_id):
                logger.info(f"跳过引用机器人自身消息的黑话：content={content}, source_id={source_id}")
                continue

            start_idx = max(0, line_index - 3)
            end_idx = min(len(messages), line_index + 4)
            context_msgs = messages[start_idx:end_idx]

            context_paragraph = "\n".join(
                [f"[{i + 1}] {msg.processed_plain_text or ''}" for i, msg in enumerate(context_msgs)]
            )
            if not context_paragraph:
                logger.warning(f"黑话条目上下文为空：content={content}, source_id={source_id}")
                continue

            entries.append({"content": content, "raw_content": {context_paragraph}})

        if not entries:
            return False

        saved, updated = await jargon_miner.process_extracted_entries(entries)
        logger.info(f"成功处理 {len(entries)} 个黑话条目")
        return saved + updated > 0
