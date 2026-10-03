"""
回复器服务模块

提供回复器相关的核心功能。
"""

import traceback
import time
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from rich.traceback import install

from src.chat.message_receive.chat_manager import BotChatSession
from src.chat.replyer.maisaka_generator import MaisakaReplyGenerator
from src.chat.replyer.replyer_manager import replyer_manager
from src.chat.utils.utils import process_llm_response_segments_async
from src.common.data_models.message_component_data_model import MessageSequence, TextComponent
from src.common.logger import get_logger
from src.core.types import ActionInfo

if TYPE_CHECKING:
    from src.common.data_models.llm_data_model import LLMGenerationDataModel
    from src.common.data_models.planned_action_data_models import PlannedAction
    from src.chat.message_receive.message import SessionMessage

install(extra_lines=3)

logger = get_logger("generator_service")


# =============================================================================
# 回复器获取函数
# =============================================================================


def _get_replyer(
    chat_stream: Optional[BotChatSession] = None,
    chat_id: Optional[str] = None,
    request_type: str = "replyer",
) -> Optional[MaisakaReplyGenerator]:
    """获取回复器对象"""
    if not chat_id and not chat_stream:
        raise ValueError("chat_stream 和 chat_id 不可均为空")
    try:
        logger.debug(
            f"[GeneratorService] 正在获取回复器，chat_id: {chat_id}, chat_stream: {'有' if chat_stream else '无'}"
        )
        return replyer_manager.get_replyer(
            chat_stream=chat_stream,
            chat_id=chat_id,
            request_type=request_type,
        )
    except Exception as e:
        logger.error(f"[GeneratorService] 获取回复器时发生意外错误: {e}", exc_info=True)
        traceback.print_exc()
        return None


def _extract_unknown_words(action_data: Optional[Dict[str, Any]]) -> Optional[List[str]]:
    if not action_data:
        return None

    unknown_words = action_data.get("unknown_words")
    if not isinstance(unknown_words, list):
        return None

    cleaned_words: List[str] = []
    for item in unknown_words:
        if isinstance(item, str) and (cleaned_item := item.strip()):
            cleaned_words.append(cleaned_item)

    return cleaned_words or None


async def _build_message_sequence(
    content: Optional[str],
    *,
    enable_splitter: bool,
    enable_chinese_typo: bool,
) -> tuple[Optional[MessageSequence], List[str]]:
    if not content:
        return None, []

    processed_segments = await process_llm_response_segments_async(
        content,
        enable_splitter=enable_splitter,
        enable_chinese_typo=enable_chinese_typo,
    )
    processed_output = [segment.text for segment in processed_segments]
    return MessageSequence(components=[TextComponent(text) for text in processed_output]), processed_output


# =============================================================================
# 回复生成函数
# =============================================================================


async def generate_reply(
    chat_stream: Optional[BotChatSession] = None,
    chat_id: Optional[str] = None,
    action_data: Optional[Dict[str, Any]] = None,
    reply_message: Optional["SessionMessage"] = None,
    think_level: int = 1,
    extra_info: str = "",
    reply_reason: str = "",
    available_actions: Optional[Dict[str, ActionInfo]] = None,
    chosen_actions: Optional[List["PlannedAction"]] = None,
    unknown_words: Optional[List[str]] = None,
    enable_splitter: bool = True,
    enable_chinese_typo: bool = True,
    request_type: str = "generator_api",
    from_plugin: bool = True,
    reply_time_point: Optional[float] = None,
) -> Tuple[bool, Optional["LLMGenerationDataModel"]]:
    """生成回复"""
    try:
        if reply_time_point is None:
            reply_time_point = time.time()

        logger.debug("[GeneratorService] 开始生成回复")
        replyer = _get_replyer(chat_stream, chat_id, request_type=request_type)
        if not replyer:
            logger.error("[GeneratorService] 无法获取回复器")
            return False, None

        if action_data:
            if not extra_info:
                extra_info = action_data.get("extra_info", "")
            if not reply_reason:
                reply_reason = action_data.get("reason", "")
            if unknown_words is None:
                unknown_words = _extract_unknown_words(action_data)

        success, llm_response = await replyer.generate_reply_with_context(
            extra_info=extra_info,
            available_actions=available_actions,
            chosen_actions=chosen_actions,
            reply_message=reply_message,
            reply_reason=reply_reason,
            unknown_words=unknown_words,
            think_level=think_level,
            from_plugin=from_plugin,
            stream_id=chat_stream.session_id if chat_stream else chat_id,
            reply_time_point=reply_time_point,
            log_reply=False,
        )
        if not success:
            logger.warning("[GeneratorService] 回复生成失败")
            return False, None
        reply_set, processed_output = await _build_message_sequence(
            llm_response.content,
            enable_splitter=enable_splitter,
            enable_chinese_typo=enable_chinese_typo,
        )
        llm_response.processed_output = processed_output
        llm_response.reply_set = reply_set
        logger.debug(
            f"[GeneratorService] 回复生成成功，生成了 {len(reply_set.components) if reply_set else 0} 个回复项"
        )

        return success, llm_response

    except ValueError as ve:
        raise ve

    except UserWarning as uw:
        logger.warning(f"[GeneratorService] 中断了生成: {uw}")
        return False, None

    except Exception as e:
        logger.error(f"[GeneratorService] 生成回复时出错: {e}")
        logger.error(traceback.format_exc())
        return False, None


async def rewrite_reply(
    chat_stream: Optional[BotChatSession] = None,
    reply_data: Optional[Dict[str, Any]] = None,
    chat_id: Optional[str] = None,
    enable_splitter: bool = True,
    enable_chinese_typo: bool = True,
    raw_reply: str = "",
    reason: str = "",
    reply_to: str = "",
    request_type: str = "generator_api",
) -> Tuple[bool, Optional["LLMGenerationDataModel"]]:
    """重写回复"""
    try:
        replyer = _get_replyer(chat_stream, chat_id, request_type=request_type)
        if not replyer:
            logger.error("[GeneratorService] 无法获取回复器")
            return False, None

        logger.info("[GeneratorService] 开始重写回复")

        if reply_data:
            raw_reply = raw_reply or reply_data.get("raw_reply", "")
            reason = reason or reply_data.get("reason", "")
            reply_to = reply_to or reply_data.get("reply_to", "")

        success, llm_response = await replyer.rewrite_reply_with_context(
            raw_reply=raw_reply,
            reason=reason,
            reply_to=reply_to,
        )
        reply_set, processed_output = _build_message_sequence(
            llm_response.content if success and llm_response else None,
            enable_splitter=enable_splitter,
            enable_chinese_typo=enable_chinese_typo,
        )
        if llm_response is not None:
            llm_response.processed_output = processed_output
        llm_response.reply_set = reply_set
        if success:
            logger.info(
                f"[GeneratorService] 重写回复成功，生成了 {len(reply_set.components) if reply_set else 0} 个回复项"
            )
        else:
            logger.warning("[GeneratorService] 重写回复失败")

        return success, llm_response

    except ValueError as ve:
        raise ve

    except Exception as e:
        logger.error(f"[GeneratorService] 重写回复时出错: {e}")
        return False, None


