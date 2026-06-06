"""reply 内置工具。"""

from typing import Any, Optional

import traceback

from src.chat.replyer.replyer_manager import replyer_manager
from src.cli.maisaka_cli_sender import CLI_PLATFORM_NAME, render_cli_message
from src.common.data_models.reply_generation_data_models import ReplyGenerationResult
from src.common.logger import get_logger
from src.config import config as config_module
from src.core.tooling import ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec
from src.maisaka.context_messages import ToolResultMessage
from src.maisaka.heuristic_memory_injector import merge_heuristic_memory_reference_for_replyer
from src.maisaka.message_adapter import build_visible_text_from_sequence
from src.services import send_service

from .context import BuiltinToolRuntimeContext
from .query_memory import REPLYER_MEMORY_REFERENCE_MARKER

logger = get_logger("maisaka_builtin_reply")


async def _run_expression_selector(tool_ctx: BuiltinToolRuntimeContext, system_prompt: str) -> str:
    """运行 replyer 侧表达方式选择子代理，并返回文本结果。"""
    response = await tool_ctx.runtime.run_sub_agent(
        context_message_limit=10,
        system_prompt=system_prompt,
        request_kind="expression_selector",
    )
    return (response.content or "").strip()


def get_tool_spec() -> ToolSpec:
    """获取 reply 工具声明。"""

    return ToolSpec(
        name="reply",
        description="根据当前思考生成并发送一条可见回复。",
        parameters_schema={
            "type": "object",
            "properties": {
                "msg_id": {
                    "type": "string",
                    "description": "要回复的目标用户消息编号。",
                },
                "set_quote": {
                    "type": "boolean",
                    "description": "以引用回复的方式发送这条回复，不用每句都引用。",
                    "default": True,
                },
                "reference_info": {
                    "type": "string",
                    "description": "有助于回复的信息，之前搜集得到的事实性信息，记忆等，使用平文本格式。",
                    "default": True,
                },
            },
            "required": ["msg_id"],
        },
        provider_name="maisaka_builtin",
        provider_type="builtin",
    )


def _build_monitor_metadata(reply_result: ReplyGenerationResult) -> dict[str, object]:
    """从 reply 结果中提取统一监控详情。"""

    monitor_detail = reply_result.monitor_detail
    if isinstance(monitor_detail, dict):
        return {"monitor_detail": monitor_detail}
    return {}


def _build_send_result(
    *,
    index: int,
    segment: str,
    set_quote: bool,
    success: bool,
    message_id: str = "",
) -> dict[str, Any]:
    """构建分段回复的轻量发送结果。"""

    return {
        "index": index,
        "segment": segment,
        "set_quote": set_quote,
        "success": success,
        "message_id": message_id,
    }


def _get_selected_expression_habits(reply_result: ReplyGenerationResult) -> str:
    """读取 replyer 本轮实际使用的表达方式说明。"""

    extra = reply_result.metrics.extra
    if not isinstance(extra, dict):
        return ""
    return str(extra.get("selected_expression_habits") or "").strip()


def _get_context_message_id(message: Any) -> str:
    """读取上下文消息对应的真实消息编号。"""

    message_id = str(getattr(message, "message_id", "") or "").strip()
    if message_id:
        return message_id

    original_message = getattr(message, "original_message", None)
    return str(getattr(original_message, "message_id", "") or "").strip()


def _find_target_message_index(chat_history: list[Any], target_message_id: str) -> int:
    """定位当前回复目标在上下文历史中的位置。"""

    if not target_message_id:
        return -1
    for index in range(len(chat_history) - 1, -1, -1):
        if _get_context_message_id(chat_history[index]) == target_message_id:
            return index
    return -1


def _collect_replyer_memory_references(chat_history: list[Any], *, target_message_id: str) -> list[str]:
    """收集目标消息之后 query_memory 留给 replyer 的内部参考。"""

    target_index = _find_target_message_index(chat_history, target_message_id)
    if target_index < 0:
        return []

    references: list[str] = []
    seen_references: set[str] = set()
    for message in chat_history[target_index + 1 :]:
        if not isinstance(message, ToolResultMessage):
            continue
        if message.tool_name != "query_memory" or not message.success:
            continue
        metadata = message.metadata if isinstance(message.metadata, dict) else {}
        reference = str(metadata.get("replyer_memory_reference") or "").strip()
        if not reference or reference in seen_references:
            continue
        seen_references.add(reference)
        references.append(reference)
    return references


def _merge_query_memory_reference_for_replyer(
    *,
    reference_info: str,
    chat_history: list[Any],
    target_message_id: str,
) -> str:
    """把本轮 query_memory 结果合并给 replyer。"""

    existing = str(reference_info or "").strip()
    if REPLYER_MEMORY_REFERENCE_MARKER in existing:
        return existing

    references = _collect_replyer_memory_references(chat_history, target_message_id=target_message_id)
    if not references:
        return existing

    memory_reference = "\n\n".join(references)
    if not existing:
        return memory_reference
    return f"{existing}\n\n{memory_reference}"


async def handle_tool(
    tool_ctx: BuiltinToolRuntimeContext,
    invocation: ToolInvocation,
    context: Optional[ToolExecutionContext] = None,
) -> ToolExecutionResult:
    """执行 reply 内置工具。"""

    latest_thought = context.reasoning if context is not None else invocation.reasoning
    raw_reference_info = str(invocation.arguments.get("reference_info") or "").strip()
    target_message_id = str(invocation.arguments.get("msg_id") or "").strip()
    set_quote = bool(invocation.arguments.get("set_quote", True))
    reply_tool_args = {
        key: value
        for key, value in dict(invocation.arguments or {}).items()
        if key not in {"msg_id", "set_quote", "reference_info"}
    }
    enable_reply_quote = bool(config_module.global_config.chat.enable_reply_quote)
    effective_set_quote = set_quote and enable_reply_quote

    if not target_message_id:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "reply 工具需要提供有效的 `msg_id` 参数。",
        )

    target_message = tool_ctx.runtime.find_source_message_by_id(target_message_id)
    if target_message is None:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            f"未找到要回复的目标消息，msg_id={target_message_id}",
        )

    try:
        replyer = replyer_manager.get_replyer(
            chat_stream=tool_ctx.runtime.chat_stream,
            request_type="maisaka_replyer",
            replyer_type="maisaka",
        )
    except Exception:
        logger.exception(
            f"{tool_ctx.runtime.log_prefix} 获取回复生成器时发生异常: 目标消息编号={target_message_id}"
        )
        logger.info(traceback.format_exc())
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "获取 Maisaka 回复生成器时发生异常。",
        )

    if replyer is None:
        logger.error(f"{tool_ctx.runtime.log_prefix} 获取 Maisaka 回复生成器失败")
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "Maisaka 回复生成器当前不可用。",
        )

    replyer_chat_history = list(tool_ctx.runtime._chat_history)
    reference_info_with_memory = _merge_query_memory_reference_for_replyer(
        reference_info=raw_reference_info,
        chat_history=replyer_chat_history,
        target_message_id=target_message_id,
    )
    reference_info = merge_heuristic_memory_reference_for_replyer(
        session_id=str(tool_ctx.runtime.session_id or ""),
        reference_info=reference_info_with_memory,
    )

    try:
        success, reply_result = await replyer.generate_reply_with_context(
            reply_reason=latest_thought,
            reference_info=reference_info,
            stream_id=tool_ctx.runtime.session_id,
            reply_message=target_message,
            chat_history=replyer_chat_history,
            reply_tool_args=reply_tool_args,
            sub_agent_runner=lambda system_prompt: _run_expression_selector(
                tool_ctx,
                system_prompt,
            ),
            log_reply=False,
        )
    except Exception as exc:
        logger.exception(
            f"{tool_ctx.runtime.log_prefix} 回复生成器执行异常: 目标消息编号={target_message_id} 异常={exc}"
        )
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "生成可见回复时发生异常。",
        )

    reply_metadata = _build_monitor_metadata(reply_result)
    reply_text = reply_result.completion.response_text.strip() if success else ""
    if not reply_text:
        logger.warning(
            f"{tool_ctx.runtime.log_prefix} 回复生成器返回空文本: "
            f"目标消息编号={target_message_id} 错误信息={reply_result.error_message!r}"
        )
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "生成可见回复失败。",
            metadata=reply_metadata,
        )

    reply_sequences = await tool_ctx.post_process_reply_message_sequences_async(reply_text)
    reply_segments = [build_visible_text_from_sequence(sequence) for sequence in reply_sequences]
    combined_reply_text = "".join(reply_segments)
    sent_message_ids: list[str] = []
    send_results: list[dict[str, Any]] = []
    try:
        sent = False
        if tool_ctx.runtime.chat_stream.platform == CLI_PLATFORM_NAME:
            for index, segment in enumerate(reply_segments):
                render_cli_message(segment)
                send_results.append(
                    _build_send_result(
                        index=index,
                        segment=segment,
                        set_quote=effective_set_quote if index == 0 else False,
                        success=True,
                    )
                )
            sent = True
        else:
            for index, reply_sequence in enumerate(reply_sequences):
                segment = reply_segments[index]
                segment_set_quote = effective_set_quote if index == 0 else False
                sent_message = await send_service._send_to_target_with_message(
                    message_sequence=reply_sequence,
                    stream_id=tool_ctx.runtime.session_id,
                    processed_plain_text=segment,
                    set_reply=segment_set_quote,
                    reply_message=target_message if segment_set_quote else None,
                    selected_expressions=reply_result.selected_expression_ids or None,
                    typing=index > 0,
                    sync_to_maisaka_history=True,
                    maisaka_source_kind="guided_reply",
                )
                sent = sent_message is not None
                if not sent:
                    send_results.append(
                        _build_send_result(
                            index=index,
                            segment=segment,
                            set_quote=segment_set_quote,
                            success=False,
                        )
                    )
                    break
                sent_message_id = str(getattr(sent_message, "message_id", "") or "").strip()
                if sent_message_id:
                    sent_message_ids.append(sent_message_id)
                send_results.append(
                    _build_send_result(
                        index=index,
                        segment=segment,
                        set_quote=segment_set_quote,
                        success=True,
                        message_id=sent_message_id,
                    )
                )
    except Exception:
        logger.exception(
            f"{tool_ctx.runtime.log_prefix} 发送文字消息时发生异常，目标消息编号={target_message_id}"
        )
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "发送可见回复时发生异常。",
            metadata=reply_metadata,
        )

    if not sent:
        return tool_ctx.build_failure_result(
            invocation.tool_name,
            "可见回复生成成功，但发送失败。",
            structured_content={
                "msg_id": target_message_id,
                "set_quote": set_quote,
                "effective_set_quote": effective_set_quote,
                "reply_segments": reply_segments,
                "send_results": send_results,
            },
            metadata=reply_metadata,
        )

    target_user_info = target_message.message_info.user_info
    target_user_name = target_user_info.user_cardname or target_user_info.user_nickname or target_user_info.user_id
    bot_name = config_module.global_config.bot.nickname.strip() or "MaiSaka"

    if tool_ctx.runtime.chat_stream.platform == CLI_PLATFORM_NAME:
        tool_ctx.append_guided_reply_to_chat_history(combined_reply_text)
    tool_ctx.append_replyer_expression_annotation(
        selected_expression_ids=reply_result.selected_expression_ids,
        expression_habits=_get_selected_expression_habits(reply_result),
    )
    reply_metadata["sent_message_ids"] = sent_message_ids
    reply_metadata["send_results"] = send_results
    track_reply_effect = getattr(tool_ctx.runtime, "track_reply_effect", None)
    if track_reply_effect is not None:
        await track_reply_effect(
            tool_call_id=invocation.call_id,
            target_message=target_message,
            set_quote=effective_set_quote,
            reply_text=combined_reply_text,
            reply_segments=reply_segments,
            planner_reasoning=latest_thought,
            reference_info=reference_info,
            tool_context={
                "tool_name": invocation.tool_name,
                "call_id": invocation.call_id,
                "arguments": dict(invocation.arguments or {}),
                "reasoning": latest_thought,
            },
            send_results=send_results,
            reply_metadata=reply_metadata,
            replyer_context_messages=replyer_chat_history,
        )
    return tool_ctx.build_success_result(
        invocation.tool_name,
        f'"{bot_name}"已生成并向"{target_user_name}"发送了回复"{combined_reply_text}"',
        structured_content={
            "msg_id": target_message_id,
            "set_quote": set_quote,
            "effective_set_quote": effective_set_quote,
            "reply_text": combined_reply_text,
            "reply_segments": reply_segments,
            "send_results": send_results,
            "target_user_name": target_user_name,
        },
        metadata=reply_metadata,
    )
