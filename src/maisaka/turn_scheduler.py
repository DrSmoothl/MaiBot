"""Maisaka 消息触发调度。"""

from datetime import datetime
from typing import Sequence, TYPE_CHECKING
import time

from src.chat.message_receive.message import SessionMessage
from src.chat.utils.utils import is_bot_self
from src.common.logger import get_logger
from src.maisaka.focus import focus_mode_manager
from src.maisaka.mode_policy import is_reply_necessity_trigger_enabled
from src.maisaka.reply_necessity import REPLY_NECESSITY_TRIGGER_SCORE, ReplyNecessityInput, score_reply_necessity

if TYPE_CHECKING:
    from src.maisaka.runtime import MaisakaHeartFlowChatting

logger = get_logger("maisaka_turn_scheduler")


class MessageTurnScheduler:
    """决定外部消息何时进入 Maisaka 内部循环。"""

    def __init__(self, runtime: "MaisakaHeartFlowChatting") -> None:
        self._runtime = runtime

    def _count_recent_self_replies(self, window_seconds: float = 300.0) -> int:
        """统计最近一段时间内麦麦自己已经同步进历史的发言数。"""
        now = datetime.now()
        recent_count = 0
        for message in reversed(self._runtime._chat_history):
            if (now - message.timestamp).total_seconds() > window_seconds:
                break
            if message.source == "guided_reply":
                recent_count += 1
        return recent_count

    def _count_consecutive_self_replies(self) -> int:
        """统计历史尾部连续的麦麦发言数，用于控制存在感。"""
        consecutive_count = 0
        for message in reversed(self._runtime._chat_history):
            if not message.count_in_context:
                continue
            if message.source == "guided_reply":
                consecutive_count += 1
                continue
            break
        return consecutive_count

    def score_reply_necessity(
        self,
        *,
        pending_messages: Sequence[SessionMessage],
        trigger_threshold: int,
    ) -> tuple[int, str]:
        """按当前 runtime 快照为待处理消息计算回复必要性评分。"""
        runtime = self._runtime
        external_messages = [
            message
            for message in pending_messages
            if not is_bot_self(message.platform, message.message_info.user_info.user_id)
        ]
        average_interval = runtime._get_recent_average_external_message_interval()
        if average_interval is not None and average_interval > 0:
            last_external_received_at = runtime._last_external_message_received_at or runtime._last_message_received_at
            idle_seconds = max(0.0, time.time() - last_external_received_at)
            idle_reached_average = idle_seconds >= average_interval
        else:
            idle_seconds = 0.0
            idle_reached_average = False

        score_result = score_reply_necessity(
            ReplyNecessityInput(
                texts=[(message.processed_plain_text or "").strip() for message in external_messages],
                pending_count=len(external_messages),
                trigger_threshold=trigger_threshold,
                has_at=any(message.is_at for message in external_messages),
                has_mention=any(message.is_mentioned for message in external_messages),
                is_group_chat=runtime.chat_stream.is_group_session,
                focus_active=runtime._is_focus_mode_active_for_current_chat(),
                recent_self_replies=self._count_recent_self_replies(),
                consecutive_self_replies=self._count_consecutive_self_replies(),
                effective_frequency=runtime._get_effective_reply_frequency(),
                idle_seconds=idle_seconds,
                idle_reached_average=idle_reached_average,
            )
        )
        return score_result.score, score_result.detail

    def should_trigger_by_reply_necessity(
        self,
        *,
        pending_messages: Sequence[SessionMessage],
        trigger_threshold: int,
        schedule_detail: str | None = None,
    ) -> bool:
        """判断新 Maisaka 是否应基于回复必要性进入 Planner。"""
        score, detail = self.score_reply_necessity(
            pending_messages=pending_messages,
            trigger_threshold=trigger_threshold,
        )
        decision = "进入Planner" if score >= REPLY_NECESSITY_TRIGGER_SCORE else "等待更多消息"
        schedule_detail_prefix = f"{schedule_detail} " if schedule_detail else ""
        logger.info(
            f"{self._runtime.log_prefix} 回复调度: {schedule_detail_prefix}"
            f"必要性: {detail} 评分阈值={REPLY_NECESSITY_TRIGGER_SCORE} 判定={decision}"
        )
        if score >= REPLY_NECESSITY_TRIGGER_SCORE:
            return True

        return False

    def _calculate_idle_compensation(
        self,
        *,
        pending_count: int,
        trigger_threshold: int,
    ) -> tuple[bool, float | None, str]:
        """在新消息不足阈值时，按空窗时间折算补齐触发条件，并返回下次检查延迟。

        空窗折算量被限制在 ``trigger_threshold - 1`` 以内，确保至少要有一条真实新消息
        才可能触发，杜绝纯靠沉默累积反复唤醒回复。
        """
        # 与下方折算封顶互为双保险：纯沉默（pending_count == 0）一律不触发。
        if pending_count < 1:
            return False, None, "pending=0，不允许纯沉默触发"

        runtime = self._runtime
        average_message_interval = runtime._get_recent_average_external_message_interval()
        if average_message_interval is None or average_message_interval <= 0:
            return False, None, "平均消息间隔不可用，无法进行空窗补偿"

        last_external_received_at = runtime._last_external_message_received_at or runtime._last_message_received_at
        idle_seconds = max(0.0, time.time() - last_external_received_at)
        # 即便空窗无限长，也不能让纯沉默跨过阈值。
        idle_equivalent_count = min(
            idle_seconds / average_message_interval,
            float(max(0, trigger_threshold - 1)),
        )
        equivalent_message_count = pending_count + idle_equivalent_count
        detail = (
            f"平均间隔={average_message_interval:.2f}s "
            f"空窗={idle_seconds:.2f}s "
            f"空窗折算={idle_equivalent_count:.2f} "
            f"等效消息数={equivalent_message_count:.2f}/{trigger_threshold}"
        )
        if equivalent_message_count >= trigger_threshold:
            return True, None, detail

        delay_seconds = max(0.0, (trigger_threshold - pending_count) * average_message_interval - idle_seconds)
        return False, delay_seconds, f"{detail} 延迟={delay_seconds:.2f}s"

    def schedule_message_turn(self) -> None:
        runtime = self._runtime
        if not focus_mode_manager.can_decide(
            runtime.session_id,
            is_group_chat=runtime.chat_stream.is_group_session,
        ):
            logger.debug(f"{runtime.log_prefix} 当前不在 focus 状态，跳过 Maisaka 决策调度")
            return

        if runtime._agent_state == runtime._STATE_WAIT:
            if not runtime._is_reply_frequency_silent():
                if runtime.chat_stream.is_group_session:
                    return
                logger.info(f"{runtime.log_prefix} 私聊 wait 期间收到新消息，结束等待并进入 Planner")
                runtime._enter_running_state()
            else:
                runtime._enter_stop_state()

        if runtime._message_turn_scheduled:
            return

        pending_count = runtime._get_pending_message_count()
        if pending_count <= 0:
            return

        effective_frequency = runtime._get_effective_reply_frequency()
        formatted_frequency = runtime._format_reply_frequency_for_display(effective_frequency)
        if runtime._is_reply_frequency_silent():
            logger.info(
                f"{runtime.log_prefix} 回复频率调度: 频率={formatted_frequency} "
                f"pending={pending_count} 判定=静默消费"
            )
            runtime._enqueue_message_turn()
            return

        if runtime._has_forced_turn_trigger():
            logger.info(
                f"{runtime.log_prefix} 回复频率调度: 频率={formatted_frequency} "
                f"pending={pending_count} 判定=强制触发"
            )
            runtime._enqueue_message_turn()
            return

        if runtime._idle_backoff.should_delay(pending_count):
            return

        trigger_threshold = runtime._get_message_trigger_threshold()
        schedule_detail = f"频率={formatted_frequency} pending={pending_count} 消息阈值={trigger_threshold}"
        if is_reply_necessity_trigger_enabled():
            if self.should_trigger_by_reply_necessity(
                pending_messages=runtime.message_cache[runtime._last_processed_index :],
                trigger_threshold=trigger_threshold,
                schedule_detail=schedule_detail,
            ):
                runtime._enqueue_message_turn()
            return

        logger.info(f"{runtime.log_prefix} 回复频率调度: {schedule_detail}")
        if pending_count >= trigger_threshold:
            logger.info(
                f"{runtime.log_prefix} 回复频率调度: pending={pending_count} 达到阈值={trigger_threshold} "
                "判定=进入Planner"
            )
            runtime._enqueue_message_turn()
            return

        idle_compensation_triggered, delay_seconds, idle_detail = self._calculate_idle_compensation(
            pending_count=pending_count,
            trigger_threshold=trigger_threshold,
        )
        if idle_compensation_triggered:
            logger.info(f"{runtime.log_prefix} 回复频率调度: {idle_detail} 判定=空窗补偿进入Planner")
            runtime._enqueue_message_turn()
            return

        if delay_seconds is not None:
            logger.info(f"{runtime.log_prefix} 回复频率调度: {idle_detail} 判定=延迟检查")
            runtime._defer_message_turn_check(delay_seconds)
            return

        logger.info(f"{runtime.log_prefix} 回复频率调度: {idle_detail} 判定=等待更多消息")
