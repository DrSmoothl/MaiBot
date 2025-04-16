import asyncio
import time
from typing import Dict, List, Optional, Union

from src.common.logger import get_module_logger
from ....common.database import db
from ...message.api import global_api
from ...message import MessageSending, MessageThinking, MessageSet

from ...storage.storage import MessageStorage
from ....config.config import global_config
from ...chat.utils import truncate_message, calculate_typing_time, count_messages_between

from src.common.logger import LogConfig, SENDER_STYLE_CONFIG

# 定义日志配置
sender_config = LogConfig(
    # 使用消息发送专用样式
    console_format=SENDER_STYLE_CONFIG["console_format"],
    file_format=SENDER_STYLE_CONFIG["file_format"],
)

logger = get_module_logger("msg_sender", config=sender_config)


class MessageSender:
    """发送器"""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MessageSender, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        # 确保 __init__ 只被调用一次
        if not hasattr(self, '_initialized'):
            self.message_interval = (0.5, 1)  # 消息间隔时间范围(秒)
            self.last_send_time = 0
            self._current_bot = None
            self._initialized = True

    def set_bot(self, bot):
        """设置当前bot实例"""
        pass


    async def send_via_ws(self, message: MessageSending) -> None:
        try:
            await global_api.send_message(message)
        except Exception as e:
            raise ValueError(f"未找到平台：{message.message_info.platform} 的url配置，请检查配置文件") from e

    async def send_message(
        self,
        message: MessageSending,
    ) -> None:
        """发送消息"""

        if isinstance(message, MessageSending):

            typing_time = calculate_typing_time(
                input_string=message.processed_plain_text,
                thinking_start_time=message.thinking_start_time,
                is_emoji=message.is_emoji,
            )
            logger.trace(f"{message.processed_plain_text},{typing_time},计算输入时间结束")
            await asyncio.sleep(typing_time)
            logger.trace(f"{message.processed_plain_text},{typing_time},等待输入时间结束")

            message_json = message.to_dict()

            message_preview = truncate_message(message.processed_plain_text)
            try:
                end_point = global_config.api_urls.get(message.message_info.platform, None)
                if end_point:
                    # logger.info(f"发送消息到{end_point}")
                    # logger.info(message_json)
                    try:
                        await global_api.send_message_rest(end_point, message_json)
                    except Exception as e:
                        logger.error(f"REST方式发送失败，出现错误: {str(e)}")
                        logger.info("尝试使用ws发送")
                        await self.send_via_ws(message)
                else:
                    await self.send_via_ws(message)
                logger.success(f"发送消息   {message_preview}   成功")
            except Exception as e:
                logger.error(f"发送消息   {message_preview}   失败: {str(e)}")


class MessageContainer:
    """单个聊天流的发送/思考消息容器"""

    def __init__(self, chat_id: str, max_size: int = 100):
        self.chat_id = chat_id
        self.max_size = max_size
        self.messages = []
        self.last_send_time = 0
        self.thinking_wait_timeout = 20  # 思考等待超时时间（秒）

    def get_timeout_messages(self) -> List[MessageSending]:
        """获取所有超时的Message_Sending对象（思考时间超过20秒），按thinking_start_time排序"""
        current_time = time.time()
        timeout_messages = []

        for msg in self.messages:
            if isinstance(msg, MessageSending):
                if current_time - msg.thinking_start_time > self.thinking_wait_timeout:
                    timeout_messages.append(msg)

        # 按thinking_start_time排序，时间早的在前面
        timeout_messages.sort(key=lambda x: x.thinking_start_time)

        return timeout_messages

    def get_earliest_message(self) -> Optional[Union[MessageThinking, MessageSending]]:
        """获取thinking_start_time最早的消息对象"""
        if not self.messages:
            return None
        earliest_time = float("inf")
        earliest_message = None
        for msg in self.messages:
            msg_time = msg.thinking_start_time
            if msg_time < earliest_time:
                earliest_time = msg_time
                earliest_message = msg
        return earliest_message

    def add_message(self, message: Union[MessageThinking, MessageSending]) -> None:
        """添加消息到队列"""
        if isinstance(message, MessageSet):
            for single_message in message.messages:
                self.messages.append(single_message)
        else:
            self.messages.append(message)

    def remove_message(self, message: Union[MessageThinking, MessageSending]) -> bool:
        """移除消息，如果消息存在则返回True，否则返回False"""
        try:
            if message in self.messages:
                self.messages.remove(message)
                return True
            return False
        except Exception:
            logger.exception("移除消息时发生错误")
            return False

    def has_messages(self) -> bool:
        """检查是否有待发送的消息"""
        return bool(self.messages)

    def get_all_messages(self) -> List[Union[MessageSending, MessageThinking]]:
        """获取所有消息"""
        return list(self.messages)


class MessageManager:
    """管理所有聊天流的消息容器"""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MessageManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        # 确保 __init__ 只被调用一次
        if not hasattr(self, '_initialized'):
            self.containers: Dict[str, MessageContainer] = {}  # chat_id -> MessageContainer
            self.storage = MessageStorage()
            self._running = True
            self._initialized = True
            # 在实例首次创建时启动消息处理器
            asyncio.create_task(self.start_processor())

    def get_container(self, chat_id: str) -> MessageContainer:
        """获取或创建聊天流的消息容器"""
        if chat_id not in self.containers:
            self.containers[chat_id] = MessageContainer(chat_id)
        return self.containers[chat_id]

    def add_message(self, message: Union[MessageThinking, MessageSending, MessageSet]) -> None:
        chat_stream = message.chat_stream
        if not chat_stream:
            raise ValueError("无法找到对应的聊天流")
        container = self.get_container(chat_stream.stream_id)
        container.add_message(message)

    async def process_chat_messages(self, chat_id: str):
        """处理聊天流消息"""
        container = self.get_container(chat_id)
        if container.has_messages():
            # print(f"处理有message的容器chat_id: {chat_id}")
            message_earliest = container.get_earliest_message()

            if isinstance(message_earliest, MessageThinking):
                """取得了思考消息"""
                message_earliest.update_thinking_time()
                thinking_time = message_earliest.thinking_time
                # print(thinking_time)
                print(
                    f"消息正在思考中，已思考{int(thinking_time)}秒\r",
                    end="",
                    flush=True,
                )

                # 检查是否超时
                if thinking_time > global_config.thinking_timeout:
                    logger.warning(f"消息思考超时({thinking_time}秒)，移除该消息")
                    container.remove_message(message_earliest)

            else:
                """取得了发送消息"""
                thinking_time = message_earliest.update_thinking_time()
                thinking_start_time = message_earliest.thinking_start_time
                now_time = time.time()
                thinking_messages_count, thinking_messages_length = count_messages_between(
                    start_time=thinking_start_time, end_time=now_time, stream_id=message_earliest.chat_stream.stream_id
                )
                # print(thinking_time)
                # print(thinking_messages_count)
                # print(thinking_messages_length)

                if (
                    message_earliest.is_head
                    and (thinking_messages_count > 4 or thinking_messages_length > 250)
                    and not message_earliest.is_private_message()  # 避免在私聊时插入reply
                ):
                    logger.debug(f"设置回复消息{message_earliest.processed_plain_text}")
                    message_earliest.set_reply()

                await message_earliest.process()

                # print(f"message_earliest.thinking_start_tim22222e:{message_earliest.thinking_start_time}")

                # 获取 MessageSender 的单例实例并发送消息
                await MessageSender().send_message(message_earliest)

                await self.storage.store_message(message_earliest, message_earliest.chat_stream)

                container.remove_message(message_earliest)

    async def start_processor(self):
        """启动消息处理器"""
        while self._running:
            await asyncio.sleep(1)
            tasks = []
            for chat_id in list(self.containers.keys()): # 使用 list 复制 key，防止在迭代时修改字典
                tasks.append(self.process_chat_messages(chat_id))

            if tasks: # 仅在有任务时执行 gather
                await asyncio.gather(*tasks)


# # 创建全局消息管理器实例 # 已改为单例模式
# message_manager = MessageManager()
# # 创建全局发送器实例 # 已改为单例模式
# message_sender = MessageSender()
