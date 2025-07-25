import asyncio
import time
from maim_message import MessageServer

from src.chat.express.expression_learner import get_expression_learner
from src.common.remote import TelemetryHeartBeatTask
from src.manager.async_task_manager import async_task_manager
from src.chat.utils.statistic import OnlineTimeRecordTask, StatisticOutputTask
from src.chat.emoji_system.emoji_manager import get_emoji_manager
from src.chat.willing.willing_manager import get_willing_manager
from src.chat.message_receive.chat_stream import get_chat_manager
from src.config.config import global_config
from src.chat.message_receive.bot import chat_bot
from src.common.logger import get_logger
from src.individuality.individuality import get_individuality, Individuality
from src.common.server import get_global_server, Server
from src.mood.mood_manager import mood_manager
from rich.traceback import install
# from src.api.main import start_api_server

# 导入新的插件管理器
from src.plugin_system.core.plugin_manager import plugin_manager

# 导入消息API和traceback模块
from src.common.message import get_global_api

# 条件导入记忆系统
if global_config.memory.enable_memory:
    from src.chat.memory_system.Hippocampus import hippocampus_manager

# 插件系统现在使用统一的插件加载器

install(extra_lines=3)

willing_manager = get_willing_manager()

logger = get_logger("main")


class MainSystem:
    def __init__(self):
        # 根据配置条件性地初始化记忆系统
        if global_config.memory.enable_memory:
            self.hippocampus_manager = hippocampus_manager
        else:
            self.hippocampus_manager = None

        self.individuality: Individuality = get_individuality()

        # 使用消息API替代直接的FastAPI实例
        self.app: MessageServer = get_global_api()
        self.server: Server = get_global_server()

    async def initialize(self):
        """初始化系统组件"""
        logger.debug(f"正在唤醒{global_config.bot.nickname}......")

        # 其他初始化任务
        await asyncio.gather(self._init_components())

        logger.debug("系统初始化完成")

    async def _init_components(self):
        """初始化其他组件"""
        init_start_time = time.time()

        # 添加在线时间统计任务
        await async_task_manager.add_task(OnlineTimeRecordTask())

        # 添加统计信息输出任务
        await async_task_manager.add_task(StatisticOutputTask())

        # 添加遥测心跳任务
        await async_task_manager.add_task(TelemetryHeartBeatTask())

        # 启动API服务器
        # start_api_server()
        # logger.info("API服务器启动成功")

        # 加载所有actions，包括默认的和插件的
        plugin_manager.load_all_plugins()

        # 初始化表情管理器
        get_emoji_manager().initialize()
        logger.info("表情包管理器初始化成功")

        # 启动愿望管理器
        await willing_manager.async_task_starter()

        logger.info("willing管理器初始化成功")

        # 启动情绪管理器
        await mood_manager.start()
        logger.info("情绪管理器初始化成功")

        # 初始化聊天管理器

        await get_chat_manager()._initialize()
        asyncio.create_task(get_chat_manager()._auto_save_task())

        logger.info("聊天管理器初始化成功")

        # 根据配置条件性地初始化记忆系统
        if global_config.memory.enable_memory:
            if self.hippocampus_manager:
                self.hippocampus_manager.initialize()
                logger.info("记忆系统初始化成功")
        else:
            logger.info("记忆系统已禁用，跳过初始化")

        # await asyncio.sleep(0.5) #防止logger输出飞了

        # 将bot.py中的chat_bot.message_process消息处理函数注册到api.py的消息处理基类中
        self.app.register_message_handler(chat_bot.message_process)

        # 初始化个体特征
        await self.individuality.initialize()

        try:
            init_time = int(1000 * (time.time() - init_start_time))
            logger.info(f"初始化完成，神经元放电{init_time}次")
        except Exception as e:
            logger.error(f"启动大脑和外部世界失败: {e}")
            raise

    async def schedule_tasks(self):
        """调度定时任务"""
        while True:
            tasks = [
                get_emoji_manager().start_periodic_check_register(),
                self.app.run(),
                self.server.run(),
            ]

            # 根据配置条件性地添加记忆系统相关任务
            if global_config.memory.enable_memory and self.hippocampus_manager:
                tasks.extend(
                    [
                        self.build_memory_task(),
                        self.forget_memory_task(),
                        self.consolidate_memory_task(),
                    ]
                )

            tasks.append(self.learn_and_store_expression_task())

            await asyncio.gather(*tasks)

    async def build_memory_task(self):
        """记忆构建任务"""
        while True:
            await asyncio.sleep(global_config.memory.memory_build_interval)
            logger.info("正在进行记忆构建")
            await self.hippocampus_manager.build_memory()  # type: ignore

    async def forget_memory_task(self):
        """记忆遗忘任务"""
        while True:
            await asyncio.sleep(global_config.memory.forget_memory_interval)
            logger.info("[记忆遗忘] 开始遗忘记忆...")
            await self.hippocampus_manager.forget_memory(percentage=global_config.memory.memory_forget_percentage)  # type: ignore
            logger.info("[记忆遗忘] 记忆遗忘完成")

    async def consolidate_memory_task(self):
        """记忆整合任务"""
        while True:
            await asyncio.sleep(global_config.memory.consolidate_memory_interval)
            logger.info("[记忆整合] 开始整合记忆...")
            await self.hippocampus_manager.consolidate_memory()  # type: ignore
            logger.info("[记忆整合] 记忆整合完成")

    @staticmethod
    async def learn_and_store_expression_task():
        """学习并存储表达方式任务"""
        expression_learner = get_expression_learner()
        while True:
            await asyncio.sleep(global_config.expression.learning_interval)
            if global_config.expression.enable_expression_learning and global_config.expression.enable_expression:
                logger.info("[表达方式学习] 开始学习表达方式...")
                await expression_learner.learn_and_store_expression()
                logger.info("[表达方式学习] 表达方式学习完成")


async def main():
    """主函数"""
    system = MainSystem()
    await asyncio.gather(
        system.initialize(),
        system.schedule_tasks(),
    )


if __name__ == "__main__":
    asyncio.run(main())
