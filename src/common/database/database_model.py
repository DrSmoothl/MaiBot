from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, Enum as SQLEnum, Float, Index, Integer, Text, UniqueConstraint
from sqlmodel import Field, LargeBinary, SQLModel


class ModelUser(str, Enum):
    SYSTEM = "system"
    PLUGIN = "plugin"


class ImageType(str, Enum):
    EMOJI = "emoji"
    IMAGE = "image"


class ModifiedBy(str, Enum):
    AI = "AI"
    USER = "USER"


class JargonCreatedBy(str, Enum):
    AI = "AI"
    MANUAL = "MANUAL"


class Messages(SQLModel, table=True):
    __tablename__ = "mai_messages"  # type: ignore
    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 消息元数据
    message_id: str = Field(index=True, max_length=255)  # 消息id
    timestamp: datetime = Field(sa_column=Column(DateTime))  # 消息时间，单位为秒
    platform: str = Field(index=True, max_length=100)  # 顶层平台字段
    # 消息发送者信息
    user_id: str = Field(index=True, max_length=255)  # 发送者用户id
    user_nickname: str = Field(index=True, max_length=255)  # 发送者昵称
    user_cardname: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 发送者备注名
    # 群聊信息（如果有）
    group_id: Optional[str] = Field(index=True, default=None, max_length=255, nullable=True)  # 群组id
    group_name: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 群组名称
    # 被提及/at字段
    is_mentioned: bool = Field(default=False)  # 被提及
    is_at: bool = Field(default=False)  # 被at

    # 消息内部元数据
    session_id: str = Field(index=True, max_length=255)  # 聊天会话id
    reply_to: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 回复的消息id
    is_emoji: bool = Field(default=False)  # 是否为表情包消息
    is_picture: bool = Field(default=False)  # 是否为图片消息
    is_command: bool = Field(default=False)  # 是否为命令
    is_notify: bool = Field(default=False)  # 是否为通知消息

    # 消息内容
    raw_content: bytes = Field(sa_column=Column(LargeBinary))  # msgpack后的原始消息内容
    processed_plain_text: Optional[str] = Field(default=None)  # 平面化处理后的纯文本消息

    # 其他配置
    additional_config: Optional[str] = Field(default=None)  # 额外配置，JSON格式存储
    reply_frequency: Optional[float] = Field(default=None, sa_column=Column(Float, nullable=True))
    # 消息发生时当前会话的生效回复频率；无法解析时为空


class ModelUsage(SQLModel, table=True):
    __tablename__ = "llm_usage"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 模型相关信息
    model_name: str = Field(index=True, max_length=255)  # 模型实际名称（供应商名称）
    model_assign_name: Optional[str] = Field(index=True, default=None, max_length=255)  # 模型分配名称（用户自定义名称）
    model_api_provider_name: str = Field(index=True, max_length=255)  # 模型API供应商名称

    # 请求相关信息
    endpoint: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 模型API的具体endpoint
    user_type: ModelUser = Field(sa_column=Column(SQLEnum(ModelUser)), default=ModelUser.SYSTEM)  # 模型使用者类型
    task_name: Optional[str] = Field(default=None, index=True, max_length=100, nullable=True)  # 模型任务配置名称
    request_type: str = Field(max_length=50)  # 内部请求类型，记录哪种模块使用了此模型
    time_cost: float = Field(sa_column=Column(Float))  # 本次请求耗时，单位秒
    timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))  # 请求时间戳

    # Token使用情况
    prompt_tokens: int  # 提示词令牌数
    completion_tokens: int  # 完成词令牌数
    total_tokens: int  # 总令牌数
    prompt_cache_enabled: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default="0"),
    )  # 本次请求发生时是否启用了模型输入缓存计费
    prompt_cache_hit_tokens: int = Field(
        default=0,
        sa_column=Column(Integer, nullable=False, server_default="0"),
    )  # prompt cache 命中令牌数
    prompt_cache_miss_tokens: int = Field(
        default=0,
        sa_column=Column(Integer, nullable=False, server_default="0"),
    )  # prompt cache 未命中令牌数
    cost: float  # 本次请求的费用，单位元


class Images(SQLModel, table=True):
    """用于同时存储表情包和图片的数据库模型。"""

    __tablename__ = "images"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 元信息
    image_hash: str = Field(index=True, max_length=255)  # 图片哈希，使用sha256哈希值，亦作为图片唯一ID
    description: str  # 图片的描述
    full_path: str = Field(max_length=1024)  # 文件的完整路径 (包括文件名)
    image_type: ImageType = Field(sa_column=Column(SQLEnum(ImageType)), default=ImageType.EMOJI)
    """图片类型，例如 'emoji' 或 'image'"""

    query_count: int = Field(default=0)  # 被查询次数
    is_registered: bool = Field(default=False)  # 是否已经注册
    is_banned: bool = Field(default=False)  # 被手动禁用

    no_file_flag: bool = Field(default=False)  # 文件不存在标记，如果为True表示文件已经不存在，仅保留描述字段

    record_time: datetime = Field(
        default_factory=datetime.now, sa_column=Column(DateTime, index=True)
    )  # 记录时间（数据库记录被创建的时间）
    register_time: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime, nullable=True)
    )  # 注册时间（被注册为可用表情包的时间）
    last_used_time: Optional[datetime] = Field(default=None, sa_column=Column(DateTime, nullable=True))  # 上次使用时间

    vlm_processed: bool = Field(default=False)  # 是否已经过VLM处理


class ToolRecord(SQLModel, table=True):
    """存储工具调用记录"""

    __tablename__ = "tool_records"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 元信息
    tool_id: str = Field(index=True, max_length=255)  # 工具调用ID
    timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))  # 记录时间戳
    session_id: str = Field(index=True, max_length=255)  # 对应的 ChatSession session_id

    # 调用信息
    tool_name: str = Field(index=True, max_length=255)  # 工具名称
    tool_reasoning: Optional[str] = Field(default=None)  # 工具调用推理过程
    tool_data: Optional[str] = Field(default=None)  # 工具数据，JSON格式存储

    tool_builtin_prompt: Optional[str] = Field(default=None)  # 内置工具提示
    tool_display_prompt: Optional[str] = Field(default=None)  # 最终输入到 Prompt 的内容


class StatisticsAggregationCursor(SQLModel, table=True):
    """统计汇总增量游标。"""

    __tablename__ = "statistics_aggregation_cursors"  # type: ignore

    source_name: str = Field(primary_key=True, max_length=100)
    last_processed_id: int = Field(default=0)
    updated_at: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))


class StatisticsMessageHourly(SQLModel, table=True):
    """按小时聚合的消息统计。"""

    __tablename__ = "statistics_message_hourly"  # type: ignore
    __table_args__ = (
        UniqueConstraint("bucket_time", "chat_id", name="uq_statistics_message_hourly_bucket_chat"),
        Index("ix_statistics_message_hourly_bucket_time", "bucket_time"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    bucket_time: datetime = Field(sa_column=Column(DateTime, nullable=False))
    chat_id: str = Field(max_length=255)
    chat_name: str = Field(max_length=255)
    chat_type: str = Field(max_length=20)
    message_count: int = Field(default=0)
    latest_timestamp: datetime = Field(sa_column=Column(DateTime, nullable=False))


class StatisticsToolHourly(SQLModel, table=True):
    """按小时聚合的工具调用统计。"""

    __tablename__ = "statistics_tool_hourly"  # type: ignore
    __table_args__ = (
        UniqueConstraint("bucket_time", "tool_name", name="uq_statistics_tool_hourly_bucket_tool"),
        Index("ix_statistics_tool_hourly_bucket_time", "bucket_time"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    bucket_time: datetime = Field(sa_column=Column(DateTime, nullable=False))
    tool_name: str = Field(max_length=255)
    call_count: int = Field(default=0)


class StatisticsModelHourly(SQLModel, table=True):
    """按小时聚合的模型调用统计。"""

    __tablename__ = "statistics_model_hourly"  # type: ignore
    __table_args__ = (
        UniqueConstraint(
            "bucket_time",
            "request_type",
            "model_name",
            "provider_name",
            name="uq_statistics_model_hourly_bucket_request_model_provider",
        ),
        Index("ix_statistics_model_hourly_bucket_time", "bucket_time"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    bucket_time: datetime = Field(sa_column=Column(DateTime, nullable=False))
    request_type: str = Field(max_length=100)
    module_name: str = Field(max_length=100)
    provider_name: str = Field(max_length=255)
    model_name: str = Field(max_length=255)
    request_count: int = Field(default=0)
    prompt_tokens: int = Field(default=0)
    completion_tokens: int = Field(default=0)
    total_tokens: int = Field(default=0)
    cost: float = Field(default=0.0)
    time_cost_sum: float = Field(default=0.0)
    time_cost_sq_sum: float = Field(default=0.0)


class HighFrequencyTerm(SQLModel, table=True):
    """高频词/词组词库。"""

    __tablename__ = "high_frequency_terms"  # type: ignore
    __table_args__ = (
        UniqueConstraint("normalized_term", name="uq_high_frequency_terms_normalized_term"),
        Index("ix_high_frequency_terms_rank", "rank"),
        Index("ix_high_frequency_terms_updated_at", "updated_at"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    term: str = Field(sa_column=Column(Text, nullable=False))
    normalized_term: str = Field(sa_column=Column(Text, nullable=False))
    term_type: str = Field(default="word", max_length=20)
    rank: int = Field(default=0)
    occurrence_count: int = Field(default=0)
    message_count: int = Field(default=0)
    frequency: float = Field(default=0.0)
    message_frequency: float = Field(default=0.0)
    created_at: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, nullable=False))
    updated_at: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, nullable=False))


class CommandRecord(SQLModel, table=True):
    """记录命令执行情况"""

    __tablename__ = "command_records"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 元信息
    timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))  # 记录时间戳
    session_id: str = Field(index=True, max_length=255)  # 对应的 ChatSession session_id

    # 调用信息
    command_name: str = Field(index=True, max_length=255)  # 命令名称
    command_data: Optional[str] = Field(default=None)  # 命令数据，JSON格式存储
    command_result: Optional[str] = Field(default=None)  # 命令执行结果


class OnlineTime(SQLModel, table=True):
    """
    用于存储在线时长记录的模型。
    """

    __tablename__ = "online_time"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))  # 时间戳
    duration_minutes: int = Field()  # 时长，单位秒
    start_timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime))  # 上线时间
    end_timestamp: datetime = Field(sa_column=Column(DateTime))  # 下线时间


class Expression(SQLModel, table=True):
    """用于存储表达方式的模型"""

    __tablename__ = "expressions"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    situation: str = Field(index=True, max_length=255)  # 情景
    style: str = Field(index=True, max_length=255)  # 风格

    # context: str  # 上下文
    # up_content: str

    content_list: str  # 内容列表，JSON格式存储
    count: int = Field(default=0)  # 使用次数
    last_active_time: datetime = Field(
        default_factory=datetime.now, sa_column=Column(DateTime, index=True)
    )  # 上次使用时间
    create_time: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime))  # 创建时间
    session_id: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 会话ID，区分是否为全局表达方式

    checked: bool = Field(default=False)  # 是否已经通过人工审核
    modified_by: Optional[ModifiedBy] = Field(
        default=None, sa_column=Column(SQLEnum(ModifiedBy), nullable=True)
    )  # 最后修改者，标记用户或AI，为空表示暂无修改来源


class BehaviorPattern(SQLModel, table=True):
    """用于存储可复用的行为表现模式。"""

    __tablename__ = "behavior_patterns"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)

    trigger: str = Field(sa_column=Column(Text, nullable=False))
    action: str = Field(sa_column=Column(Text, nullable=False))
    outcome: str = Field(sa_column=Column(Text, nullable=False))

    evidence_list: str = Field(default="[]", sa_column=Column(Text, nullable=False))
    feedback_list: str = Field(default="[]", sa_column=Column(Text, nullable=False))

    count: int = Field(default=0)
    activation_count: int = Field(default=0)
    success_count: int = Field(default=0)
    failure_count: int = Field(default=0)
    score: float = Field(default=0.0, sa_column=Column(Float, nullable=False, server_default="0"))
    enabled: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, server_default="1"))

    last_active_time: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))
    last_feedback_time: Optional[datetime] = Field(default=None, sa_column=Column(DateTime, nullable=True))
    create_time: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime))
    update_time: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))
    session_id: Optional[str] = Field(default=None, max_length=255, nullable=True, index=True)


class Jargon(SQLModel, table=True):
    """存黑话的模型"""

    __tablename__ = "jargons"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    content: str = Field(index=True, max_length=255)  # 黑话内容
    raw_content: Optional[str] = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )  # 原始内容，未处理的黑话内容，为List[str]

    meaning: str = Field(sa_column=Column(Text, nullable=False))  # 黑话含义
    session_id_dict: str = Field(
        default=r"{}", sa_column=Column(Text, nullable=False)
    )  # 会话ID列表，格式为{"session_id": session_count, ...}

    count: int = Field(default=0)  # 使用次数
    is_jargon: Optional[bool] = Field(default=True)  # 是否为黑话，False表示为白话
    is_complete: bool = Field(default=False)  # 是否为已经完成全部推断（count > 100后不再推断）
    is_global: bool = Field(default=False)  # 是否为全局黑话（独立于session_id_dict）
    last_inference_count: int = Field(default=0)  # 上一次进行推断时的count值，用于判断是否需要重新推断
    created_by: JargonCreatedBy = Field(
        default=JargonCreatedBy.AI,
        sa_column=Column(SQLEnum(JargonCreatedBy), nullable=False),
    )  # 创建来源，AI 表示自动学习，MANUAL 表示手动创建
    created_timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))
    updated_timestamp: datetime = Field(default_factory=datetime.now, sa_column=Column(DateTime, index=True))


class ChatHistory(SQLModel, table=True):
    """存储聊天历史记录的模型"""

    __tablename__ = "chat_history"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    # 元信息
    session_id: str = Field(index=True, max_length=255)  # 聊天会话ID
    start_timestamp: datetime = Field(sa_column=Column(DateTime, index=True))  # 聊天开始时间
    end_timestamp: datetime = Field(sa_column=Column(DateTime, index=True))  # 聊天结束时间
    query_count: int = Field(default=0)  # 被检索次数
    query_forget_count: int = Field(default=0)  # 被遗忘检查的次数

    # 历史消息内容
    original_messages: str  # 对话原文
    participants: str  # 参与者列表，JSON格式存储
    theme: str  # 对话主题：这段对话的主要内容，一个简短的标题
    keywords: str  # 关键词：这段对话的关键词，JSON格式存储
    summary: str  # 概括：对这段话的平文本概括


class BinaryData(SQLModel, table=True):
    """存储二进制数据的模型"""

    __tablename__ = "binary_data"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    data_hash: str = Field(index=True, max_length=255)  # 数据哈希，使用sha256哈希值，亦作为数据唯一ID
    full_path: str = Field(max_length=1024)  # 文件的完整路径 (包括文件名)


class PersonInfo(SQLModel, table=True):
    """存储个人信息的模型"""

    __tablename__ = "person_info"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    is_known: bool = Field(default=False)  # 是否为已知人
    person_id: str = Field(unique=True, index=True, max_length=255)  # 人员ID
    person_name: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 人员名称
    name_reason: Optional[str] = Field(default=None, nullable=True)  # 名称原因

    # 身份元数据
    platform: str = Field(index=True, max_length=100)  # 平台名称
    user_id: str = Field(index=True, max_length=255)  # 用户ID
    user_nickname: str = Field(index=True, max_length=255)  # 用户昵称
    group_cardname: Optional[str] = Field(
        default=None, nullable=True
    )  # 群昵称 (JSON, [{"group_id": str, "group_cardname": str}])

    # 印象
    memory_points: Optional[str] = Field(default=None, nullable=True)  # 记忆要点，JSON格式存储

    # 认识次数和时间
    know_counts: int = Field(default=0)  # 认识次数
    first_known_time: Optional[datetime] = Field(
        default=None, sa_column=Column(DateTime, nullable=True)
    )  # 首次认识时间
    last_known_time: Optional[datetime] = Field(default=None, sa_column=Column(DateTime, nullable=True))  # 最后认识时间


class ChatSession(SQLModel, table=True):
    """存储聊天会话的模型"""

    __tablename__ = "chat_sessions"  # type: ignore

    id: Optional[int] = Field(default=None, primary_key=True)  # 自增主键

    session_id: str = Field(unique=True, index=True, max_length=255)  # 聊天会话ID

    created_timestamp: datetime = Field(
        default_factory=datetime.now, sa_column=Column(DateTime, index=True)
    )  # 创建时间
    last_active_timestamp: Optional[datetime] = Field(
        default_factory=datetime.now, sa_column=Column(DateTime, index=True)
    )  # 最后活跃时间

    # 身份元数据
    user_id: Optional[str] = Field(index=True, max_length=255, nullable=True)  # 用户ID
    user_nickname: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 私聊用户昵称
    user_cardname: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 私聊用户群名片/备注
    group_id: Optional[str] = Field(index=True, default=None, max_length=255, nullable=True)  # 群组id
    group_name: Optional[str] = Field(default=None, max_length=255, nullable=True)  # 群组名称
    platform: str = Field(index=True, max_length=100)  # 会话所在平台
    account_id: Optional[str] = Field(default=None, index=True, max_length=255, nullable=True)  # 平台账号 ID
    scope: Optional[str] = Field(default=None, index=True, max_length=255, nullable=True)  # 路由作用域
