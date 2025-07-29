from dataclasses import dataclass, field
from typing import List, Dict, Union
import threading
import time

from packaging.version import Version

NEWEST_VER = "0.2.2"  # 当前支持的最新版本

@dataclass
class APIProvider:
    name: str = ""  # API提供商名称
    base_url: str = ""  # API基础URL
    api_key: str = field(repr=False, default="")  # API密钥（向后兼容）
    api_keys: List[str] = field(repr=False, default_factory=list)  # API密钥列表（新格式）
    client_type: str = "openai"  # 客户端类型（如openai/google等，默认为openai）
    
    # 多API Key管理相关属性
    _current_key_index: int = field(default=0, init=False, repr=False)  # 当前使用的key索引
    _key_failure_count: Dict[int, int] = field(default_factory=dict, init=False, repr=False)  # 每个key的失败次数
    _key_last_failure_time: Dict[int, float] = field(default_factory=dict, init=False, repr=False)  # 每个key最后失败时间
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)  # 线程锁
    
    def __post_init__(self):
        """初始化后处理，确保API keys列表正确"""
        # 向后兼容：如果只设置了api_key，将其添加到api_keys列表
        if self.api_key and not self.api_keys:
            self.api_keys = [self.api_key]
        # 如果api_keys不为空但api_key为空，设置api_key为第一个
        elif self.api_keys and not self.api_key:
            self.api_key = self.api_keys[0]
        
        # 初始化失败计数器
        for i in range(len(self.api_keys)):
            self._key_failure_count[i] = 0
            self._key_last_failure_time[i] = 0
    
    def get_current_api_key(self) -> str:
        """获取当前应该使用的API Key"""
        with self._lock:
            if not self.api_keys:
                return ""
            
            # 确保索引在有效范围内
            if self._current_key_index >= len(self.api_keys):
                self._current_key_index = 0
            
            return self.api_keys[self._current_key_index]
    
    def get_next_api_key(self) -> Union[str, None]:
        """获取下一个可用的API Key（负载均衡）"""
        with self._lock:
            if not self.api_keys:
                return None
            
            # 如果只有一个key，直接返回
            if len(self.api_keys) == 1:
                return self.api_keys[0]
            
            # 轮询到下一个key
            self._current_key_index = (self._current_key_index + 1) % len(self.api_keys)
            return self.api_keys[self._current_key_index]
    
    def mark_key_failed(self, api_key: str) -> Union[str, None]:
        """标记某个API Key失败，返回下一个可用的key"""
        with self._lock:
            if not self.api_keys or api_key not in self.api_keys:
                return None
            
            key_index = self.api_keys.index(api_key)
            self._key_failure_count[key_index] += 1
            self._key_last_failure_time[key_index] = time.time()
            
            # 寻找下一个可用的key
            current_time = time.time()
            for _ in range(len(self.api_keys)):
                self._current_key_index = (self._current_key_index + 1) % len(self.api_keys)
                next_key_index = self._current_key_index
                
                # 检查该key是否最近失败过（5分钟内失败超过3次则暂时跳过）
                if (self._key_failure_count[next_key_index] <= 3 or 
                    current_time - self._key_last_failure_time[next_key_index] > 300):  # 5分钟后重试
                    return self.api_keys[next_key_index]
            
            # 如果所有key都不可用，返回当前key（让上层处理）
            return api_key
    
    def reset_key_failures(self, api_key: str | None = None):
        """重置失败计数（成功调用后调用）"""
        with self._lock:
            if api_key and api_key in self.api_keys:
                key_index = self.api_keys.index(api_key)
                self._key_failure_count[key_index] = 0
                self._key_last_failure_time[key_index] = 0
            else:
                # 重置所有key的失败计数
                for i in range(len(self.api_keys)):
                    self._key_failure_count[i] = 0
                    self._key_last_failure_time[i] = 0
    
    def get_api_key_stats(self) -> Dict[str, Dict[str, Union[int, float]]]:
        """获取API Key使用统计"""
        with self._lock:
            stats = {}
            for i, key in enumerate(self.api_keys):
                # 只显示key的前8位和后4位，中间用*代替
                masked_key = f"{key[:8]}***{key[-4:]}" if len(key) > 12 else "***"
                stats[masked_key] = {
                    "failure_count": self._key_failure_count.get(i, 0),
                    "last_failure_time": self._key_last_failure_time.get(i, 0),
                    "is_current": i == self._current_key_index
                }
            return stats


@dataclass
class ModelInfo:
    model_identifier: str = ""  # 模型标识符（用于URL调用）
    name: str = ""  # 模型名称（用于模块调用）
    api_provider: str = ""  # API提供商（如OpenAI、Azure等）

    # 以下用于模型计费
    price_in: float = 0.0  # 每M token输入价格
    price_out: float = 0.0  # 每M token输出价格

    force_stream_mode: bool = False  # 是否强制使用流式输出模式
    
    # 新增：任务类型和能力字段
    task_type: str = ""  # 任务类型：llm_normal, llm_reasoning, vision, embedding, speech
    capabilities: List[str] = field(default_factory=list)  # 模型能力：text, vision, embedding, speech, tool_calling, reasoning


@dataclass
class RequestConfig:
    max_retry: int = 2  # 最大重试次数（单个模型API调用失败，最多重试的次数）
    timeout: int = (
        10  # API调用的超时时长（超过这个时长，本次请求将被视为“请求超时”，单位：秒）
    )
    retry_interval: int = 10  # 重试间隔（如果API调用失败，重试的间隔时间，单位：秒）
    default_temperature: float = 0.7  # 默认的温度（如果bot_config.toml中没有设置temperature参数，默认使用这个值）
    default_max_tokens: int = 1024  # 默认的最大输出token数（如果bot_config.toml中没有设置max_tokens参数，默认使用这个值）


@dataclass
class ModelUsageArgConfigItem:
    """模型使用的配置类
    该类用于加载和存储子任务模型使用的配置
    """

    name: str = ""  # 模型名称
    temperature: float | None = None  # 温度
    max_tokens: int | None = None  # 最大token数
    max_retry: int | None = None  # 调用失败时的最大重试次数


@dataclass
class ModelUsageArgConfig:
    """子任务使用模型的配置类
    该类用于加载和存储子任务使用的模型配置
    """

    name: str = ""  # 任务名称
    usage: List[ModelUsageArgConfigItem] = field(
        default_factory=lambda: []
    )  # 任务使用的模型列表



@dataclass
class ModuleConfig:
    INNER_VERSION: Version | None = None  # 配置文件版本

    req_conf: RequestConfig = field(default_factory=lambda: RequestConfig())  # 请求配置
    api_providers: Dict[str, APIProvider] = field(
        default_factory=lambda: {}
    )  # API提供商列表
    models: Dict[str, ModelInfo] = field(default_factory=lambda: {})  # 模型列表
    task_model_arg_map: Dict[str, ModelUsageArgConfig] = field(
        default_factory=lambda: {}
    )