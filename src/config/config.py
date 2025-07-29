import os
import tomlkit
import shutil

from datetime import datetime
from tomlkit import TOMLDocument
from tomlkit.items import Table, KeyType
from dataclasses import field, dataclass
from rich.traceback import install
from packaging import version
from packaging.specifiers import SpecifierSet
from packaging.version import Version, InvalidVersion
from typing import Any, Dict, List

from src.common.logger import get_logger
from src.config.config_base import ConfigBase
from src.config.official_configs import (
    BotConfig,
    PersonalityConfig,
    ExpressionConfig,
    ChatConfig,
    NormalChatConfig,
    EmojiConfig,
    MemoryConfig,
    MoodConfig,
    KeywordReactionConfig,
    ChineseTypoConfig,
    ResponsePostProcessConfig,
    ResponseSplitterConfig,
    TelemetryConfig,
    ExperimentalConfig,
    ModelConfig,
    MessageReceiveConfig,
    MaimMessageConfig,
    LPMMKnowledgeConfig,
    RelationshipConfig,
    ToolConfig,
    VoiceConfig,
    DebugConfig,
    CustomPromptConfig,
)

from .api_ada_configs import (
    ModelUsageArgConfigItem,
    ModelUsageArgConfig,
    APIProvider,
    ModelInfo,
    NEWEST_VER,
    ModuleConfig,
)



install(extra_lines=3)


# 配置主程序日志格式
logger = get_logger("config")

# 获取当前文件所在目录的父目录的父目录（即MaiBot项目根目录）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
TEMPLATE_DIR = os.path.join(PROJECT_ROOT, "template")

# 考虑到，实际上配置文件中的mai_version是不会自动更新的,所以采用硬编码
# 对该字段的更新，请严格参照语义化版本规范：https://semver.org/lang/zh-CN/
MMC_VERSION = "0.10.0-snapshot1"




def _get_config_version(toml: Dict) -> Version:
    """提取配置文件的 SpecifierSet 版本数据
    Args:
        toml[dict]: 输入的配置文件字典
    Returns:
        Version
    """

    if "inner" in toml and "version" in toml["inner"]:
        config_version: str = toml["inner"]["version"]
    else:
        config_version = "0.0.0"  # 默认版本

    try:
        ver = version.parse(config_version)
    except InvalidVersion as e:
        logger.error(
            "配置文件中 inner段 的 version 键是错误的版本描述\n"
            f"请检查配置文件，当前 version 键: {config_version}\n"
            f"错误信息: {e}"
        )
        raise InvalidVersion(
            "配置文件中 inner段 的 version 键是错误的版本描述\n"
        ) from e

    return ver


def _request_conf(parent: Dict, config: ModuleConfig):
    request_conf_config = parent.get("request_conf")
    config.req_conf.max_retry = request_conf_config.get(
        "max_retry", config.req_conf.max_retry
    )
    config.req_conf.timeout = request_conf_config.get(
        "timeout", config.req_conf.timeout
    )
    config.req_conf.retry_interval = request_conf_config.get(
        "retry_interval", config.req_conf.retry_interval
    )
    config.req_conf.default_temperature = request_conf_config.get(
        "default_temperature", config.req_conf.default_temperature
    )
    config.req_conf.default_max_tokens = request_conf_config.get(
        "default_max_tokens", config.req_conf.default_max_tokens
    )


def _api_providers(parent: Dict, config: ModuleConfig):
    api_providers_config = parent.get("api_providers")
    for provider in api_providers_config:
        name = provider.get("name", None)
        base_url = provider.get("base_url", None)
        api_key = provider.get("api_key", None)
        api_keys = provider.get("api_keys", [])  # 新增：支持多个API Key
        client_type = provider.get("client_type", "openai")

        if name in config.api_providers:  # 查重
            logger.error(f"重复的API提供商名称: {name}，请检查配置文件。")
            raise KeyError(f"重复的API提供商名称: {name}，请检查配置文件。")

        if name and base_url:
            # 处理API Key配置：支持单个api_key或多个api_keys
            if api_keys:
                # 使用新格式：api_keys列表
                logger.debug(f"API提供商 '{name}' 配置了 {len(api_keys)} 个API Key")
            elif api_key:
                # 向后兼容：使用单个api_key
                api_keys = [api_key]
                logger.debug(f"API提供商 '{name}' 使用单个API Key（向后兼容模式）")
            else:
                logger.warning(f"API提供商 '{name}' 没有配置API Key，某些功能可能不可用")
            
            config.api_providers[name] = APIProvider(
                name=name,
                base_url=base_url,
                api_key=api_key,  # 保留向后兼容
                api_keys=api_keys,  # 新格式
                client_type=client_type,
            )
        else:
            logger.error(f"API提供商 '{name}' 的配置不完整，请检查配置文件。")
            raise ValueError(f"API提供商 '{name}' 的配置不完整，请检查配置文件。")


def _models(parent: Dict, config: ModuleConfig):
    models_config = parent.get("models")
    for model in models_config:
        model_identifier = model.get("model_identifier", None)
        name = model.get("name", model_identifier)
        api_provider = model.get("api_provider", None)
        price_in = model.get("price_in", 0.0)
        price_out = model.get("price_out", 0.0)
        force_stream_mode = model.get("force_stream_mode", False)
        task_type = model.get("task_type", "")
        capabilities = model.get("capabilities", [])

        if name in config.models:  # 查重
            logger.error(f"重复的模型名称: '{name}'，请检查配置文件。")
            logger.error(f"重复位置：模型标识符 '{model_identifier}' 与已存在的模型名称冲突。")
            logger.error(f"解决方案：请为模型 '{model_identifier}' 设置一个唯一的 name 字段，或删除重复的模型配置。")
            raise KeyError(f"重复的模型名称: '{name}'，每个模型的 name 字段必须唯一，请检查配置文件。")

        if model_identifier and api_provider:
            # 检查API提供商是否存在
            if api_provider not in config.api_providers:
                logger.error(f"未声明的API提供商 '{api_provider}' ，请检查配置文件。")
                raise ValueError(
                    f"未声明的API提供商 '{api_provider}' ，请检查配置文件。"
                )
            config.models[name] = ModelInfo(
                name=name,
                model_identifier=model_identifier,
                api_provider=api_provider,
                price_in=price_in,
                price_out=price_out,
                force_stream_mode=force_stream_mode,
                task_type=task_type,
                capabilities=capabilities,
            )
        else:
            logger.error(f"模型 '{name}' 的配置不完整，请检查配置文件。")
            raise ValueError(f"模型 '{name}' 的配置不完整，请检查配置文件。")


def _task_model_usage(parent: Dict, config: ModuleConfig):
    model_usage_configs = parent.get("task_model_usage")
    config.task_model_arg_map = {}
    for task_name, item in model_usage_configs.items():
        if task_name in config.task_model_arg_map:
            logger.error(f"子任务 {task_name} 已存在，请检查配置文件。")
            raise KeyError(f"子任务 {task_name} 已存在，请检查配置文件。")

        usage = []
        if isinstance(item, Dict):
            if "model" in item:
                usage.append(
                    ModelUsageArgConfigItem(
                        name=item["model"],
                        temperature=item.get("temperature", None),
                        max_tokens=item.get("max_tokens", None),
                        max_retry=item.get("max_retry", None),
                    )
                )
            else:
                logger.error(f"子任务 {task_name} 的模型配置不合法，请检查配置文件。")
                raise ValueError(
                    f"子任务 {task_name} 的模型配置不合法，请检查配置文件。"
                )
        elif isinstance(item, List):
            for model in item:
                if isinstance(model, Dict):
                    usage.append(
                        ModelUsageArgConfigItem(
                            name=model["model"],
                            temperature=model.get("temperature", None),
                            max_tokens=model.get("max_tokens", None),
                            max_retry=model.get("max_retry", None),
                        )
                    )
                elif isinstance(model, str):
                    usage.append(
                        ModelUsageArgConfigItem(
                            name=model,
                            temperature=None,
                            max_tokens=None,
                            max_retry=None,
                        )
                    )
                else:
                    logger.error(
                        f"子任务 {task_name} 的模型配置不合法，请检查配置文件。"
                    )
                    raise ValueError(
                        f"子任务 {task_name} 的模型配置不合法，请检查配置文件。"
                    )
        elif isinstance(item, str):
            usage.append(
                ModelUsageArgConfigItem(
                    name=item,
                    temperature=None,
                    max_tokens=None,
                    max_retry=None,
                )
            )

        config.task_model_arg_map[task_name] = ModelUsageArgConfig(
            name=task_name,
            usage=usage,
        )


def api_ada_load_config(config_path: str) -> ModuleConfig:
    """从TOML配置文件加载配置"""
    config = ModuleConfig()

    include_configs: Dict[str, Dict[str, Any]] = {
        "request_conf": {
            "func": _request_conf,
            "support": ">=0.0.0",
            "necessary": False,
        },
        "api_providers": {"func": _api_providers, "support": ">=0.0.0"},
        "models": {"func": _models, "support": ">=0.0.0"},
        "task_model_usage": {"func": _task_model_usage, "support": ">=0.0.0"},
    }

    if os.path.exists(config_path):
        with open(config_path, "rb") as f:
            try:
                toml_dict = tomlkit.load(f)
            except tomlkit.TOMLDecodeError as e:
                logger.critical(
                    f"配置文件model_list.toml填写有误，请检查第{e.lineno}行第{e.colno}处：{e.msg}"
                )
                exit(1)

        # 获取配置文件版本
        config.INNER_VERSION = _get_config_version(toml_dict)

        # 检查版本
        if config.INNER_VERSION > Version(NEWEST_VER):
            logger.warning(
                f"当前配置文件版本 {config.INNER_VERSION} 高于支持的最新版本 {NEWEST_VER}，可能导致异常，建议更新依赖。"
            )

        # 解析配置文件
        # 如果在配置中找到了需要的项，调用对应项的闭包函数处理
        for key in include_configs:
            if key in toml_dict:
                group_specifier_set: SpecifierSet = SpecifierSet(
                    include_configs[key]["support"]
                )

                # 检查配置文件版本是否在支持范围内
                if config.INNER_VERSION in group_specifier_set:
                    # 如果版本在支持范围内，检查是否存在通知
                    if "notice" in include_configs[key]:
                        logger.warning(include_configs[key]["notice"])
                    # 调用闭包函数处理配置
                    (include_configs[key]["func"])(toml_dict, config)
                else:
                    # 如果版本不在支持范围内，崩溃并提示用户
                    logger.error(
                        f"配置文件中的 '{key}' 字段的版本 ({config.INNER_VERSION}) 不在支持范围内。\n"
                        f"当前程序仅支持以下版本范围: {group_specifier_set}"
                    )
                    raise InvalidVersion(
                        f"当前程序仅支持以下版本范围: {group_specifier_set}"
                    )

            # 如果 necessary 项目存在，而且显式声明是 False，进入特殊处理
            elif (
                "necessary" in include_configs[key]
                and include_configs[key].get("necessary") is False
            ):
                # 通过 pass 处理的项虽然直接忽略也是可以的，但是为了不增加理解困难，依然需要在这里显式处理
                if key == "keywords_reaction":
                    pass
            else:
                # 如果用户根本没有需要的配置项，提示缺少配置
                logger.error(f"配置文件中缺少必需的字段: '{key}'")
                raise KeyError(f"配置文件中缺少必需的字段: '{key}'")

        logger.info(f"成功加载配置文件: {config_path}")

    return config

def get_key_comment(toml_table, key):
    # 获取key的注释（如果有）
    if hasattr(toml_table, "trivia") and hasattr(toml_table.trivia, "comment"):
        return toml_table.trivia.comment
    if hasattr(toml_table, "value") and isinstance(toml_table.value, dict):
        item = toml_table.value.get(key)
        if item is not None and hasattr(item, "trivia"):
            return item.trivia.comment
    if hasattr(toml_table, "keys"):
        for k in toml_table.keys():
            if isinstance(k, KeyType) and k.key == key:
                return k.trivia.comment
    return None


def compare_dicts(new, old, path=None, logs=None):
    # 递归比较两个dict，找出新增和删减项，收集注释
    if path is None:
        path = []
    if logs is None:
        logs = []
    # 新增项
    for key in new:
        if key == "version":
            continue
        if key not in old:
            comment = get_key_comment(new, key)
            logs.append(f"新增: {'.'.join(path + [str(key)])}  注释: {comment if comment else '无'}")
        elif isinstance(new[key], (dict, Table)) and isinstance(old.get(key), (dict, Table)):
            compare_dicts(new[key], old[key], path + [str(key)], logs)
    # 删减项
    for key in old:
        if key == "version":
            continue
        if key not in new:
            comment = get_key_comment(old, key)
            logs.append(f"删减: {'.'.join(path + [str(key)])}  注释: {comment if comment else '无'}")
    return logs


def get_value_by_path(d, path):
    for k in path:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return None
    return d


def set_value_by_path(d, path, value):
    for k in path[:-1]:
        if k not in d or not isinstance(d[k], dict):
            d[k] = {}
        d = d[k]
    d[path[-1]] = value


def compare_default_values(new, old, path=None, logs=None, changes=None):
    # 递归比较两个dict，找出默认值变化项
    if path is None:
        path = []
    if logs is None:
        logs = []
    if changes is None:
        changes = []
    for key in new:
        if key == "version":
            continue
        if key in old:
            if isinstance(new[key], (dict, Table)) and isinstance(old[key], (dict, Table)):
                compare_default_values(new[key], old[key], path + [str(key)], logs, changes)
            else:
                # 只要值发生变化就记录
                if new[key] != old[key]:
                    logs.append(
                        f"默认值变化: {'.'.join(path + [str(key)])}  旧默认值: {old[key]}  新默认值: {new[key]}"
                    )
                    changes.append((path + [str(key)], old[key], new[key]))
    return logs, changes


def _get_version_from_toml(toml_path):
    """从TOML文件中获取版本号"""
    if not os.path.exists(toml_path):
        return None
    with open(toml_path, "r", encoding="utf-8") as f:
        doc = tomlkit.load(f)
    if "inner" in doc and "version" in doc["inner"]:  # type: ignore
        return doc["inner"]["version"]  # type: ignore
    return None


def _version_tuple(v):
    """将版本字符串转换为元组以便比较"""
    if v is None:
        return (0,)
    return tuple(int(x) if x.isdigit() else 0 for x in str(v).replace("v", "").split("-")[0].split("."))


def _update_dict(target: TOMLDocument | dict | Table, source: TOMLDocument | dict):
    """
    将source字典的值更新到target字典中（如果target中存在相同的键）
    """
    for key, value in source.items():
        # 跳过version字段的更新
        if key == "version":
            continue
        if key in target:
            target_value = target[key]
            if isinstance(value, dict) and isinstance(target_value, (dict, Table)):
                _update_dict(target_value, value)
            else:
                try:
                    # 对数组类型进行特殊处理
                    if isinstance(value, list):
                        # 如果是空数组，确保它保持为空数组
                        target[key] = tomlkit.array(str(value)) if value else tomlkit.array()
                    else:
                        # 其他类型使用item方法创建新值
                        target[key] = tomlkit.item(value)
                except (TypeError, ValueError):
                    # 如果转换失败，直接赋值
                    target[key] = value


def _update_config_generic(config_name: str, template_name: str, should_quit_on_new: bool = True):
    """
    通用的配置文件更新函数
    
    Args:
        config_name: 配置文件名（不含扩展名），如 'bot_config' 或 'model_config'
        template_name: 模板文件名（不含扩展名），如 'bot_config_template' 或 'model_config_template'
        should_quit_on_new: 创建新配置文件后是否退出程序
    """
    # 获取根目录路径
    old_config_dir = os.path.join(CONFIG_DIR, "old")
    compare_dir = os.path.join(TEMPLATE_DIR, "compare")

    # 定义文件路径
    template_path = os.path.join(TEMPLATE_DIR, f"{template_name}.toml")
    old_config_path = os.path.join(CONFIG_DIR, f"{config_name}.toml")
    new_config_path = os.path.join(CONFIG_DIR, f"{config_name}.toml")
    compare_path = os.path.join(compare_dir, f"{template_name}.toml")

    # 创建compare目录（如果不存在）
    os.makedirs(compare_dir, exist_ok=True)

    template_version = _get_version_from_toml(template_path)
    compare_version = _get_version_from_toml(compare_path)

    # 先读取 compare 下的模板（如果有），用于默认值变动检测
    if os.path.exists(compare_path):
        with open(compare_path, "r", encoding="utf-8") as f:
            compare_config = tomlkit.load(f)
    else:
        compare_config = None

    # 读取当前模板
    with open(template_path, "r", encoding="utf-8") as f:
        new_config = tomlkit.load(f)

    # 检查默认值变化并处理（只有 compare_config 存在时才做）
    if compare_config is not None:
        # 读取旧配置
        with open(old_config_path, "r", encoding="utf-8") as f:
            old_config = tomlkit.load(f)
        logs, changes = compare_default_values(new_config, compare_config)
        if logs:
            logger.info(f"检测到{config_name}模板默认值变动如下：")
            for log in logs:
                logger.info(log)
            # 检查旧配置是否等于旧默认值，如果是则更新为新默认值
            for path, old_default, new_default in changes:
                old_value = get_value_by_path(old_config, path)
                if old_value == old_default:
                    set_value_by_path(old_config, path, new_default)
                    logger.info(
                        f"已自动将{config_name}配置 {'.'.join(path)} 的值从旧默认值 {old_default} 更新为新默认值 {new_default}"
                    )
        else:
            logger.info(f"未检测到{config_name}模板默认值变动")
        # 保存旧配置的变更（后续合并逻辑会用到 old_config）
    else:
        old_config = None

    # 检查 compare 下没有模板，或新模板版本更高，则复制
    if not os.path.exists(compare_path):
        shutil.copy2(template_path, compare_path)
        logger.info(f"已将{config_name}模板文件复制到: {compare_path}")
    else:
        if _version_tuple(template_version) > _version_tuple(compare_version):
            shutil.copy2(template_path, compare_path)
            logger.info(f"{config_name}模板版本较新，已替换compare下的模板: {compare_path}")
        else:
            logger.debug(f"compare下的{config_name}模板版本不低于当前模板，无需替换: {compare_path}")

    # 检查配置文件是否存在
    if not os.path.exists(old_config_path):
        logger.info(f"{config_name}.toml配置文件不存在，从模板创建新配置")
        os.makedirs(CONFIG_DIR, exist_ok=True)  # 创建文件夹
        shutil.copy2(template_path, old_config_path)  # 复制模板文件
        logger.info(f"已创建新{config_name}配置文件，请填写后重新运行: {old_config_path}")
        # 如果是新创建的配置文件，根据参数决定是否退出
        if should_quit_on_new:
            quit()
        else:
            return

    # 读取旧配置文件和模板文件（如果前面没读过 old_config，这里再读一次）
    if old_config is None:
        with open(old_config_path, "r", encoding="utf-8") as f:
            old_config = tomlkit.load(f)
    # new_config 已经读取

    # 检查version是否相同
    if old_config and "inner" in old_config and "inner" in new_config:
        old_version = old_config["inner"].get("version")  # type: ignore
        new_version = new_config["inner"].get("version")  # type: ignore
        if old_version and new_version and old_version == new_version:
            logger.info(f"检测到{config_name}配置文件版本号相同 (v{old_version})，跳过更新")
            return
        else:
            logger.info(
                f"\n----------------------------------------\n检测到{config_name}版本号不同: 旧版本 v{old_version} -> 新版本 v{new_version}\n----------------------------------------"
            )
    else:
        logger.info(f"已有{config_name}配置文件未检测到版本号，可能是旧版本。将进行更新")

    # 创建old目录（如果不存在）
    os.makedirs(old_config_dir, exist_ok=True)  # 生成带时间戳的新文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    old_backup_path = os.path.join(old_config_dir, f"{config_name}_{timestamp}.toml")

    # 移动旧配置文件到old目录
    shutil.move(old_config_path, old_backup_path)
    logger.info(f"已备份旧{config_name}配置文件到: {old_backup_path}")

    # 复制模板文件到配置目录
    shutil.copy2(template_path, new_config_path)
    logger.info(f"已创建新{config_name}配置文件: {new_config_path}")

    # 输出新增和删减项及注释
    if old_config:
        logger.info(f"{config_name}配置项变动如下：\n----------------------------------------")
        logs = compare_dicts(new_config, old_config)
        if logs:
            for log in logs:
                logger.info(log)
        else:
            logger.info("无新增或删减项")

    # 将旧配置的值更新到新配置中
    logger.info(f"开始合并{config_name}新旧配置...")
    _update_dict(new_config, old_config)

    # 保存更新后的配置（保留注释和格式）
    with open(new_config_path, "w", encoding="utf-8") as f:
        f.write(tomlkit.dumps(new_config))
    logger.info(f"{config_name}配置文件更新完成，建议检查新配置文件中的内容，以免丢失重要信息")


def update_config():
    """更新bot_config.toml配置文件"""
    _update_config_generic("bot_config", "bot_config_template", should_quit_on_new=True)


def update_model_config():
    """更新model_config.toml配置文件"""
    _update_config_generic("model_config", "model_config_template", should_quit_on_new=False)


@dataclass
class Config(ConfigBase):
    """总配置类"""

    MMC_VERSION: str = field(default=MMC_VERSION, repr=False, init=False)  # 硬编码的版本信息

    bot: BotConfig
    personality: PersonalityConfig
    relationship: RelationshipConfig
    chat: ChatConfig
    message_receive: MessageReceiveConfig
    normal_chat: NormalChatConfig
    emoji: EmojiConfig
    expression: ExpressionConfig
    memory: MemoryConfig
    mood: MoodConfig
    keyword_reaction: KeywordReactionConfig
    chinese_typo: ChineseTypoConfig
    response_post_process: ResponsePostProcessConfig
    response_splitter: ResponseSplitterConfig
    telemetry: TelemetryConfig
    experimental: ExperimentalConfig
    model: ModelConfig
    maim_message: MaimMessageConfig
    lpmm_knowledge: LPMMKnowledgeConfig
    tool: ToolConfig
    debug: DebugConfig
    custom_prompt: CustomPromptConfig
    voice: VoiceConfig

def load_config(config_path: str) -> Config:
    """
    加载配置文件
    :param config_path: 配置文件路径
    :return: Config对象
    """
    # 读取配置文件
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = tomlkit.load(f)

    # 创建Config对象
    try:
        return Config.from_dict(config_data)
    except Exception as e:
        logger.critical("配置文件解析失败")
        raise e


def get_config_dir() -> str:
    """
    获取配置目录
    :return: 配置目录路径
    """
    return CONFIG_DIR


# 获取配置文件路径
logger.info(f"MaiCore当前版本: {MMC_VERSION}")
update_config()
update_model_config()

logger.info("正在品鉴配置文件...")
global_config = load_config(config_path=os.path.join(CONFIG_DIR, "bot_config.toml"))
model_config = api_ada_load_config(config_path=os.path.join(CONFIG_DIR, "model_config.toml"))
logger.info("非常的新鲜，非常的美味！")