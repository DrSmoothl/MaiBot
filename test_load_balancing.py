#!/usr/bin/env python3
"""
测试跨provider负载均衡功能
"""
import asyncio
import sys
import os

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.llm_models.utils_model import LLMRequest
from src.config.config import global_config, model_config

async def test_load_balancing():
    """测试负载均衡功能"""
    print("开始测试负载均衡功能...")
    
    # 初始化配置
    try:
        config = global_config
        print("✓ 配置加载成功")
    except Exception as e:
        print(f"✗ 配置加载失败: {e}")
        return
    
    # 测试具有model_names数组的模型
    test_model_names = []
    
    # 检查bot_config中的模型配置
    print("正在检查配置...")
    if hasattr(config, 'model'):
        model_section = config.model
        print(f"找到model配置节: {type(model_section)}")
        for attr_name in dir(model_section):
            if not attr_name.startswith('_') and not callable(getattr(model_section, attr_name)):
                model_config_obj = getattr(model_section, attr_name)
                print(f"检查 model.{attr_name}: {type(model_config_obj)}")
                if isinstance(model_config_obj, dict):
                    if 'model_names' in model_config_obj and model_config_obj['model_names']:
                        test_model_names.append(f"model.{attr_name}")
                        print(f"✓ 发现支持负载均衡的模型配置: model.{attr_name}, 包含 {len(model_config_obj['model_names'])} 个模型: {model_config_obj['model_names']}")
                    else:
                        print(f"  - 字典中没有model_names字段或为空")
                else:
                    print(f"  - 不是字典类型")
    else:
        print("没有找到model配置节")
    
    if not test_model_names:
        print("✗ 没有找到配置了model_names的模型，请检查配置文件")
        return
    
    # 使用第一个支持负载均衡的模型进行测试
    test_model_config_name = test_model_names[0]
    print(f"\n使用模型配置 {test_model_config_name} 进行测试...")
    
    # 从config中获取实际的模型配置
    section_name, model_config_name = test_model_config_name.split('.', 1)
    model_section = getattr(config, section_name)
    model_config_obj = getattr(model_section, model_config_name)
    
    # 构建model字典参数
    model_dict = {
        "model_names": model_config_obj["model_names"],
        "temperature": model_config_obj.get('temperature', 0.7),
        "max_tokens": model_config_obj.get('max_tokens', 800)
    }
    
    try:
        # 创建LLMRequest实例
        llm_request = LLMRequest(model_dict)
        print("✓ LLMRequest 实例创建成功")
        print(f"  - 当前模型索引: {llm_request.current_model_index}")
        print(f"  - 可用模型数量: {len(llm_request.model_names) if llm_request.model_names else 0}")
        print(f"  - 模型列表: {llm_request.model_names}")
        
        # 测试模型切换功能
        if llm_request._has_more_models():
            print("\n测试模型切换功能...")
            original_index = llm_request.current_model_index
            llm_request._switch_to_next_model()
            new_index = llm_request.current_model_index
            print(f"✓ 模型索引从 {original_index} 切换到 {new_index}")
            
            # 重置回原来的模型
            llm_request._reset_model_index()
            print(f"✓ 模型索引重置到 {llm_request.current_model_index}")
        else:
            print("⚠ 只有一个模型，无法测试切换功能")
        
        # 测试一个简单的请求（这可能会因为API密钥问题而失败，但我们主要测试结构）
        print("\n测试简单请求...")
        try:
            response = await llm_request.generate_response_async("Hello, this is a test message.")
            print(f"✓ 请求成功，响应长度: {len(str(response))}")
        except Exception as e:
            print(f"⚠ 请求失败（这是预期的，可能是API密钥问题）: {e}")
            
    except Exception as e:
        print(f"✗ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_load_balancing())
