"""插件运行时相关 WebUI 路由。"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Cookie, HTTPException

from src.core.types import ActionInfo, ComponentInfo, ComponentType, ToolInfo
from src.plugin_runtime.component_query import component_query_service

from .schemas import HookSpecListResponse, HookSpecResponse
from .support import find_plugin_path_by_id, require_plugin_token, validate_plugin_id

router = APIRouter()


def _ensure_installed_plugin(plugin_id: str) -> None:
    """确认插件 ID 合法且本地已安装。"""

    validate_plugin_id(plugin_id)
    if find_plugin_path_by_id(plugin_id) is None:
        raise HTTPException(status_code=404, detail=f"未找到插件: {plugin_id}")


def _serialize_component_info(component: ComponentInfo) -> Dict[str, Any]:
    """将插件组件快照转换为前端展示结构。"""

    component_type = component.component_type.value
    data: Dict[str, Any] = {
        "name": component.name,
        "description": component.description,
        "enabled": component.enabled,
        "plugin_name": component.plugin_name,
        "component_type": component_type,
    }

    if isinstance(component, ActionInfo):
        data.update(
            {
                "action_parameters": dict(component.action_parameters),
                "action_require": list(component.action_require),
                "associated_types": list(component.associated_types),
                "activation_type": component.activation_type.value,
                "random_activation_probability": component.random_activation_probability,
                "activation_keywords": list(component.activation_keywords),
                "parallel_action": component.parallel_action,
            }
        )
    elif isinstance(component, ToolInfo):
        data["parameters_schema"] = dict(component.parameters_schema or {})

    return data


@router.get("/runtime/plugins/{plugin_id}/components")
async def list_plugin_components(plugin_id: str, maibot_session: Optional[str] = Cookie(None)) -> Dict[str, Any]:
    """返回指定插件当前注册的全部组件。"""

    require_plugin_token(maibot_session)
    _ensure_installed_plugin(plugin_id)

    components = []
    for component_type in (ComponentType.ACTION, ComponentType.COMMAND, ComponentType.TOOL):
        for component in component_query_service.get_components_by_type(component_type).values():
            if component.plugin_name != plugin_id:
                continue
            components.append(_serialize_component_info(component))

    return {"success": True, "components": components}


@router.get("/runtime/hooks", response_model=HookSpecListResponse)
async def list_runtime_hook_specs(maibot_session: Optional[str] = Cookie(None)) -> HookSpecListResponse:
    """返回当前插件运行时公开的 Hook 规格清单。

    Args:
        maibot_session: 当前 WebUI 会话令牌。

    Returns:
        HookSpecListResponse: Hook 规格列表响应。
    """

    require_plugin_token(maibot_session)
    hooks = [HookSpecResponse(**hook_data) for hook_data in component_query_service.list_hook_specs()]
    return HookSpecListResponse(success=True, hooks=hooks)
