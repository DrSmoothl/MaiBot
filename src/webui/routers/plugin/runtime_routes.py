"""插件运行时相关 WebUI 路由。"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Cookie, HTTPException

from src.plugin_runtime.component_query import component_query_service
from src.plugin_runtime.host.component_registry import CommandEntry, ComponentEntry, ComponentTypes, ToolEntry

from .schemas import HookSpecListResponse, HookSpecResponse
from .support import find_plugin_path_by_id, require_plugin_token, validate_plugin_id

router = APIRouter()


def _ensure_installed_plugin(plugin_id: str) -> None:
    """确认插件 ID 合法且本地已安装。"""

    validate_plugin_id(plugin_id)
    if find_plugin_path_by_id(plugin_id) is None:
        raise HTTPException(status_code=404, detail=f"未找到插件: {plugin_id}")


def _serialize_component_entry(component: ComponentEntry) -> Dict[str, Any]:
    """将插件原始注册组件条目转换为前端展示结构。"""

    component_type = component.component_type.value.lower()
    if isinstance(component, ToolEntry) and component.legacy_component_type.upper() == ComponentTypes.ACTION.value:
        component_type = "action"

    description = str(component.metadata.get("description", "") or component.metadata.get("brief_description", "") or "")
    if isinstance(component, ToolEntry):
        description = component.description

    data: Dict[str, Any] = {
        "name": component.name,
        "description": description,
        "enabled": component.enabled,
        "plugin_name": component.plugin_id,
        "component_type": component_type,
    }

    if component_type == "action":
        data.update(
            {
                "action_parameters": dict(component.metadata.get("action_parameters") or {}),
                "action_require": list(component.metadata.get("action_require") or []),
                "associated_types": list(component.metadata.get("associated_types") or []),
                "activation_type": str(component.metadata.get("activation_type", "") or ""),
                "random_activation_probability": float(component.metadata.get("activation_probability") or 0.0),
                "activation_keywords": list(component.metadata.get("activation_keywords") or []),
                "parallel_action": bool(component.metadata.get("parallel_action", False)),
            }
        )
    elif isinstance(component, ToolEntry):
        data["parameters_schema"] = dict(component._get_parameters_schema() or {})
    elif isinstance(component, CommandEntry):
        data["pattern"] = str(component.metadata.get("command_pattern", "") or "")
        data["aliases"] = list(component.aliases)

    return data


@router.get("/runtime/plugins/{plugin_id}/components")
async def list_plugin_components(plugin_id: str, maibot_session: Optional[str] = Cookie(None)) -> Dict[str, Any]:
    """返回指定插件当前注册的全部组件。"""

    require_plugin_token(maibot_session)
    _ensure_installed_plugin(plugin_id)

    components = []
    for supervisor in component_query_service._iter_supervisors():
        for component in supervisor.component_registry.get_components_by_plugin(plugin_id, enabled_only=False):
            if component.component_type not in {ComponentTypes.COMMAND, ComponentTypes.TOOL}:
                continue
            components.append(_serialize_component_entry(component))

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
