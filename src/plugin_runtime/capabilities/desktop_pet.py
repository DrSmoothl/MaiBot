"""内置桌宠控制能力；不允许插件执行窗口脚本或改写主人聊天记录。"""

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, StrictBool

from src.common.runtime_loop import run_on_main_loop
from src.desktop_pet.service import desktop_pet_service


class EmptyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class SayRequest(EmptyRequest):
    text: str = Field(min_length=1, max_length=2000)


class ThinkingRequest(EmptyRequest):
    active: StrictBool


async def control(plugin_id: str, capability: str, args: Dict[str, Any]) -> Dict[str, Any]:
    del plugin_id
    action = capability.removeprefix("desktop_pet.")
    if action == "say":
        request = SayRequest.model_validate(args)
    elif action == "set_thinking":
        request = ThinkingRequest.model_validate(args)
    elif action in {"status", "show", "hide"}:
        request = EmptyRequest.model_validate(args)
    else:
        raise ValueError(f"未知桌宠能力: {capability}")
    result = await run_on_main_loop(desktop_pet_service.control(action, **request.model_dump()))
    return {"success": True, "result": result}
