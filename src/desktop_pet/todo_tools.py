"""主人聊天通过窗口进程管理同一份待办，不在主循环读写文件。"""

from typing import List, Optional
import json

from src.common.runtime_loop import run_on_main_loop
from src.core.tooling import ToolAvailabilityContext, ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .chat_tools import is_owner_chat
from .service import desktop_pet_service


class TodoToolProvider:
    provider_name = "desktop_pet_todos"
    provider_type = "builtin"

    async def list_tools(self, context: Optional[ToolAvailabilityContext] = None) -> List[ToolSpec]:
        if context is None or not is_owner_chat(context.session_id):
            return []
        return [ToolSpec(
            name="desktop_pet_todo", provider_name=self.provider_name, provider_type=self.provider_type,
            description="查看和管理主人的桌宠待办。list 查询全部；add 新增；update 修改；delete 删除或完成（完成即删除）。修改、删除前先查询真实 todo_id，不猜 ID。桌宠须已启动。",
            parameters_schema={"type": "object", "additionalProperties": False, "required": ["action"],
                "properties": {
                    "action": {"type": "string", "enum": ["list", "add", "update", "delete"]},
                    "todo_id": {"type": "string", "description": "update、delete 必填，使用查询结果中的 id"},
                    "text": {"type": "string", "minLength": 1, "maxLength": 200,
                             "description": "add、update 必填，新待办内容"},
                }},
        )]

    async def invoke(self, invocation: ToolInvocation, context: Optional[ToolExecutionContext] = None) -> ToolExecutionResult:
        try:
            if context is None or not is_owner_chat(context.session_id):
                raise PermissionError("只有主人内部聊天可以管理待办")
            if invocation.tool_name != "desktop_pet_todo":
                raise ValueError("未知待办工具")
            args = invocation.arguments
            action = args["action"]
            if action not in {"list", "add", "update", "delete"}:
                raise ValueError("未知待办操作")
            command = {"type": "todo", "action": action}
            if action in {"add", "update"}:
                text = args.get("text")
                if not isinstance(text, str) or not 1 <= len(text.strip()) <= 200:
                    raise ValueError("待办内容必须为 1 至 200 字")
                command["text"] = text.strip()
            if action in {"update", "delete"}:
                todo_id = args.get("todo_id")
                if not isinstance(todo_id, str) or not todo_id.strip():
                    raise ValueError("请先查询待办并提供 todo_id")
                command["todo_id"] = todo_id
            result = await run_on_main_loop(desktop_pet_service._command(command))
            return ToolExecutionResult(tool_name=invocation.tool_name, success=True,
                content=json.dumps(result, ensure_ascii=False), structured_content=result)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            return ToolExecutionResult(tool_name=invocation.tool_name, success=False, error_message=str(exc))

    async def close(self) -> None:
        pass
