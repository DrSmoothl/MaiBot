"""供内部 Planner、子 Agent 和外接 Agent 共用的电脑工具。"""

from typing import Any, Dict, List, Optional
import asyncio
import json
import traceback

from src.core.tooling import ToolAvailabilityContext, ToolContentItem, ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .actions import action_schemas
from .service import ComputerService, computer_service


def tool_specs() -> List[ToolSpec]:
    definitions = [
        ("computer_desktop", "在 Windows Sandbox 内批量操作并返回截图。先 observe，坐标按最新截图。观察用 computer_observe，查询操作用 computer_poll。"
         "动作：observe；move/click/double_click(x,y,button)；drag(x,y,end_x,end_y,seconds,button)；mouse_down/mouse_up(button)；scroll(delta)；"
         "keys/key_down/key_up(keys 数组，Windows 键名，keys 可加 seconds 持续按住)；type(text)；open(path,arguments 数组，任意沙盒程序或 URL)；"
         "shell(command，沙盒内任意 PowerShell，返回 stdout/stderr/exit_code，工作目录 C:\\MaiWorkspace)；wait(seconds)。"
         "inspect(depth,limit) 读取 UIA；uia(runtime_id,operation,value) 操作控件；browser(method,target,params) 调用沙盒 CDP；"
         "wait_window(title,seconds)、wait_process(pid,seconds) 等待具体条件。"
         "没有探索步数、时长或应用名单限制。timeout 默认60秒，0一直等待；超时返回 running，用 computer_poll 查询，不重复输入。", {
            "actions": {"type": "array", "items": {"oneOf": action_schemas()}, "minItems": 1},
            "timeout": {"type": "number", "minimum": 0},
        }, ["actions"]),
        ("computer_observe", "独立获取当前桌面，长命令执行期间也可观察；返回截图编号、时间、活动窗口及尺寸。", {
            "timeout": {"type": "number", "minimum": 0},
        }, []),
        ("computer_poll", "查询原操作，不重复执行。running 是正常等待，completed/failed/cancelled 是最终结果。", {
            "operation_id": {"type": "string"}, "timeout": {"type": "number", "minimum": 0},
        }, ["operation_id"]),
        ("computer_cancel", "取消指定操作并释放按键，不关闭桌面；已完成的动作不会回滚。running 表示尚未确认取消。", {
            "operation_id": {"type": "string"},
        }, ["operation_id"]),
    ]
    return [ToolSpec(name=name, description=description, provider_name="computer", provider_type="builtin",
                     parameters_schema={"type": "object", "properties": properties, "required": required,
                                        "additionalProperties": False}, metadata={"visibility": "deferred"})
            for name, description, properties, required in definitions]


class ComputerToolProvider:
    provider_name = "computer"
    provider_type = "builtin"

    def __init__(self, service: ComputerService = computer_service, *, visible: bool = False) -> None:
        self.service = service
        self.visible = visible

    async def list_tools(self, context: Optional[ToolAvailabilityContext] = None) -> List[ToolSpec]:
        if not self.service.config.enabled:
            return []
        specs = tool_specs()
        if self.visible:
            for spec in specs:
                spec.metadata["visibility"] = "visible"
        return specs

    async def invoke(self, invocation: ToolInvocation, context: Optional[ToolExecutionContext] = None) -> ToolExecutionResult:
        try:
            if context is None or not context.session_id:
                raise PermissionError("电脑工具需要可信执行上下文。")
            name, args = invocation.tool_name, invocation.arguments
            result: Dict[str, Any]
            if name == "computer_desktop":
                result = await self.service.desktop(args["actions"], args.get("timeout"),
                                                    reason=context.reasoning, caller=context.session_id)
            elif name == "computer_observe":
                result = await self.service.observe(args.get("timeout"))
            elif name == "computer_poll":
                result = await self.service.poll(args["operation_id"], args.get("timeout"))
            elif name == "computer_cancel":
                result = await self.service.cancel(args["operation_id"])
            else:
                raise ValueError(f"未知电脑工具：{name}")
            media = [ToolContentItem(content_type="image", data=result["image"], mime_type=result["mime_type"])] if "image" in result else []
            payload = {key: value for key, value in result.items() if key != "image"}
            return ToolExecutionResult(tool_name=name, success=result.get("success", True),
                                       content=await asyncio.to_thread(json.dumps, payload, ensure_ascii=False),
                                       structured_content=payload, content_items=media,
                                       error_message="\n".join(result[key] for key in ("error", "capture_error") if key in result))
        except (OSError, RuntimeError, ValueError, KeyError):
            return ToolExecutionResult(tool_name=invocation.tool_name, success=False, error_message=traceback.format_exc())

    async def close(self) -> None:
        """聊天或外接 Agent 结束时不关闭共享电脑。"""
