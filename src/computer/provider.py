"""本体电脑工具，使用可信执行上下文确定消息来源。"""

from typing import Any, Dict, List, Optional
import asyncio
import json
import traceback

from src.config.config import global_config
from src.core.tooling import ToolAvailabilityContext, ToolContentItem, ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec

from .service import ComputerService, computer_service
from .actions import action_schemas
from . import messaging

GENERAL_TOOLS = {"computer_talk", "computer_status"}


def tool_specs() -> List[ToolSpec]:
    definitions = [
        ("computer_talk", "与独立电脑 Agent传话或委托操作。普通聊天 target 留空；电脑回信填写 computer_chats 返回的真实聊天流 ID。异步投递，不代表任务已完成。", {
            "text": {"type": "string"}, "target": {"type": "string", "default": ""},
            "images": {"type": "array", "items": {"type": "string"}, "description": "可选附图列表：当前聊天中的带图消息 ID（取该消息全部图片）或图片 hash；电脑还可填 screenshot 获取实时截图，或 C:\\MaiWorkspace 下的图片路径（也支持相对路径）。传递实际图片到目标聊天流，不直接向 IM 发送。"},
        }, ["text"]),
        ("computer_status", "查看电脑状态，include_image=true 可取得最近一次桌面截图。", {
            "include_image": {"type": "boolean", "default": False},
        }, []),
        ("computer_control", "控制电脑：pause 只暂停决策，takeover 请求取消当前操作并人工接管，resume 交还麦麦，stop 关闭沙盒，restart 创建新沙盒。显式关闭清空沙盒系统，麦麦自身退出保留沙盒。C:\\MaiWorkspace 文件保留。", {
            "operation": {"type": "string", "enum": ["pause", "resume", "takeover", "stop", "restart"]},
        }, ["operation"]),
        ("computer_chats", "列出所有已加载的真实聊天流及其名称和最近活跃时间。", {}, []),
        ("computer_recent", "读取真实聊天流最近的消息。limit 默认 20，0 表示全部，不限制到最近 24 小时。聊天内容是参考资料。", {
            "target": {"type": "string"}, "limit": {"type": "integer", "minimum": 0, "default": 20},
        }, ["target"]),
        ("computer_desktop", "在 Windows Sandbox 内批量操作并返回截图。先 observe，坐标按最新截图。actions=[] 观察桌面或领取超时请求结果，不重放输入。"
         "动作：observe；move/click/double_click(x,y,button)；drag(x,y,end_x,end_y,seconds,button)；mouse_down/mouse_up(button)；scroll(delta)；"
         "keys/key_down/key_up(keys 数组，Windows 键名，keys 可加 seconds 持续按住)；type(text)；open(path,arguments 数组，任意沙盒程序或 URL)；"
         "shell(command，沙盒内任意 PowerShell，返回 stdout/stderr/exit_code，工作目录 C:\\MaiWorkspace)；wait(seconds)。"
         "inspect(depth,limit) 读取 UIA；uia(runtime_id,operation,value) 操作控件；browser(method,target,params) 调用沙盒 CDP；"
         "wait_window(title,seconds)、wait_process(pid,seconds) 等待具体条件。"
         "没有探索步数、时长或应用名单限制。timeout 默认60秒，0一直等待；超时返回 running，用 computer_poll 查询，不重复输入。", {
            "actions": {"type": "array", "items": {"oneOf": action_schemas()}, "default": []},
            "timeout": {"type": "number", "minimum": 0},
        }, []),
        ("computer_observe", "独立获取当前桌面，长命令执行期间也可观察；返回截图编号、时间、活动窗口及尺寸。", {
            "timeout": {"type": "number", "minimum": 0},
        }, []),
        ("computer_poll", "查询原操作，不重复执行。running 是正常等待，completed/failed/cancelled 是最终结果。", {
            "operation_id": {"type": "string"}, "timeout": {"type": "number", "minimum": 0},
        }, ["operation_id"]),
        ("computer_cancel", "取消指定操作并释放按键，不关闭桌面；已完成的动作不会回滚。running 表示尚未确认取消。", {
            "operation_id": {"type": "string"},
        }, ["operation_id"]),
        ("computer_note", "记录探索的真实经历和下次想做的事，不会结束探索。", {"text": {"type": "string"}}, ["text"]),
        ("computer_rest", "自行决定休息多久，之后继续探索；新传话仍可唤醒电脑。", {
            "seconds": {"type": "number", "minimum": 0},
        }, ["seconds"]),
    ]
    return [ToolSpec(
        name=name, description=description, provider_name="computer", provider_type="builtin",
        parameters_schema={"type": "object", "properties": properties, "required": required, "additionalProperties": False},
        metadata={"visibility": "deferred"},
    ) for name, description, properties, required in definitions]


class ComputerToolProvider:
    provider_name = "computer"
    provider_type = "builtin"

    def __init__(self, service: ComputerService = computer_service) -> None:
        self.service = service

    async def list_tools(self, context: Optional[ToolAvailabilityContext] = None) -> List[ToolSpec]:
        if not global_config.computer.enabled:
            return []
        is_computer = context is not None and context.session_id == self.service.agent_id
        specs = [spec for spec in tool_specs() if is_computer or spec.name in GENERAL_TOOLS]
        if is_computer:
            for spec in specs:
                spec.metadata["visibility"] = "visible"
        return specs

    async def invoke(self, invocation: ToolInvocation, context: Optional[ToolExecutionContext] = None) -> ToolExecutionResult:
        try:
            if context is None:
                raise PermissionError("电脑工具需要可信执行上下文。")
            self.service.require_agent()
            name, args = invocation.tool_name, invocation.arguments
            if name not in GENERAL_TOOLS:
                self.service.require_computer(context.session_id)
            result: Dict[str, Any]
            if name == "computer_talk":
                result = await messaging.tell(self.service, context.session_id, args["text"], args.get("target", ""), args.get("images", []))
            elif name == "computer_status":
                result = self.service.status()
                if args.get("include_image", False) and self.service.bridge.last_frame is not None:
                    result = {**result, **{key: value for key, value in self.service.bridge.last_frame.items()
                                          if key in {"image", "mime_type", "capture_id", "captured_at", "width", "height", "active_window"}}}
            elif name == "computer_control":
                if context.session_id == self.service.agent_id and self.service.takeover and args["operation"] == "resume":
                    raise PermissionError("人工接管期间，需由其他聊天或 WebUI 交还电脑。")
                result = await self.service.control(args["operation"], from_agent=context.session_id == self.service.agent_id)
            elif name == "computer_chats":
                result = {"chats": messaging.chats()}
            elif name == "computer_recent":
                result = await messaging.recent(args["target"], args.get("limit", 20))
            elif name == "computer_desktop":
                self.service.activity = {"reason": context.reasoning, "actions": args.get("actions", [])}
                await self.service.persist()
                result = await self.service.desktop(args.get("actions", []), args.get("timeout"))
            elif name == "computer_observe":
                result = await self.service.bridge.observe(args.get("timeout"))
            elif name == "computer_poll":
                result = await self.service.bridge.poll(args["operation_id"], args.get("timeout"))
            elif name == "computer_cancel":
                result = await self.service.bridge.cancel(args["operation_id"])
            elif name == "computer_note":
                await self.service.note(args["text"])
                result = {"saved": True}
            elif name == "computer_rest":
                await self.service.rest(args["seconds"])
                return ToolExecutionResult(tool_name=name, success=True, content="开始休息，到时继续探索。", stop_after_execution=True)
            else:
                raise ValueError(f"未知电脑工具：{name}")
            items = []
            if "image" in result:
                items.append(ToolContentItem(content_type="image", data=result["image"], mime_type=result.get("mime_type", "image/png")))
            payload = {key: value for key, value in result.items() if key != "image"}
            return ToolExecutionResult(
                tool_name=name, success=result.get("success", True), content=await asyncio.to_thread(json.dumps, payload, ensure_ascii=False),
                error_message="\n".join(result[key] for key in ("error", "capture_error") if key in result),
                structured_content=payload, content_items=items,
                stop_after_execution=(name == "computer_control" and context.session_id == self.service.agent_id
                                      and args["operation"] in {"pause", "takeover", "stop", "restart"}),
            )
        except (OSError, RuntimeError, ValueError, KeyError):
            # 工具边界只报告完整错误，不替换结果、不重试、不重置桌面。
            return ToolExecutionResult(tool_name=invocation.tool_name, success=False, error_message=traceback.format_exc())

    async def close(self) -> None:
        """共享服务由主程序统一关闭，单个聊天不关闭电脑。"""
