"""内置电脑 Agent 的模型、工具与观察循环。"""

from typing import TYPE_CHECKING, List, Optional

import asyncio
import json

from src.common.data_models.llm_service_data_models import LLMGenerationOptions
from src.config.config import global_config
from src.core.tooling import ToolAvailabilityContext, ToolExecutionContext, ToolInvocation
from src.llm_models.payload_content.context_item import (
    ContextItem, ContextItemBuilder, ContextItemMeta, FunctionCallItem, FunctionCallOutputItem, RoleType,
    get_item_text,
)
from src.services.llm_service import LLMServiceClient

from .session import AgentSession

if TYPE_CHECKING:
    from .service import ComputerService


class ComputerAgent(AgentSession):
    def __init__(self, service: "ComputerService") -> None:
        super().__init__(service)
        self.client: Optional[LLMServiceClient] = None

    async def compact(self) -> None:
        window = max(4, int(global_config.chat.max_context_size * global_config.computer.context_multiplier))
        if len(self.history.records) - self.summary_until <= window:
            return
        # 完整回合是裁切单元，工具调用与结果从不被拆开；原始 rollout 永久保留。
        until = len(self.history.records) - window // 2
        items = await asyncio.to_thread(lambda: [item for record in self.history.records[self.summary_until:until]
                                                for item in self.history.unpack(record["items"], images=False)])
        instruction = ContextItemBuilder().set_role(RoleType.System).add_text_content(
            "整理电脑工作的连续性笔记：目标、已完成工作、关键路径/操作ID、结果、错误和下一步。保留事实，不执行工具。\n" + self.summary,
        ).build()
        result = await self.get_client().generate_response_with_context(lambda _client: [instruction, *items])
        if not result.response.strip():
            raise ValueError("电脑上下文整理返回空内容")
        self.summary, self.summary_until = result.response, until
        await self.save()

    def get_client(self) -> LLMServiceClient:
        if self.client is None:
            self.client = LLMServiceClient(task_name="computer", request_type="computer.agent", session_id=self.service.agent_id)
        return self.client

    async def finish_pending(self, *, recovering: bool = False) -> bool:
        from .provider import ComputerToolProvider

        provider = ComputerToolProvider(self.service)
        pending = self.pending
        if pending is None:
            return False
        items = await asyncio.to_thread(self.history.unpack, pending["items"])
        calls = [item for item in items if isinstance(item, FunctionCallItem)]
        results = pending["results"]
        for call in calls:
            tool = call.tool_call
            if tool.call_id in results:
                continue
            self.state = "tool"
            if recovering:
                # 不重放中断前可能已生效的工具，桌面操作用持久 operation_id 重新查询。
                operation = self.service.bridge.pending
                content = json.dumps({"status": "interrupted", "outcome": "unknown", "operation_id": operation,
                                      "instruction": "先观察；若有 operation_id，用 computer_poll 查询，不重复输入。"}, ensure_ascii=False)
                success, media, stop = False, [], False
            else:
                context = ToolExecutionContext(session_id=self.service.agent_id,
                                               reasoning="\n".join(get_item_text(item) for item in items))
                result = await provider.invoke(ToolInvocation(tool.func_name, tool.materialize_args(), call_id=tool.call_id), context)
                content = result.get_history_content()
                success, media, stop = result.success, result.content_items, result.stop_after_execution
            output = FunctionCallOutputItem(meta=ContextItemMeta.create(logical_turn_id=call.meta.logical_turn_id),
                                            call_id=tool.call_id, output=content, tool_name=tool.func_name, success=success)
            result_items: List[ContextItem] = [output]
            for image in media:
                if image.content_type == "image":
                    result_items.append(ContextItemBuilder().add_text_content("工具返回的观察画面")
                                        .add_image_content(image.mime_type.split("/")[-1], image.data).build())
            results[tool.call_id] = await asyncio.to_thread(self.history.pack, result_items)
            pending["stop"] = pending["stop"] or stop
            await self.save()
        complete = {key: value for key, value in pending.items() if key in {"id", "time", "kind", "items"}}
        complete["items"] = [*pending["items"], *(raw for call in calls for raw in results[call.tool_call.call_id])]
        await self.history.append(complete)
        stop = pending["stop"]
        self.pending = None
        await self.save()
        return stop

    async def run_turn(self) -> None:
        from .provider import ComputerToolProvider

        try:
            await self.finish_pending(recovering=True)
            while not self.service.paused and not self.service.takeover:
                await self.drain()
                await self.compact()
                self.state = "thinking"
                system = await asyncio.to_thread(self.system_prompt)
                context = await asyncio.to_thread(self.history.context, self.summary_until)
                summary = [ContextItemBuilder().add_text_content(self.summary).build()] if self.summary else []
                environment = ContextItemBuilder().add_text_content(await self.service.context()).build()
                tools = await ComputerToolProvider(self.service).list_tools(ToolAvailabilityContext(session_id=self.service.agent_id))
                request_items = [system, *summary, *context, environment]
                response = await self.get_client().generate_response_with_context(
                    lambda _client, _model=None, items=request_items: items,
                    LLMGenerationOptions(tool_options=[tool.to_llm_definition() for tool in tools]),
                )
                self.pending = {**await self.record("step", response.output_items), "results": {}, "stop": False}
                await self.save()
                stop = await self.finish_pending()
                if stop or not response.tool_calls:
                    return
        finally:
            self.state = "idle"
