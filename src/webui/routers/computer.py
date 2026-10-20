"""本体电脑状态与控制；桌面操作始终投递到 bot 循环。"""

from typing import Any, Dict, List, Literal
import asyncio
import base64

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel

from src.common.runtime_loop import run_on_main_loop
from src.computer.service import computer_service
from src.webui.dependencies import require_auth

router = APIRouter(prefix="/computer", tags=["computer"], dependencies=[Depends(require_auth)])


class ComputerControl(BaseModel):
    operation: Literal["pause", "resume", "takeover", "stop", "restart"]


@router.get("")
def status():
    return computer_service.status()


@router.post("/control")
async def control(request: ComputerControl):
    if computer_service.agent is None:
        raise HTTPException(status_code=409, detail="电脑流未启动，请先启用 computer.enabled。")
    return await run_on_main_loop(computer_service.control(request.operation))


@router.get("/viewer", response_class=RedirectResponse)
def viewer():
    return RedirectResponse("/computer")


@router.get("/frame")
async def frame():
    if computer_service.agent is None:
        raise HTTPException(status_code=409, detail="电脑功能未启动")
    if computer_service.paused and not computer_service.bridge.running:
        raise HTTPException(status_code=409, detail="沙盒已停止，请先恢复电脑。")
    result = await run_on_main_loop(computer_service.bridge.observe(0))
    if not result["success"]:
        raise HTTPException(status_code=409, detail=result)
    return Response(base64.b64decode(result["image"]), media_type=result["mime_type"],
                    headers={"Cache-Control": "no-store", "X-Capture-Id": result["capture_id"]})


class DesktopInput(BaseModel):
    actions: List[Dict[str, Any]]


@router.post("/input")
async def desktop_input(request: DesktopInput):
    async def execute():
        if not computer_service.takeover:
            raise HTTPException(status_code=409, detail="先接管电脑，再发送人工输入。")
        result = await computer_service.bridge.perform(request.actions, 5)
        return {key: value for key, value in result.items() if key != "image"}
    return await run_on_main_loop(execute())


@router.get("/operations/{operation_id}")
async def poll_operation(operation_id: str):
    async def poll():
        result = await computer_service.bridge.poll(operation_id, 1)
        return {key: value for key, value in result.items() if key != "image"}
    return await run_on_main_loop(poll())


@router.get("/events")
async def events(after: int = 0):
    if after < 0:
        raise HTTPException(status_code=422, detail="after 不能为负数")
    if computer_service.agent is None:
        raise HTTPException(status_code=409, detail="电脑 Agent 未启动")
    async def snapshot():
        agent = computer_service.require_agent()
        # 先取队列，再读已提交记录；输入即使此时被消费，也只会重复而不会漏掉，前端按 ID 合并。
        queued = [{"index": -1, **record} for record in agent.inbox]
        return {"events": await asyncio.to_thread(agent.history.events, after),
                "state": "paused" if computer_service.paused or computer_service.takeover else agent.state,
                "error": computer_service.error, "backend": agent.backend,
                "pending": agent.pending is not None, "queued": queued}
    return await run_on_main_loop(snapshot())


class AgentMessage(BaseModel):
    text: str


@router.post("/message")
async def message(request: AgentMessage):
    if not request.text.strip():
        raise HTTPException(status_code=422, detail="消息不能为空")
    if computer_service.agent is None:
        raise HTTPException(status_code=409, detail="电脑 Agent 未启动")
    async def enqueue():
        agent = computer_service.require_agent()
        return {"id": await agent.enqueue(request.text), "backend": agent.backend}
    return await run_on_main_loop(enqueue())
