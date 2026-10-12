from types import SimpleNamespace
from typing import Any, List

import json

import pytest

from src.config.config import global_config
from src.core.tooling import ToolAnnotation, ToolExecutionContext, ToolExecutionResult, ToolInvocation, ToolSpec
from src.llm_models.payload_content.context_item import (
    AssistantMessageItem,
    ContextItemMeta,
    ContextTextPart,
    ContextToolCall,
    FunctionCallItem,
)
from src.maisaka.builtin_tool import delegate, get_all_builtin_tool_specs
from src.maisaka.builtin_tool.context import BuiltinToolRuntimeContext
from src.maisaka.chat_loop_service import ChatResponse
from src.maisaka.context.messages import ToolResultMessage
from src.maisaka.context.usage import resolve_tool_definition_name


def _response(*, text: str = "", calls: List[tuple[str, dict[str, Any]]] | None = None) -> ChatResponse:
    """构造一条子代理模型响应：可带正文和若干工具调用。"""

    items: list[Any] = []
    if text:
        items.append(AssistantMessageItem(meta=ContextItemMeta.create(logical_turn_id="turn-1"), parts=(ContextTextPart(text),)))
    for index, (name, args) in enumerate(calls or []):
        items.append(
            FunctionCallItem(
                meta=ContextItemMeta.create(logical_turn_id="turn-1"),
                tool_call=ContextToolCall(
                    call_id=f"call-{name}-{index}",
                    func_name=name,
                    args_json=json.dumps(args).encode("utf-8"),
                ),
            )
        )
    return ChatResponse(
        output_items=tuple(items),
        request_messages=[],
        selected_history_count=0,
        tool_count=0,
        prompt_tokens=0,
        built_message_count=0,
        completion_tokens=0,
        total_tokens=0,
    )


class _Registry:
    def __init__(self) -> None:
        self.invocations: list[ToolInvocation] = []
        self.specs = [
            ToolSpec(name="docs_search", description="查文档"),
            ToolSpec(name="reply", description="发言"),
            ToolSpec(name="wipe_data", description="清数据", annotation=ToolAnnotation(destructive=True)),
        ]

    async def list_tools(self, context: Any = None) -> list[ToolSpec]:
        del context
        return list(self.specs)

    async def invoke(self, invocation: ToolInvocation, context: Any = None) -> ToolExecutionResult:
        del context
        self.invocations.append(invocation)
        return ToolExecutionResult(tool_name=invocation.tool_name, success=True, content="文档内容")


def _build_tool_ctx(responses: List[ChatResponse]) -> tuple[BuiltinToolRuntimeContext, _Registry, list[dict[str, Any]]]:
    registry = _Registry()
    sub_agent_requests: list[dict[str, Any]] = []
    pending_responses = list(responses)

    async def run_sub_agent(**kwargs: Any) -> ChatResponse:
        sub_agent_requests.append(
            {
                "history": list(kwargs["extra_messages"]),
                "tool_names": [resolve_tool_definition_name(definition) for definition in kwargs["tool_definitions"]],
                "include_parent_context": kwargs["include_parent_context"],
            }
        )
        return pending_responses.pop(0)

    runtime = SimpleNamespace(log_prefix="[test]", session_id="s", _tool_registry=registry, run_sub_agent=run_sub_agent)
    engine = SimpleNamespace(
        _build_tool_availability_context=lambda: None,
        _build_tool_execution_context=lambda thought: ToolExecutionContext(session_id="s", reasoning=thought),
        _build_tool_invocation=lambda tool_call, thought: ToolInvocation(
            tool_name=tool_call.func_name,
            arguments=dict(tool_call.args or {}),
            call_id=tool_call.call_id,
            reasoning=thought,
        ),
    )
    return BuiltinToolRuntimeContext(engine, runtime), registry, sub_agent_requests


def _invocation(task: str, tools: list[str]) -> ToolInvocation:
    return ToolInvocation(tool_name="delegate", arguments={"task": task, "tools": tools}, call_id="call-delegate")


@pytest.mark.asyncio
async def test_delegate_runs_tool_rounds_in_isolated_context_and_returns_conclusion() -> None:
    tool_ctx, registry, requests = _build_tool_ctx(
        [
            _response(text="先查文档", calls=[("docs_search", {"query": "部署"})]),
            _response(text="部署需要 Python 3.12。"),
        ]
    )

    result = await delegate.handle_tool(tool_ctx, _invocation("查部署要求", ["docs_search", "reply"]))

    assert result.success
    assert result.content.startswith("[子代理结论｜工具使用：docs_search×1]\n部署需要 Python 3.12。")
    assert "未授权的工具：reply" in result.content
    assert [invocation.tool_name for invocation in registry.invocations] == ["docs_search"]
    assert all(request["include_parent_context"] is False for request in requests)
    assert requests[0]["tool_names"] == ["docs_search"]
    # 第二轮的子代理上下文里有任务说明、上一轮输出和对应的工具结果
    second_history = requests[1]["history"]
    assert second_history[0].content == "查部署要求"
    assert isinstance(second_history[-1], ToolResultMessage)
    assert second_history[-1].tool_call_id == "call-docs_search-0"
    assert second_history[-1].logical_turn_id == "turn-1"


@pytest.mark.asyncio
async def test_delegate_rejects_tools_only_the_planner_may_call() -> None:
    tool_ctx, registry, requests = _build_tool_ctx([])

    result = await delegate.handle_tool(tool_ctx, _invocation("替我发言", ["reply", "delegate", "wipe_data", "missing"]))

    assert not result.success
    assert requests == []
    assert registry.invocations == []


@pytest.mark.asyncio
async def test_delegate_blocks_ungranted_tool_calls_and_forces_conclusion_after_round_limit() -> None:
    looping_response = _response(calls=[("reply", {"text": "hi"})])
    tool_ctx, registry, requests = _build_tool_ctx(
        [*([looping_response] * delegate.DELEGATE_MAX_ROUNDS), _response(text="没有查到结果。")]
    )

    result = await delegate.handle_tool(tool_ctx, _invocation("查点东西", ["docs_search"]))

    assert result.success
    assert "没有查到结果。" in result.content
    assert registry.invocations == []
    assert len(requests) == delegate.DELEGATE_MAX_ROUNDS + 1
    assert requests[-1]["tool_names"] == []
    assert not requests[1]["history"][-1].success


def test_delegate_tool_is_exposed_only_when_switch_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(global_config.experimental, "planner_delegate", False)
    assert "delegate" not in {tool_spec.name for tool_spec in get_all_builtin_tool_specs()}

    monkeypatch.setattr(global_config.experimental, "planner_delegate", True)
    assert "delegate" in {tool_spec.name for tool_spec in get_all_builtin_tool_specs()}
