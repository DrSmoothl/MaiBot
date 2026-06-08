from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import inspect
import json
import pytest

from src.maisaka import chat_loop_service as chat_loop_service_module
from src.maisaka.chat_loop_service import MaisakaChatLoopService


async def _resolve_message_factory(message_factory: Callable[..., Any], client: object) -> list[Any]:
    result = message_factory(client)
    if inspect.isawaitable(result):
        return await result
    return result


class _FakeChatLoopHookManager:
    async def invoke_hook(self, hook_name: str, **kwargs: Any) -> SimpleNamespace:
        del hook_name
        return SimpleNamespace(kwargs=dict(kwargs), aborted=False)


class _FakeChatLoopLLMClient:
    def __init__(self) -> None:
        self.prompt_texts: list[str] = []

    async def generate_response_with_messages(
        self,
        message_factory: Callable[..., Any],
        options: Any = None,
    ) -> SimpleNamespace:
        del options
        messages = await _resolve_message_factory(message_factory, object())
        self.prompt_texts.append("\n".join(message.get_text_content() for message in messages))
        return SimpleNamespace(
            response="ok",
            reasoning="",
            model_name="fake-model",
            tool_calls=[],
            prompt_tokens=1,
            completion_tokens=1,
            total_tokens=2,
        )


def test_expression_selector_uses_text_context() -> None:
    assert MaisakaChatLoopService._resolve_enable_visual_message("expression_selector") is False


def test_reply_effect_judge_uses_text_context() -> None:
    assert MaisakaChatLoopService._resolve_enable_visual_message("reply_effect_judge") is False


@pytest.mark.parametrize("request_kind", ["planner", "timing_gate"])
def test_planner_requests_follow_planner_visual_mode(
    monkeypatch: pytest.MonkeyPatch,
    request_kind: str,
) -> None:
    monkeypatch.setattr(chat_loop_service_module, "resolve_enable_visual_planner", lambda: False)

    assert MaisakaChatLoopService._resolve_enable_visual_message(request_kind) is False


def test_visual_sub_agent_requests_keep_visual_context() -> None:
    assert MaisakaChatLoopService._resolve_enable_visual_message("emotion") is True


@pytest.mark.asyncio
async def test_chat_loop_step_accepts_runtime_system_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(chat_loop_service_module.global_config.debug, "show_maisaka_thinking", False)
    monkeypatch.setattr(chat_loop_service_module.global_config.debug, "record_planner_request", False)

    service = MaisakaChatLoopService(session_id="session/1")
    fake_llm_client = _FakeChatLoopLLMClient()
    monkeypatch.setattr(service, "_get_runtime_manager", lambda: _FakeChatLoopHookManager())
    monkeypatch.setattr(service, "_get_llm_chat_client", lambda request_kind: fake_llm_client)

    response = await service.chat_loop_step(
        [],
        request_kind="timing_gate",
        system_prompt="custom timing gate prompt",
        tool_definitions=[],
    )

    assert response.request_messages[0].get_text_content() == "custom timing gate prompt"
    assert fake_llm_client.prompt_texts[0].startswith("custom timing gate prompt")


def test_save_debug_planner_request_body_uses_llm_request_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(chat_loop_service_module, "DEBUG_PLANNER_CACHE_DIR", tmp_path)
    monkeypatch.setattr(chat_loop_service_module.global_config.debug, "record_planner_request", True)

    service = MaisakaChatLoopService(session_id="session/1")

    service._save_debug_planner_request_body(
        request_kind="planner",
        model_name="demo-model",
        messages=[],
        tool_definitions=[],
        response_format=None,
        selection_reason="test",
        selected_history_count=0,
        response_body={},
        final_response_body={},
    )

    snapshot_files = list(tmp_path.glob("*.json"))
    assert len(snapshot_files) == 1
    payload = json.loads(snapshot_files[0].read_text(encoding="utf-8"))
    assert payload["request_type"] == "maisaka_planner"
