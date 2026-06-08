from typing import Any

import base64

import pytest

from src.plugin_runtime.integration import PluginRuntimeManager
from src.plugin_runtime.host.supervisor import PluginSupervisor


def test_asr_capability_is_registered() -> None:
    """Host 注册能力时应包含插件 ASR 调用入口。"""

    manager = PluginRuntimeManager()
    supervisor = PluginSupervisor(plugin_dirs=[])

    manager._register_capability_impls(supervisor)

    assert "llm.transcribe_audio" in supervisor.capability_service.list_capabilities()


@pytest.mark.asyncio
async def test_asr_capability_forwards_to_voice_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """llm.transcribe_audio 应调用 Host 当前 voice 任务的音频转写服务。"""

    from src.services import llm_service

    captured: dict[str, Any] = {}

    class _FakeASRClient:
        def __init__(self, task_name: str, request_type: str = "", session_id: str = "") -> None:
            captured["task_name"] = task_name
            captured["request_type"] = request_type
            captured["session_id"] = session_id

        async def transcribe_audio(self, voice_base64: str) -> Any:
            captured["voice_base64"] = voice_base64
            return type("AudioResult", (), {"text": "转写结果"})()

    monkeypatch.setattr(llm_service, "LLMServiceClient", _FakeASRClient)
    monkeypatch.setattr(llm_service, "resolve_task_name", lambda task_name="": task_name or "voice")

    audio_base64 = base64.b64encode(b"voice-bytes").decode("utf-8")
    manager = PluginRuntimeManager()
    result = await manager._cap_llm_transcribe_audio(
        "demo.plugin",
        "llm.transcribe_audio",
        {"audio_base64": f"data:audio/mpeg;base64,{audio_base64}"},
    )

    assert result == {"success": True, "text": "转写结果", "content": "转写结果"}
    assert captured == {
        "task_name": "voice",
        "request_type": "plugin.demo.plugin.asr",
        "session_id": "",
        "voice_base64": audio_base64,
    }


@pytest.mark.asyncio
async def test_asr_capability_rejects_invalid_base64() -> None:
    """非法音频 Base64 不应继续请求模型。"""

    manager = PluginRuntimeManager()
    result = await manager._cap_llm_transcribe_audio(
        "demo.plugin",
        "llm.transcribe_audio",
        {"audio_base64": "not valid base64"},
    )

    assert result["success"] is False
    assert "Base64" in result["error"]
