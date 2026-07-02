from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.common.prompt_i18n import clear_prompt_cache
from src.webui.dependencies import require_auth
from src.webui.routers import config as config_router_module


@pytest.fixture(name="client")
def client_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    prompts_dir = tmp_path / "prompts"
    custom_prompts_dir = tmp_path / "data" / "custom_prompts"
    source_dir = prompts_dir / "zh-CN"
    source_dir.mkdir(parents=True)
    (source_dir / "replyer.prompt").write_text("Hello {name}", encoding="utf-8")

    monkeypatch.setattr(config_router_module, "PROMPTS_DIR", prompts_dir)
    monkeypatch.setattr(config_router_module, "CUSTOM_PROMPTS_DIR", custom_prompts_dir)
    clear_prompt_cache()

    app = FastAPI()
    app.include_router(config_router_module.router, prefix="/api/webui")
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def test_update_prompt_file_saves_custom_version(client: TestClient) -> None:
    response = client.put(
        "/api/webui/config/prompts/zh-CN/replyer.prompt",
        json={"content": "Hi {name}", "create_version": True, "label": "测试版本"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["content"] == "Hi {name}"
    assert payload["customized"] is True
    assert payload["active_version_id"]
    assert payload["validation"]["valid"] is True
    assert payload["versions"][0]["label"] == "测试版本"
    assert payload["versions"][0]["active"] is True

    catalog_response = client.get("/api/webui/config/prompts")
    assert catalog_response.status_code == 200
    [file_info] = catalog_response.json()["files"]["zh-CN"]
    assert file_info["customized"] is True
    assert file_info["custom_version_count"] == 1


def test_update_prompt_file_rejects_placeholder_mismatch(client: TestClient) -> None:
    response = client.put(
        "/api/webui/config/prompts/zh-CN/replyer.prompt",
        json={"content": "Hi {other}", "create_version": True},
    )

    assert response.status_code == 400
    assert "缺少参数: name" in response.json()["detail"]
    assert "多余参数: other" in response.json()["detail"]


def test_activate_prompt_version_rejects_placeholder_mismatch(client: TestClient) -> None:
    save_response = client.put(
        "/api/webui/config/prompts/zh-CN/replyer.prompt",
        json={"content": "Hi {name}", "create_version": True, "label": "有效版本"},
    )
    version_id = save_response.json()["active_version_id"]

    custom_root = config_router_module.CUSTOM_PROMPTS_DIR
    version_path = custom_root / "zh-CN" / ".versions" / "replyer" / f"{version_id}.prompt"
    version_path.write_text("Hi {other}", encoding="utf-8")

    response = client.post(f"/api/webui/config/prompts/zh-CN/replyer.prompt/versions/{version_id}/activate")

    assert response.status_code == 400
    assert "缺少参数: name" in response.json()["detail"]
    assert "多余参数: other" in response.json()["detail"]
