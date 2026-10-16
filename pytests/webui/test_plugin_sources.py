from types import SimpleNamespace
import asyncio
import json

import httpx
import pytest

from src.webui.services import git_mirror_service as git
from src.webui.services import plugin_market_service as market


@pytest.fixture(autouse=True)
def isolated_sources(tmp_path, monkeypatch):
    monkeypatch.setattr(market, "SETTINGS_FILE", tmp_path / "sources.json")
    monkeypatch.setattr(git, "_validate_mirror_prefix", lambda value, _: value)


def configure(**values):
    market.SETTINGS_FILE.write_text(json.dumps(values), encoding="utf-8")


@pytest.mark.parametrize("source,host", [("cnb", "cnb.cool"), ("github", "raw.githubusercontent.com")])
@pytest.mark.parametrize("status", [200, 503])
def test_information_source_is_fixed_and_never_uses_download_source(monkeypatch, source, host, status):
    configure(information_source=source, download_source="custom", custom_download_prefix="https://download.example")
    calls = []
    client = httpx.AsyncClient

    def respond(request):
        calls.append(request.url)
        return httpx.Response(status, text="[]")

    monkeypatch.setattr(market.httpx, "AsyncClient", lambda **kwargs: client(transport=httpx.MockTransport(respond)))
    if status == 200:
        result = asyncio.run(market.fetch_information_file("plugin_details.json"))
        assert result["mirror_used"] == source
    else:
        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(market.fetch_information_file("plugin_details.json"))
    assert len(calls) == 1 and calls[0].host == host


@pytest.mark.parametrize("information", ["official", "cnb", "github"])
@pytest.mark.parametrize("download,host", [("cnb", "cnb.cool"), ("github", "github.com"), ("custom", "download.example")])
def test_download_uses_only_selected_source_even_on_failure(monkeypatch, tmp_path, information, download, host):
    configure(information_source=information, download_source=download, custom_download_prefix="https://download.example")
    service = git.GitMirrorService(config=SimpleNamespace(get_enabled_mirrors=lambda: []))
    calls = []

    async def clone(url, *args, **kwargs):
        calls.append(url)
        return {"success": False, "error": "selected source failed", "mirror_used": download}

    monkeypatch.setattr(service, "_clone_with_url", clone)
    result = asyncio.run(service.clone_repository("Owner", "Plugin", tmp_path, branch="v1.2.3", mirror_id="obsolete"))
    assert not result["success"] and result["error"] == "selected source failed"
    assert len(calls) == 1 and httpx.URL(calls[0]).host == host


def test_release_index_cache_does_not_cross_information_sources(monkeypatch):
    from src.webui.routers.plugin import releases

    monkeypatch.setattr(releases, "_cache", None)
    monkeypatch.setattr(releases, "_cache_source", None)
    calls = []

    async def fetch(path, source):
        calls.append(source)
        return {"success": True, "data": '{"schema_version": 1, "plugins": []}'}

    monkeypatch.setattr(releases, "fetch_information_file", fetch)
    configure(information_source="cnb")
    asyncio.run(releases.load_release_index())
    asyncio.run(releases.load_release_index())
    configure(information_source="github")
    asyncio.run(releases.load_release_index())
    assert calls == ["cnb", "github"]


@pytest.mark.parametrize("source", ["official", "cnb", "github"])
def test_marketplace_listing_only_calls_selected_information_source(monkeypatch, source):
    from src.webui.routers.plugin import marketplace

    configure(information_source=source)
    monkeypatch.setattr(marketplace, "require_plugin_token", lambda token: None)
    calls = []

    async def request(path, params):
        calls.append(path)
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(marketplace, "request_market", request)
    if source == "official":
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as error:
            asyncio.run(marketplace.get_marketplace(False, None))
        assert error.value.status_code == 502
        assert calls == ["plugins/summary"]
    else:
        assert asyncio.run(marketplace.get_marketplace(False, None)) == {"source": "github"}
        assert calls == []
