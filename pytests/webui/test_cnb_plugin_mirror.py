from types import SimpleNamespace

import asyncio
import json
import subprocess

import pytest

from src.webui.services import git_mirror_service as mirrors
from src.webui.services import plugin_market_service as market


def config_for(*items):
    return SimpleNamespace(get_enabled_mirrors=lambda: list(items),
                           get_mirror_by_id=lambda key: next((item for item in items if item["id"] == key), None))


@pytest.fixture(autouse=True)
def isolate_source_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(market, "SETTINGS_FILE", tmp_path / "plugin_market.json")


@pytest.fixture
def cnb(monkeypatch):
    monkeypatch.setattr(mirrors, "_validate_mirror_prefix", lambda value, _: value)
    return next(mirror.copy() for mirror in mirrors.GitMirrorConfig.DEFAULT_MIRRORS if mirror["id"] == "cnb")


def test_cnb_mapping_matches_registry_and_keeps_original_identity(cnb):
    assert mirrors.cnb_repository_name("Mai-with-u", "plugin-repo") == "plugin-repo"
    assert mirrors.cnb_repository_name("Owner", "Plugin") == "p-owner--plugin-a81af15dea23"
    assert mirrors.cnb_repository_name("OWNER", "Plugin.git") == mirrors.cnb_repository_name("owner", "plugin")
    assert mirrors.cnb_repository_name("OWNER", "Plugin.GIT") == mirrors.cnb_repository_name("owner", "plugin")
    assert mirrors.mirror_raw_url("Mai-with-u", "plugin-repo", "main", "plugin_versions.json", cnb) == (
        "https://cnb.cool/Mai-with-u/plugin-repo/-/git/raw/main/plugin_versions.json")
    assert "/-/git/raw/feature%2Ftest/" in mirrors.mirror_raw_url("owner", "plugin", "feature/test", "a.json", cnb)
    with pytest.raises(ValueError):
        mirrors.mirror_raw_url("owner", "plugin", "main", "../secret", cnb)


def test_existing_config_migrates_once_and_respects_user_disable_or_delete(tmp_path, monkeypatch):
    target = tmp_path / "webui.json"
    monkeypatch.setattr(mirrors.GitMirrorConfig, "CONFIG_FILE", target)
    original = {"id": "github", "priority": 9, "enabled": False, "updated_at": "custom"}
    target.write_text(json.dumps({"git_mirrors": [original], "other_setting": "preserved"}), encoding="utf-8")
    config = mirrors.GitMirrorConfig()
    assert config.get_mirror_by_id("github") == original
    assert config.get_mirror_by_id("cnb")["priority"] == 0
    config.delete_mirror("cnb")
    reopened = mirrors.GitMirrorConfig()
    assert reopened.get_mirror_by_id("cnb") is None
    assert json.loads(target.read_text(encoding="utf-8"))["other_setting"] == "preserved"


def test_cnb_html_error_page_does_not_stop_automatic_raw_source_selection(cnb, monkeypatch):
    github = next(m.copy() for m in mirrors.GitMirrorConfig.DEFAULT_MIRRORS if m["id"] == "github")
    service = mirrors.GitMirrorService(max_retries=1, config=config_for(cnb, github))
    calls = []

    async def fetch(url, source):
        calls.append(source)
        return {"success": True, "data": "<html>login</html>" if source == "cnb" else "[]", "mirror_used": source}

    monkeypatch.setattr(service, "_fetch_with_url", fetch)
    result = asyncio.run(service.fetch_raw_file("Mai-with-u", "plugin-repo", "main", "plugins.json", report_progress=False))
    assert result["success"] and result["mirror_used"] == "github"
    assert calls == ["cnb", "github"]
    calls.clear()
    result = asyncio.run(service.fetch_raw_file("Mai-with-u", "plugin-repo", "main", "plugins.json", mirror_id="cnb",
                                               report_progress=False))
    assert not result["success"] and calls == ["cnb"]


def test_release_tag_clone_uses_cnb_but_does_not_replace_tag_or_depth(cnb, monkeypatch, tmp_path):
    service = mirrors.GitMirrorService(max_retries=1, config=config_for(cnb))
    calls = []

    async def clone(url, target_path, branch, depth, source, *args, **kwargs):
        calls.append((url, branch, depth, source))
        return {"success": True}

    monkeypatch.setattr(service, "_clone_with_url", clone)
    asyncio.run(service.clone_repository("Owner", "Plugin", tmp_path, branch="v1.2.3", depth=1, mirror_id="cnb"))
    assert calls == [("https://cnb.cool/Mai-with-u/p-owner--plugin-a81af15dea23.git", "v1.2.3", 1, "cnb")]


@pytest.mark.parametrize("merge_fails", [False, True])
def test_branch_update_uses_selected_mirror_and_never_switches_on_local_merge_failure(cnb, monkeypatch, tmp_path, merge_fails):
    (tmp_path / ".git").mkdir()
    github = next(m.copy() for m in mirrors.GitMirrorConfig.DEFAULT_MIRRORS if m["id"] == "github")
    service = mirrors.GitMirrorService(config=config_for(cnb, github))
    calls = []

    async def run(cmd, cwd=None):
        calls.append(cmd)
        return SimpleNamespace(returncode=1 if merge_fails and cmd[1] == "merge" else 0,
                               stdout="", stderr="local conflict" if merge_fails else "")

    monkeypatch.setattr(mirrors, "_run_git_process", run)
    result = asyncio.run(service.pull_repository(tmp_path, branch="main", remote_url="https://github.com/Owner/Plugin",
                                                mirror_id="cnb"))
    assert result["success"] is not merge_fails
    fetches = [cmd for cmd in calls if cmd[1] == "fetch"]
    assert len(fetches) == 1 and fetches[0][3].startswith("https://cnb.cool/")
    assert not any(cmd[1] == "remote" for cmd in calls)
    assert [cmd[1] for cmd in calls][-1] == "merge"


def test_branch_fetch_failure_does_not_try_another_source(cnb, monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    github = next(m.copy() for m in mirrors.GitMirrorConfig.DEFAULT_MIRRORS if m["id"] == "github")
    service = mirrors.GitMirrorService(config=config_for(cnb, github))
    fetches = []

    async def run(cmd, cwd=None):
        if cmd[1] == "fetch":
            fetches.append(cmd[3])
        return SimpleNamespace(returncode=int(cmd[1] == "fetch" and "cnb.cool" in cmd[3]), stdout="", stderr="missing")

    monkeypatch.setattr(mirrors, "_run_git_process", run)
    result = asyncio.run(service.pull_repository(tmp_path, branch="main", remote_url="https://github.com/Owner/Plugin"))
    assert not result["success"]
    assert len(fetches) == 1


def test_update_without_repository_url_resolves_github_origin(cnb, monkeypatch, tmp_path):
    (tmp_path / ".git").mkdir()
    service = mirrors.GitMirrorService(config=config_for(cnb))
    commands = []

    async def run(cmd, cwd=None):
        commands.append(cmd)
        return SimpleNamespace(returncode=0, stdout="https://github.com/Owner/Plugin\n" if cmd[1] == "remote" else "", stderr="")

    monkeypatch.setattr(mirrors, "_run_git_process", run)
    result = asyncio.run(service.pull_repository(tmp_path, branch="main", mirror_id="cnb"))
    assert result["success"]
    assert any(cmd[1] == "fetch" and "cnb.cool" in cmd[3] for cmd in commands)


def test_permanent_raw_404_is_not_retried(cnb, monkeypatch):
    import httpx

    requests = []
    original = httpx.AsyncClient

    def respond(request):
        requests.append(request)
        return httpx.Response(404)

    monkeypatch.setattr(mirrors.httpx, "AsyncClient", lambda **kwargs: original(
        **kwargs, transport=httpx.MockTransport(respond), trust_env=False))
    service = mirrors.GitMirrorService(max_retries=3, config=config_for(cnb))
    result = asyncio.run(service.fetch_raw_file("Mai-with-u", "plugin-repo", "main", "plugins.json", mirror_id="cnb",
                                               report_progress=False))
    assert not result["success"] and result["status_code"] == 404
    assert result["attempts"] == len(requests) == 1


def test_real_git_update_preserves_user_files_origin_and_rejects_divergence(cnb, monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()

    def git(*args, cwd=source):
        return subprocess.check_output(["git", *args], cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()

    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    (source / "plugin.py").write_text("v1", encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "v1")
    installed = tmp_path / "installed"
    git("clone", str(source), str(installed))
    (installed / "config.toml").write_text("user = true", encoding="utf-8")
    (source / "plugin.py").write_text("v2", encoding="utf-8")
    git("commit", "-am", "v2")
    monkeypatch.setattr(mirrors, "mirror_clone_url", lambda *_: str(source))
    service = mirrors.GitMirrorService(config=config_for(cnb))
    result = asyncio.run(service.pull_repository(installed, branch="main", remote_url="https://github.com/Owner/Plugin"))
    assert result["success"]
    assert git("rev-parse", "HEAD", cwd=installed) == git("rev-parse", "HEAD")
    assert (installed / "config.toml").read_text(encoding="utf-8") == "user = true"
    assert git("remote", "get-url", "origin", cwd=installed) == str(source)
    git("config", "user.name", "Test", cwd=installed)
    git("config", "user.email", "test@example.com", cwd=installed)
    (installed / "plugin.py").write_text("local", encoding="utf-8")
    git("commit", "-am", "local change", cwd=installed)
    (source / "plugin.py").write_text("v3", encoding="utf-8")
    git("commit", "-am", "v3")
    result = asyncio.run(service.pull_repository(installed, branch="main", remote_url="https://github.com/Owner/Plugin"))
    assert not result["success"] and result["status_code"] == 409
    assert (installed / "plugin.py").read_text(encoding="utf-8") == "local"
