from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import import_module
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from threading import Thread
from unittest.mock import Mock

import os
import shutil
import subprocess
import sys
import zipfile

import pytest


@pytest.fixture
def release(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / ".github" / "scripts"))
    return import_module("release_dependencies")


def test_missing_version_refreshes_on_each_retry(release, monkeypatch, tmp_path):
    run = Mock(
        side_effect=[
            CompletedProcess([], 1, "", "Because there is no version of maibot-dashboard==1.9.10 and ..."),
            CompletedProcess([], 0, "installed", ""),
        ]
    )
    sleep = Mock()
    monkeypatch.setattr(release.subprocess, "run", run)
    monkeypatch.setattr(release.time, "sleep", sleep)
    release.run_with_published_package(
        ["uv", "tool", "install", "maibot.whl"],
        "maibot-dashboard",
        "1.9.10",
        cwd=tmp_path,
        env={"UV_CACHE_DIR": "shared-cache", "UV_INDEX": "https://old-mirror.invalid/simple"},
    )
    assert run.call_count == 2
    for call in run.call_args_list:
        command = call.args[0]
        assert command[command.index("--refresh-package") + 1] == "maibot-dashboard"
        assert command[command.index("--default-index") + 1] == "https://pypi.org/simple"
        assert "--no-config" in command
        assert call.kwargs["env"] == {"UV_CACHE_DIR": "shared-cache"}
    sleep.assert_called_once_with(release.CHECK_INTERVAL_SECONDS)


@pytest.mark.parametrize(
    "message,code",
    [
        ("no version of maibot-dashboard==1.9.100", 1),
        ("no version of another-package==1.9.10", 1),
        ("requires Python >=3.14", 1),
        ("HTTP 401 Unauthorized", 2),
        ("connection timed out", 2),
    ],
)
def test_other_errors_fail_without_retry(release, monkeypatch, tmp_path, message, code):
    run = Mock(return_value=CompletedProcess([], code, "", message))
    sleep = Mock()
    monkeypatch.setattr(release.subprocess, "run", run)
    monkeypatch.setattr(release.time, "sleep", sleep)
    with pytest.raises(CalledProcessError) as error:
        release.run_with_published_package(["uv", "lock"], "maibot-dashboard", "1.9.10", cwd=tmp_path)
    assert error.value.stderr == message
    run.assert_called_once()
    sleep.assert_not_called()


def test_missing_version_retry_is_bounded(release, monkeypatch, tmp_path):
    run = Mock(return_value=CompletedProcess([], 1, "", "no version of maibot-dashboard==1.9.10"))
    sleep = Mock()
    monkeypatch.setattr(release, "MAX_CHECK_ATTEMPTS", 2)
    monkeypatch.setattr(release.subprocess, "run", run)
    monkeypatch.setattr(release.time, "sleep", sleep)
    with pytest.raises(CalledProcessError):
        release.run_with_published_package(["uv", "lock"], "maibot-dashboard", "1.9.10", cwd=tmp_path)
    assert run.call_count == 2
    sleep.assert_called_once()


def test_timeout_is_not_retried(release, monkeypatch, tmp_path):
    run = Mock(side_effect=subprocess.TimeoutExpired(["uv"], 120))
    monkeypatch.setattr(release.subprocess, "run", run)
    with pytest.raises(subprocess.TimeoutExpired):
        release.run_with_published_package(["uv", "lock"], "maibot-dashboard", "1.9.10", cwd=tmp_path)
    run.assert_called_once()


def test_smoke_uses_dependency_from_artifact_and_checks_init(release, monkeypatch, tmp_path):
    smoke = import_module("maibot_release")
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "maibot-1.3.16-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "maibot-1.3.16.dist-info/METADATA",
            "Name: maibot\nVersion: 1.3.16\nRequires-Dist: maibot-dashboard==1.9.10\n",
        )
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setattr(smoke, "verify", Mock())
    install = Mock()
    monkeypatch.setattr(smoke, "run_with_published_package", install)
    config = tmp_path / "maibot-smoke" / "instance" / "config"
    config.mkdir(parents=True)
    for name in ("bot_config.toml", "model_config.toml"):
        (config / name).write_text("# unchanged", encoding="utf-8")
    run = Mock()
    monkeypatch.setattr(smoke.subprocess, "run", run)
    smoke.smoke(tmp_path)
    assert install.call_args.args[1:] == ("maibot-dashboard", "1.9.10")
    assert str(wheel) in install.call_args.args[0]
    assert [call.args[0][1] for call in run.call_args_list] == ["--help", "--version", "init", "init"]


def test_prepare_probe_uses_same_release_policy(release, monkeypatch, tmp_path):
    dashboard = import_module("dashboard_dependency")
    project = tmp_path / "pyproject.toml"
    project.write_text(
        '[project]\nrequires-python = ">=3.12"\n[[tool.uv.index]]\nurl = "https://stale-mirror.invalid/simple"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "PYPROJECT_PATH", project)
    probes = []

    def resolve(command, package_name, version, *, cwd):
        probes.append((package_name, version, (cwd / "pyproject.toml").read_text(encoding="utf-8")))

    monkeypatch.setattr(dashboard, "run_with_published_package", resolve)
    dashboard.wait_for_published_version(dashboard.Version("1.9.10"))
    assert probes[0][:2] == ("maibot-dashboard", "1.9.10")
    assert "stale-mirror" not in probes[0][2]


def test_real_uv_refreshes_stale_index_in_installing_job(release, monkeypatch, tmp_path):
    """模拟任务 B 恢复旧索引：服务器已有新包，但普通解析仍看不到它。"""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("需要 uv 来验证真实索引缓存")
    published = False
    requests = []
    wheel = tmp_path / "maibot_dashboard-1.9.10-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "maibot_dashboard-1.9.10.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: maibot-dashboard\nVersion: 1.9.10\n",
        )
        archive.writestr(
            "maibot_dashboard-1.9.10.dist-info/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )

    class IndexHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            if self.path == "/simple/maibot-dashboard/":
                link = f'<a href="/{wheel.name}">{wheel.name}</a>' if published else ""
                body = f"<!doctype html><html><body>{link}</body></html>".encode()
                content_type = "text/html"
            elif self.path == f"/{wheel.name}":
                body = wheel.read_bytes()
                content_type = "application/octet-stream"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):
            self.do_GET()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), IndexHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    index = f"http://127.0.0.1:{server.server_port}/simple"
    monkeypatch.setattr(release, "PYPI_INDEX", index)
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("maibot-dashboard==1.9.10\n", encoding="utf-8")
    env = {key: value for key, value in os.environ.items() if not key.startswith("UV_")}
    env["UV_CACHE_DIR"] = str(tmp_path / "restored-job-cache")
    command = [uv, "pip", "compile", str(requirements), "--python", sys.executable]
    try:

        def resolve_without_refresh():
            return subprocess.run(
                [*command, "--no-config", "--default-index", index],
                cwd=tmp_path,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )

        # 在版本发布前缓存一个成功返回、但缺少目标版本的索引页面。
        before_publish = resolve_without_refresh()
        assert before_publish.returncode == 1, before_publish.stderr
        assert "no version of maibot-dashboard==1.9.10" in before_publish.stderr
        published = True
        count = requests.count("/simple/maibot-dashboard/")
        stale = resolve_without_refresh()
        assert stale.returncode == 1, stale.stderr
        assert requests.count("/simple/maibot-dashboard/") == count
        # 最终使用依赖的任务自己刷新，不能依赖上游任务已经探测成功。
        release.run_with_published_package(
            command,
            "maibot-dashboard",
            "1.9.10",
            cwd=tmp_path,
            env=env,
            timeout=30,
        )
        assert requests.count("/simple/maibot-dashboard/") > count
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
