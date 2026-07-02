from pathlib import Path

import pytest

from src.plugin_runtime.runner.plugin_paths import build_plugin_paths, ensure_child_path, validate_plugin_id_for_path
from src.plugin_runtime.runner.runner_main import PluginRunner


def test_build_plugin_paths_creates_scoped_directories(tmp_path: Path) -> None:
    paths = build_plugin_paths("mai.example-plugin", tmp_path)

    expected_data_dir = (tmp_path / "data" / "plugins" / "mai.example-plugin").resolve()
    expected_runtime_dir = (tmp_path / "temp" / "plugins" / "mai.example-plugin").resolve()

    assert paths.data_dir == expected_data_dir
    assert paths.runtime_dir == expected_runtime_dir
    assert paths.data_dir.is_dir()
    assert paths.runtime_dir.is_dir()


@pytest.mark.parametrize(
    "plugin_id",
    [
        "",
        "single",
        "../escape.plugin",
        "mai/escape.plugin",
        "mai\\escape.plugin",
        "mai..plugin",
    ],
)
def test_validate_plugin_id_for_path_rejects_unsafe_values(plugin_id: str) -> None:
    with pytest.raises(ValueError):
        validate_plugin_id_for_path(plugin_id)


def test_ensure_child_path_rejects_parent_escape(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()

    with pytest.raises(ValueError):
        ensure_child_path(root / ".." / "outside", root)


def test_create_plugin_context_attaches_paths_for_legacy_sdk(tmp_path: Path) -> None:
    class LegacyContext:
        def __init__(self, plugin_id: str, rpc_call: object) -> None:
            self.plugin_id = plugin_id
            self.rpc_call = rpc_call

    paths = build_plugin_paths("mai.example-plugin", tmp_path)
    ctx = PluginRunner._create_plugin_context(
        LegacyContext,
        plugin_id="mai.example-plugin",
        rpc_call=lambda: None,
        plugin_paths=paths,
    )

    assert ctx.plugin_id == "mai.example-plugin"
    assert ctx.paths == paths


def test_create_plugin_context_uses_formal_paths_argument(tmp_path: Path) -> None:
    class CurrentContext:
        def __init__(self, plugin_id: str, rpc_call: object, paths: object) -> None:
            self.plugin_id = plugin_id
            self.rpc_call = rpc_call
            self.paths = paths

    paths = build_plugin_paths("mai.example-plugin", tmp_path)
    ctx = PluginRunner._create_plugin_context(
        CurrentContext,
        plugin_id="mai.example-plugin",
        rpc_call=lambda: None,
        plugin_paths=paths,
    )

    assert ctx.plugin_id == "mai.example-plugin"
    assert ctx.paths == paths
