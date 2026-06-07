from pathlib import Path
from typing import Any

import pytest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import config as config_module
from src.config.config import ConfigManager


@pytest.mark.asyncio
async def test_reload_config_only_loads_bot_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = ConfigManager()
    manager.global_config = object()  # type: ignore[assignment]
    manager.model_config = object()  # type: ignore[assignment]
    loaded_classes: list[type[Any]] = []

    def fake_load_config_from_file(config_class: type[Any], *_args: Any, **_kwargs: Any) -> tuple[object, bool]:
        loaded_classes.append(config_class)
        return object(), False

    monkeypatch.setattr(config_module, "load_config_from_file", fake_load_config_from_file)

    result = await manager.reload_config(changed_scopes=("bot",))

    assert result is True
    assert loaded_classes == [config_module.Config]


@pytest.mark.asyncio
async def test_reload_config_only_loads_model_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = ConfigManager()
    manager.global_config = object()  # type: ignore[assignment]
    manager.model_config = object()  # type: ignore[assignment]
    loaded_classes: list[type[Any]] = []

    def fake_load_config_from_file(config_class: type[Any], *_args: Any, **_kwargs: Any) -> tuple[object, bool]:
        loaded_classes.append(config_class)
        return object(), False

    monkeypatch.setattr(config_module, "load_config_from_file", fake_load_config_from_file)

    result = await manager.reload_config(changed_scopes=("model",))

    assert result is True
    assert loaded_classes == [config_module.ModelConfig]
