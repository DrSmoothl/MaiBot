from functools import lru_cache
from pathlib import Path
from typing import Any

import tomllib


PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=None)
def read_project_version(project_root: Path | None = None) -> str:
    """读取 pyproject.toml 中声明的主程序版本号。"""

    root = project_root or PROJECT_ROOT
    pyproject_path = root / "pyproject.toml"
    with pyproject_path.open("rb") as pyproject_file:
        pyproject_data: dict[str, Any] = tomllib.load(pyproject_file)

    project_data = pyproject_data.get("project")
    if not isinstance(project_data, dict):
        raise ValueError("pyproject.toml 缺少 [project] 配置节，无法读取主程序版本号")

    version = project_data.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("pyproject.toml 缺少 project.version，无法读取主程序版本号")

    return version.strip()
