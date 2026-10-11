from __future__ import annotations

from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version
import json
import os
import time

import tomlkit


PACKAGE_NAME = os.environ.get("DASHBOARD_PACKAGE_NAME", "maibot-dashboard")
DASHBOARD_PACKAGE_PATH = Path("dashboard/package.json")
PYPROJECT_PATH = Path("pyproject.toml")
REQUIREMENTS_PATH = Path("requirements.txt")
PYPI_SIMPLE_URL = f"https://pypi.org/simple/{canonicalize_name(PACKAGE_NAME)}/"
CHECK_INTERVAL_SECONDS = 15
MAX_CHECK_ATTEMPTS = 40


def find_dashboard_requirement(requirements: Iterable[str]) -> Requirement:
    normalized_package_name = canonicalize_name(PACKAGE_NAME)

    for dependency in requirements:
        parsed_requirement = Requirement(dependency)
        if canonicalize_name(parsed_requirement.name) == normalized_package_name:
            return parsed_requirement

    raise RuntimeError(f"未在依赖列表中找到 {PACKAGE_NAME}")


def dependencies_are_current(target_version: Version) -> bool:
    expected_requirement = f"{PACKAGE_NAME}=={target_version}"

    document = tomlkit.parse(PYPROJECT_PATH.read_text(encoding="utf-8"))
    pyproject_requirement = find_dashboard_requirement(str(item) for item in document["project"]["dependencies"])

    requirement_lines = REQUIREMENTS_PATH.read_text(encoding="utf-8").splitlines()
    requirements_requirement = find_dashboard_requirement(
        line.strip() for line in requirement_lines if line.strip() and not line.strip().startswith("#")
    )

    return str(pyproject_requirement) == expected_requirement and str(requirements_requirement) == expected_requirement


def get_target_stable_version() -> Version:
    package_data = json.loads(DASHBOARD_PACKAGE_PATH.read_text(encoding="utf-8"))
    target_version = Version(str(package_data["version"]))
    if target_version.is_prerelease:
        raise RuntimeError(f"dashboard/package.json 中的 {target_version} 不是正式版本")
    return target_version


def wait_for_published_version(target_version: Version) -> None:
    # 版本 JSON 可见不代表安装索引已同步；等待 uv 实际使用的 Simple 索引出现通用 wheel。
    request = Request(
        PYPI_SIMPLE_URL,
        headers={"Accept": "application/vnd.pypi.simple.v1+json", "Cache-Control": "no-cache"},
    )

    for attempt in range(1, MAX_CHECK_ATTEMPTS + 1):
        print(f"检查 PyPI Simple 索引上的 {PACKAGE_NAME}=={target_version}（第 {attempt}/{MAX_CHECK_ATTEMPTS} 次）")

        pypi_data = None
        try:
            with urlopen(request, timeout=30) as response:
                pypi_data = json.load(response)
        except HTTPError as error:
            if error.code != 404:
                raise
            print(f"PyPI 上暂未找到 {PACKAGE_NAME}=={target_version}")

        if pypi_data is not None:
            for release_file in pypi_data["files"]:
                if release_file["yanked"] is not False or not release_file["filename"].endswith(".whl"):
                    continue
                name, version, _, tags = parse_wheel_filename(release_file["filename"])
                if (
                    name == canonicalize_name(PACKAGE_NAME)
                    and version == target_version
                    and any(str(tag) == "py3-none-any" for tag in tags)
                ):
                    print(f"PyPI Simple 索引已提供 dashboard 正式版本的通用 wheel: {target_version}")
                    return

            print(f"PyPI Simple 索引尚未提供 {PACKAGE_NAME}=={target_version} 的未撤回通用 wheel")

        if attempt < MAX_CHECK_ATTEMPTS:
            time.sleep(CHECK_INTERVAL_SECONDS)

    raise RuntimeError(
        f"等待 {MAX_CHECK_ATTEMPTS * CHECK_INTERVAL_SECONDS} 秒后，"
        f"PyPI Simple 索引上仍未找到 {PACKAGE_NAME}=={target_version} 的未撤回通用 wheel"
    )


def update_pyproject(target_version: Version) -> bool:
    document = tomlkit.parse(PYPROJECT_PATH.read_text(encoding="utf-8"))
    dependencies = document["project"]["dependencies"]
    current_requirement = find_dashboard_requirement(str(item) for item in dependencies)
    updated_dependency = f"{PACKAGE_NAME}=={target_version}"

    if str(current_requirement) == updated_dependency:
        print(f"pyproject.toml 已锁定到目标正式版本: {target_version}")
        return False

    normalized_package_name = canonicalize_name(PACKAGE_NAME)
    for index, dependency in enumerate(dependencies):
        parsed_requirement = Requirement(str(dependency))
        if canonicalize_name(parsed_requirement.name) == normalized_package_name:
            dependencies[index] = updated_dependency
            break

    PYPROJECT_PATH.write_text(tomlkit.dumps(document), encoding="utf-8")
    print(f"pyproject.toml: {current_requirement} -> {updated_dependency}")
    return True


def update_requirements(target_version: Version) -> bool:
    lines = REQUIREMENTS_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    current_requirement = find_dashboard_requirement(
        line.strip() for line in lines if line.strip() and not line.strip().startswith("#")
    )
    updated_requirement = f"{PACKAGE_NAME}=={target_version}"

    if str(current_requirement) == updated_requirement:
        print(f"requirements.txt 已锁定到目标正式版本: {target_version}")
        return False

    normalized_package_name = canonicalize_name(PACKAGE_NAME)
    for index, line in enumerate(lines):
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            continue

        parsed_requirement = Requirement(stripped_line)
        if canonicalize_name(parsed_requirement.name) == normalized_package_name:
            if line.endswith("\r\n"):
                newline = "\r\n"
            elif line.endswith("\n"):
                newline = "\n"
            else:
                newline = ""
            lines[index] = f"{updated_requirement}{newline}"
            break

    REQUIREMENTS_PATH.write_text("".join(lines), encoding="utf-8")
    print(f"requirements.txt: {current_requirement} -> {updated_requirement}")
    return True


def main() -> None:
    target_version = get_target_stable_version()
    print(f"Dashboard 目标正式版本: {target_version}")

    # 即使依赖文件已经写入目标版本，也必须确认 PyPI 上的发布物真实可用。
    wait_for_published_version(target_version)

    if dependencies_are_current(target_version):
        print(f"依赖文件已锁定到 {PACKAGE_NAME}=={target_version}")
        return

    pyproject_updated = update_pyproject(target_version)
    requirements_updated = update_requirements(target_version)

    if pyproject_updated != requirements_updated:
        raise RuntimeError("pyproject.toml 与 requirements.txt 的 dashboard 依赖更新状态不一致")


if __name__ == "__main__":
    main()
