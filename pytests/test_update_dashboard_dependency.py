from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
import json

from packaging.version import Version
import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / ".github/scripts/update_dashboard_dependency.py"
SPEC = spec_from_file_location("update_dashboard_dependency", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
updater = module_from_spec(SPEC)
SPEC.loader.exec_module(updater)
VERSION = Version("1.9.4.dev20261019539")
WHEEL = f"maibot_dashboard-{VERSION}-py3-none-any.whl"


def index_response(filename: str, yanked: bool = False) -> BytesIO:
    return BytesIO(json.dumps({"files": [{"filename": filename, "yanked": yanked}]}).encode())


def test_waits_for_exact_non_yanked_wheel() -> None:
    responses = [
        index_response("maibot_dashboard-1.9.4.dev20261018538-py3-none-any.whl"),
        index_response(WHEEL, yanked=True),
        index_response(f"maibot_dashboard-{VERSION}.tar.gz"),
        index_response(WHEEL),
    ]
    with patch.object(updater, "urlopen", side_effect=responses) as fetch, patch.object(updater, "sleep") as sleep:
        updater.wait_for_pypi_version(VERSION)

    assert fetch.call_count == 4
    assert sleep.call_count == 3
    assert fetch.call_args.args[0].get_header("Accept") == "application/vnd.pypi.simple.v1+json"


def test_missing_version_times_out() -> None:
    with (
        patch.object(updater, "monotonic", side_effect=[0, 0, 300, 300]),
        patch.object(updater, "urlopen", return_value=index_response("old.tar.gz")),
        patch.object(updater, "sleep"),
        pytest.raises(TimeoutError, match=str(VERSION).replace(".", r"\.")),
    ):
        updater.wait_for_pypi_version(VERSION)


def test_http_errors_are_reported_without_retry() -> None:
    error = HTTPError(updater.PYPI_SIMPLE_URL, 403, "Forbidden", None, None)
    with patch.object(updater, "urlopen", side_effect=error), patch.object(updater, "sleep") as sleep:
        with pytest.raises(HTTPError) as caught:
            updater.wait_for_pypi_version(VERSION)
    assert caught.value is error
    sleep.assert_not_called()


def test_index_failure_does_not_modify_dependencies() -> None:
    with (
        patch.object(updater, "get_target_base_version", return_value=Version("1.9.4")),
        patch.object(updater, "get_target_dev_version", return_value=VERSION),
        patch.object(updater, "wait_for_pypi_version", side_effect=TimeoutError),
        patch.object(updater, "update_pyproject") as update_pyproject,
        patch.object(updater, "update_requirements") as update_requirements,
        pytest.raises(TimeoutError),
    ):
        updater.main()
    update_pyproject.assert_not_called()
    update_requirements.assert_not_called()
