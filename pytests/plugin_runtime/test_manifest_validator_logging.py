from src.plugin_runtime.runner import manifest_validator
from src.plugin_runtime.runner.manifest_validator import ManifestValidator


def test_manifest_error_log_is_deduplicated_by_source_and_errors(monkeypatch):
    error_messages = []
    debug_messages = []

    monkeypatch.setattr(manifest_validator.logger, "error", error_messages.append)
    monkeypatch.setattr(manifest_validator.logger, "debug", debug_messages.append)

    validator = ManifestValidator(validate_python_package_dependencies=False)
    invalid_manifest = {"manifest_version": 2}

    assert validator.parse_manifest(invalid_manifest, source="demo.plugin") is None
    first_errors = list(validator.errors)
    assert validator.parse_manifest(invalid_manifest, source="demo.plugin") is None

    assert validator.errors == first_errors
    assert len(error_messages) == 1
    assert len(debug_messages) == 1
    assert "demo.plugin" in error_messages[0]
    assert "重复出现" in debug_messages[0]


def test_manifest_error_log_keeps_distinct_sources(monkeypatch):
    error_messages = []

    monkeypatch.setattr(manifest_validator.logger, "error", error_messages.append)
    monkeypatch.setattr(manifest_validator.logger, "debug", lambda _message: None)

    validator = ManifestValidator(validate_python_package_dependencies=False)
    invalid_manifest = {"manifest_version": 2}

    assert validator.parse_manifest(invalid_manifest, source="first.plugin") is None
    assert validator.parse_manifest(invalid_manifest, source="second.plugin") is None

    assert len(error_messages) == 2
    assert "first.plugin" in error_messages[0]
    assert "second.plugin" in error_messages[1]


def test_manifest_error_log_can_be_disabled_for_prescan(monkeypatch):
    error_messages = []
    debug_messages = []

    monkeypatch.setattr(manifest_validator.logger, "error", error_messages.append)
    monkeypatch.setattr(manifest_validator.logger, "debug", debug_messages.append)

    validator = ManifestValidator(validate_python_package_dependencies=False, log_errors=False)
    invalid_manifest = {"manifest_version": 2}

    assert validator.parse_manifest(invalid_manifest, source="prescan.plugin") is None

    assert validator.errors
    assert error_messages == []
    assert debug_messages == []
