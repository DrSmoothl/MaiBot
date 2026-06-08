from src.config.official_configs import AMemorixConfig, AMemorixFilterConfig
from src.webui.config_schema import ConfigSchemaGenerator


def test_retrieval_type_filter_defaults_are_disabled() -> None:
    config = AMemorixConfig()

    assert config.filter.retrieval.chat_stream.enabled is False
    assert config.filter.retrieval.chat_summary.enabled is False
    assert config.filter.retrieval.episode.enabled is False
    assert config.filter.retrieval.chat_stream.mode == "blacklist"
    assert config.filter.retrieval.chat_summary.chats == []


def test_retrieval_type_filter_config_schema_is_exposed() -> None:
    schema = ConfigSchemaGenerator.generate_schema(AMemorixFilterConfig)

    assert "retrieval" in schema["nested"]
    retrieval_parent_field = next(field for field in schema["fields"] if field["name"] == "retrieval")
    retrieval_schema = schema["nested"]["retrieval"]
    assert set(retrieval_schema["nested"]) >= {"chat_stream", "chat_summary", "episode"}
    chat_stream_field = next(field for field in retrieval_schema["fields"] if field["name"] == "chat_stream")
    chat_summary_schema = retrieval_schema["nested"]["chat_summary"]
    chat_summary_field = next(field for field in retrieval_schema["fields"] if field["name"] == "chat_summary")
    enabled_field = next(field for field in chat_summary_schema["fields"] if field["name"] == "enabled")

    assert retrieval_parent_field["x-collapsed-by-default"] is True
    assert chat_stream_field["x-collapsed-by-default"] is True
    assert chat_summary_field["x-collapsed-by-default"] is True
    assert enabled_field["type"] == "boolean"
    assert enabled_field["default"] is False
