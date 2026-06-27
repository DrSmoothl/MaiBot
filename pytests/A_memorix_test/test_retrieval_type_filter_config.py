import json
from pathlib import Path

from src.config.config_base import AttributeData
from src.config.official_configs import AMemorixConfig, AMemorixFilterConfig, AMemorixRetrievalConfig
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


def test_relation_vectorization_config_is_loaded_and_exposed() -> None:
    attribute_data = AttributeData()
    payload = {
        "relation_vectorization": {
            "enabled": True,
            "backfill_enabled": True,
            "write_on_import": False,
        }
    }

    config = AMemorixRetrievalConfig.from_dict(attribute_data, payload)
    dumped = config.model_dump(mode="json")
    schema = ConfigSchemaGenerator.generate_schema(AMemorixRetrievalConfig)
    relation_field = next(field for field in schema["fields"] if field["name"] == "relation_vectorization")
    relation_schema = schema["nested"]["relation_vectorization"]

    assert "relation_vectorization" not in attribute_data.redundant_attributes
    relation_config = dumped["relation_vectorization"]
    assert relation_config["enabled"] is True
    assert relation_config["backfill_enabled"] is True
    assert relation_config["write_on_import"] is False
    assert relation_field["type"] == "object"
    assert relation_schema["className"] == "AMemorixRelationVectorizationConfig"
    assert {field["name"] for field in relation_schema["fields"]} == {
        "enabled",
        "backfill_enabled",
        "write_on_import",
    }


def test_vector_pools_config_is_loaded_and_exposed() -> None:
    attribute_data = AttributeData()
    payload = {
        "vector_pools": {
            "mode": "dual",
            "graph_top_k": 32,
            "graph_weight": 0.25,
            "relation_intent": {
                "graph_top_k": 64,
                "graph_weight": 0.45,
            },
        }
    }

    config = AMemorixRetrievalConfig.from_dict(attribute_data, payload)
    dumped = config.model_dump(mode="json")
    schema = ConfigSchemaGenerator.generate_schema(AMemorixRetrievalConfig)
    vector_pools_field = next(field for field in schema["fields"] if field["name"] == "vector_pools")
    vector_pools_schema = schema["nested"]["vector_pools"]

    assert "vector_pools" not in attribute_data.redundant_attributes
    vector_pools = dumped["vector_pools"]
    assert vector_pools["mode"] == "dual"
    assert vector_pools["graph_top_k"] == 32
    assert vector_pools["graph_weight"] == 0.25
    assert vector_pools["relation_intent"]["graph_top_k"] == 64
    assert vector_pools["relation_intent"]["graph_weight"] == 0.45
    assert vector_pools_field["type"] == "object"
    assert vector_pools_schema["className"] == "AMemorixVectorPoolsConfig"
    assert "relation_intent" in vector_pools_schema["nested"]


def test_vector_pools_default_mode_is_dual_and_schema_matches() -> None:
    config = AMemorixRetrievalConfig()
    schema = ConfigSchemaGenerator.generate_schema(AMemorixRetrievalConfig)
    vector_pools_schema = schema["nested"]["vector_pools"]
    mode_field = next(field for field in vector_pools_schema["fields"] if field["name"] == "mode")
    schema_path = Path("src/A_memorix/config_schema.json")
    persisted_schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert config.vector_pools.mode == "dual"
    assert mode_field["default"] == "dual"
    assert persisted_schema["sections"]["retrieval.vector_pools"]["fields"]["mode"]["default"] == "dual"


def test_explicit_single_vector_pool_mode_is_preserved() -> None:
    attribute_data = AttributeData()
    config = AMemorixRetrievalConfig.from_dict(attribute_data, {"vector_pools": {"mode": "single"}})

    assert config.vector_pools.mode == "single"
