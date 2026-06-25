from src.config.official_configs import ChatConfig, MessageReceiveConfig
from src.config.config import Config
from src.config.config_base import ConfigBase, Field
from src.webui.config_schema import ConfigSchemaGenerator


def _get_reply_timing_schema(config_schema):
    return config_schema["nested"]["reply_timing"]


def _get_talk_value_field(config_schema):
    reply_timing_schema = _get_reply_timing_schema(config_schema)
    return next(f for f in reply_timing_schema["fields"] if f["name"] == "talk_value")


def test_field_docs_in_schema():
    """Test that field descriptions are correctly extracted from field_docs (docstrings)."""
    schema = ConfigSchemaGenerator.generate_schema(ChatConfig)
    talk_value = _get_talk_value_field(schema)

    # Verify description field exists
    assert "description" in talk_value
    # Verify description contains expected Chinese text from the docstring
    assert "频率" in talk_value["description"]


def test_json_schema_extra_merged():
    """Test that json_schema_extra fields are correctly merged into output."""
    schema = ConfigSchemaGenerator.generate_schema(ChatConfig)
    talk_value = _get_talk_value_field(schema)

    # Verify UI metadata fields from json_schema_extra exist
    assert talk_value.get("x-widget") == "slider"
    assert talk_value.get("x-icon") == "message-circle"
    assert talk_value.get("step") == 0.001


def test_pydantic_constraints_mapped():
    """Test that Pydantic constraints (ge/le) are correctly mapped to minValue/maxValue."""
    schema = ConfigSchemaGenerator.generate_schema(ChatConfig)
    talk_value = _get_talk_value_field(schema)

    # Verify constraints are mapped to frontend naming convention
    assert "minValue" in talk_value
    assert "maxValue" in talk_value
    assert talk_value["minValue"] == 0  # From ge=0
    assert talk_value["maxValue"] == 1  # From le=1


def test_nested_model_schema():
    """Test that nested models (ConfigBase fields) are correctly handled."""
    schema = ConfigSchemaGenerator.generate_schema(Config)

    # Verify nested structure exists
    assert "nested" in schema
    assert "chat" in schema["nested"]

    # Verify nested chat schema is complete
    chat_schema = schema["nested"]["chat"]
    assert chat_schema["className"] == "ChatConfig"
    assert "fields" in chat_schema

    # Verify nested schema fields include description and metadata
    talk_value = _get_talk_value_field(chat_schema)
    assert "description" in talk_value
    assert talk_value.get("x-widget") == "slider"
    assert talk_value.get("minValue") == 0


def test_config_subtab_metadata_is_exposed():
    """配置子 Tab 元数据应由 schema 提供，而不是由 WebUI 硬编码。"""
    schema = ConfigSchemaGenerator.generate_schema(Config)

    chat_schema = schema["nested"]["chat"]
    expression_schema = schema["nested"]["expression"]
    jargon_schema = schema["nested"]["jargon"]

    assert chat_schema.get("uiUseSubTabs") is True
    assert chat_schema.get("uiRootSubLabel") == "基础设置"
    assert chat_schema["nested"]["reply_timing"].get("uiLabel") == "什么时候发言"
    assert chat_schema["nested"]["reply_style"].get("uiLabel") == "如何发言"

    assert expression_schema.get("uiUseSubTabs") is True
    assert expression_schema.get("uiSubLabel") == "表达"
    assert jargon_schema.get("uiSubLabel") == "黑话"


def test_field_without_extra_metadata():
    """Test that fields without json_schema_extra still generate valid schema."""
    class PlainExampleConfig(ConfigBase):
        plain_field: str = Field(default="visible")
        """普通字段"""

    schema = ConfigSchemaGenerator.generate_schema(PlainExampleConfig)
    plain_field = next(f for f in schema["fields"] if f["name"] == "plain_field")

    # Verify basic fields are generated
    assert "name" in plain_field
    assert plain_field["name"] == "plain_field"
    assert "type" in plain_field
    assert plain_field["type"] == "string"
    assert "label" in plain_field
    assert "required" in plain_field

    # Verify no x-widget or x-icon from json_schema_extra (since field has none)
    # These fields should only be present if explicitly defined in json_schema_extra
    assert not plain_field.get("x-widget")
    assert not plain_field.get("x-icon")


def test_all_top_level_sections_have_ui_metadata():
    """所有顶层配置节都必须声明 uiParent 或独立 Tab 的标签与图标。"""
    schema = ConfigSchemaGenerator.generate_schema(Config)

    for section_name, section_schema in schema["nested"].items():
        has_parent = bool(section_schema.get("uiParent"))
        has_host_meta = bool(section_schema.get("uiLabel"))
        assert has_parent or has_host_meta, f"{section_name} 缺少 UI 元数据"


def test_tab_advanced_visibility_comes_from_config_metadata():
    """配置页 Tab 是否默认收起应由配置类元信息决定。"""
    schema = ConfigSchemaGenerator.generate_schema(Config)

    assert schema["nested"]["bot"].get("uiAdvanced") is False
    assert schema["nested"]["experimental"].get("uiAdvanced") is True
    assert schema["nested"]["message_receive"].get("uiAdvanced") is True


def test_maisaka_is_host_tab_and_mcp_is_attached_to_it():
    """MaiSaka 应作为独立 Tab，MCP 作为其子配置挂载。"""
    schema = ConfigSchemaGenerator.generate_schema(Config)

    maisaka_schema = schema["nested"]["maisaka"]
    mcp_schema = schema["nested"]["mcp"]

    assert maisaka_schema.get("uiParent") is None
    assert maisaka_schema.get("uiLabel") == "MaiSaka"
    assert mcp_schema.get("uiParent") == "maisaka"


def test_memory_query_config_fields_are_exposed():
    """query_memory 开关和默认条数应出现在记忆配置 schema 中。"""
    schema = ConfigSchemaGenerator.generate_schema(Config)
    memory_schema = schema["nested"]["memory"]

    assert memory_schema.get("uiParent") == "emoji"

    enable_field = next(field for field in memory_schema["fields"] if field["name"] == "enable_memory_query_tool")
    limit_field = next(field for field in memory_schema["fields"] if field["name"] == "memory_query_default_limit")

    assert enable_field["type"] == "boolean"
    assert enable_field.get("x-widget") == "switch"
    assert enable_field.get("x-icon") == "database"

    assert limit_field["type"] == "integer"
    assert limit_field.get("x-widget") == "input"
    assert limit_field.get("x-icon") == "hash"
    assert limit_field.get("minValue") == 1
    assert limit_field.get("maxValue") == 20


def test_set_field_is_mapped_as_array():
    """set[str] 应映射为前端可识别的 array。"""
    schema = ConfigSchemaGenerator.generate_schema(MessageReceiveConfig)
    ban_words = next(field for field in schema["fields"] if field["name"] == "ban_words")

    assert ban_words["type"] == "array"
    assert ban_words["items"]["type"] == "string"


def test_advanced_fields_are_hidden_from_webui_schema():
    """advanced=True 的字段不应出现在 WebUI 配置 schema 中，未声明时默认展示。"""

    class AdvancedExampleConfig(ConfigBase):
        normal_field: str = Field(default="visible")
        """普通字段"""

        advanced_field: str = Field(default="hidden", json_schema_extra={"advanced": True})
        """高级字段"""

    schema = ConfigSchemaGenerator.generate_schema(AdvancedExampleConfig)
    field_names = {field["name"] for field in schema["fields"]}

    assert "normal_field" in field_names
    assert "advanced_field" not in field_names
