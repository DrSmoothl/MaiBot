from functools import lru_cache
from typing import Any, Dict, List, get_args, get_origin

from pydantic_core import PydanticUndefined

import inspect

from src.config.config_base import ConfigBase

AMEMORIX_BASIC_FIELDS: Dict[str, set[str]] = {
    "AMemorixConfig": {
        "episode",
        "embedding",
        "integration",
        "memory",
        "person_profile",
        "plugin",
        "shared_memory_groups",
    },
    "AMemorixIntegrationConfig": {
        "enable_memory_query_tool",
        "heuristic_memory_cross_chat_enabled",
        "heuristic_memory_recall_enabled",
        "memory_query_default_limit",
        "enable_person_profile_query_tool",
    },
    "AMemorixEpisodeConfig": {"enabled"},
    "AMemorixPluginConfig": {"enabled"},
    "AMemorixEmbeddingConfig": {
        "model_name",
        "dimension",
        "dimension_request_mode",
        "batch_size",
        "max_concurrent",
        "enable_cache",
        "quantization_type",
    },
    "AMemorixMemoryEvolutionConfig": {"enabled"},
    "AMemorixPersonProfileConfig": {"enabled"},
}


class ConfigSchemaGenerator:
    @staticmethod
    @lru_cache(maxsize=None)
    def _get_class_field_docs(config_class: type[ConfigBase]) -> Dict[str, str]:
        return config_class.get_class_field_docs()

    @staticmethod
    def _build_label(label: str) -> Dict[str, str]:
        return {"zh_CN": label}

    @classmethod
    def generate_schema(cls, config_class: type[ConfigBase], include_nested: bool = True) -> Dict[str, Any]:
        return cls.generate_config_schema(config_class, include_nested=include_nested)

    @classmethod
    def generate_config_schema(cls, config_class: type[ConfigBase], include_nested: bool = True) -> Dict[str, Any]:
        fields: List[Dict[str, Any]] = []
        nested: Dict[str, Dict[str, Any]] = {}
        field_docs = cls._get_class_field_docs(config_class)

        for field_name, field_info in config_class.model_fields.items():
            if field_name in {"field_docs", "_validate_any", "suppress_any_warning"}:
                continue

            field_schema = cls._build_field_schema(config_class, field_name, field_info.annotation, field_info, field_docs)
            fields.append(field_schema)

            if include_nested:
                nested_schema = cls._build_nested_schema(field_info.annotation)
                if nested_schema is not None:
                    nested[field_name] = nested_schema

        schema: Dict[str, Any] = {
            "className": config_class.__name__,
            "classDoc": (config_class.__doc__ or "").strip(),
            "fields": fields,
            "nested": nested,
        }

        # 将 UI 分组元数据写入 schema
        ui_parent = getattr(config_class, "__ui_parent__", "")
        ui_label = getattr(config_class, "__ui_label__", "")
        ui_advanced = bool(getattr(config_class, "__ui_advanced__", False))
        ui_order = int(getattr(config_class, "__ui_order__", 0))
        if ui_parent:
            schema["uiParent"] = ui_parent
        if ui_label:
            schema["uiLabel"] = ui_label
        schema["uiAdvanced"] = ui_advanced
        if ui_order:
            schema["uiOrder"] = ui_order

        return schema

    @classmethod
    def _build_nested_schema(cls, annotation: Any) -> Dict[str, Any] | None:
        origin = get_origin(annotation)
        args = get_args(annotation)

        if inspect.isclass(annotation) and issubclass(annotation, ConfigBase):
            return cls.generate_config_schema(annotation)

        if origin in {list, set, tuple} and args:
            first = args[0]
            if inspect.isclass(first) and issubclass(first, ConfigBase):
                return cls.generate_config_schema(first)

        return None

    @classmethod
    def _build_field_schema(
        cls,
        config_class: type[ConfigBase],
        field_name: str,
        annotation: Any,
        field_info: Any,
        field_docs: Dict[str, str],
    ) -> Dict[str, Any]:
        field_type = cls._map_field_type(annotation)
        raw_description = field_docs.get(field_name, field_info.description or "")
        # `_wrap_` 标记在配置类 docstring 中表示该说明应作为块级注释（独立成行）
        # 在前端展示时把它转为换行符，使描述以新行起始或在中间换行
        description = raw_description.replace("_wrap_", "\n").strip("\n")
        schema: Dict[str, Any] = {
            "name": field_name,
            "type": field_type,
            "label": cls._build_label(field_name),
            "description": description,
            "required": field_info.is_required(),
        }

        if field_info.default is not PydanticUndefined:
            schema["default"] = field_info.default

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin in {list, set} and args:
            schema["items"] = {"type": cls._map_field_type(args[0])}

        if options := cls._extract_options(annotation):
            schema["options"] = options

        # Task 1c: Merge json_schema_extra (x-widget, x-icon, step, etc.)
        if hasattr(field_info, "json_schema_extra") and field_info.json_schema_extra:
            schema.update(field_info.json_schema_extra)

        # Task 1d: Map Pydantic constraints to minValue/maxValue (frontend naming convention)
        if hasattr(field_info, "metadata") and field_info.metadata:
            for constraint in field_info.metadata:
                if hasattr(constraint, "ge"):
                    schema["minValue"] = constraint.ge
                if hasattr(constraint, "le"):
                    schema["maxValue"] = constraint.le

        cls._apply_a_memorix_visibility_policy(config_class, field_name, schema)

        return schema

    @staticmethod
    def _apply_a_memorix_visibility_policy(
        config_class: type[ConfigBase],
        field_name: str,
        schema: Dict[str, Any],
    ) -> None:
        class_name = config_class.__name__
        if not class_name.startswith("AMemorix"):
            return

        basic_fields = AMEMORIX_BASIC_FIELDS.get(class_name, set())
        if field_name not in basic_fields:
            schema["advanced"] = True

    @staticmethod
    def _extract_options(annotation: Any) -> List[str] | None:
        origin = get_origin(annotation)
        if origin is None:
            return None
        if str(origin) != "typing.Literal":
            return None

        args = get_args(annotation)
        options = [str(item) for item in args]
        return options or None

    @classmethod
    def _map_field_type(cls, annotation: Any) -> str:
        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin in {list, set, tuple}:
            return "array"
        if inspect.isclass(annotation) and issubclass(annotation, ConfigBase):
            return "object"
        if annotation is bool:
            return "boolean"
        if annotation is int:
            return "integer"
        if annotation is float:
            return "number"
        if annotation is str:
            return "string"

        if origin in {list, set, tuple} and args:
            return "array"

        if origin in {dict}:
            return "object"

        if origin is not None and str(origin) == "typing.Literal":
            return "select"

        return "string"
