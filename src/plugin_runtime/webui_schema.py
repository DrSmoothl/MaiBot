"""插件 WebUI 声明协议：只接受宿主支持的组件和所属插件的 API 绑定。"""

from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

import json
import math

from pydantic import BaseModel, ConfigDict, Field, model_validator

Identifier = Annotated[str, Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,63}$")]
Text = Annotated[str, Field(max_length=4000)]
Label = Annotated[str, Field(min_length=1, max_length=80)]
Scalar = Union[str, int, float, bool, None]
MAX_DECLARATION_BYTES = 131072


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class DataReference(StrictModel):
    source: Identifier
    field: str = Field(default="", pattern=r"^(?:[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*)?$", max_length=200)


class Parameter(StrictModel):
    type: Literal["string", "integer", "number", "boolean"]
    required: bool = False
    max_length: int = Field(default=4000, ge=1, le=4000)
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    choices: List[Scalar] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def validate_constraints(self) -> "Parameter":
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("参数 minimum 不能大于 maximum")
        if any(not self.accepts(value) for value in self.choices):
            raise ValueError("参数 choices 必须符合声明类型和范围")
        return self

    def accepts(self, value: Any) -> bool:
        """严格校验参数，不把字符串、布尔值隐式转换成数字。"""
        expected = {
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "number": type(value) in (int, float),
            "boolean": type(value) is bool,
        }[self.type]
        if not expected or (self.choices and value not in self.choices):
            return False
        if isinstance(value, str):
            return len(value) <= self.max_length
        if type(value) in (int, float):
            return (
                math.isfinite(value)
                and (self.minimum is None or value >= self.minimum)
                and (self.maximum is None or value <= self.maximum)
            )
        return True


class APIBinding(StrictModel):
    api: str = Field(pattern=r"^[a-zA-Z][a-zA-Z0-9_.-]{0,127}$")
    version: str = Field(default="1", min_length=1, max_length=40)
    parameters: Dict[Identifier, Parameter] = Field(default_factory=dict, max_length=30)
    confirmation: Optional[Label] = None

    def validate_args(self, args: Dict[str, Any]) -> None:
        for name in args:
            if name not in self.parameters:
                raise ValueError(f"未声明的参数: {name}")
        for name, parameter in self.parameters.items():
            if name not in args:
                if parameter.required:
                    raise ValueError(f"缺少参数: {name}")
            elif not parameter.accepts(args[name]):
                raise ValueError(f"参数不符合声明: {name}")


class Column(StrictModel):
    field: Identifier
    label: Label


class Option(StrictModel):
    label: Label
    value: str = Field(min_length=1, max_length=200)


class WebUINode(StrictModel):
    type: Literal[
        "stack", "grid", "card", "tabs", "text", "stat", "table", "chart", "input", "select", "switch", "date", "button"
    ]
    label: Optional[Label] = None
    value: Union[Text, int, float, bool, DataReference, None] = None
    children: List["WebUINode"] = Field(default_factory=list, max_length=50)
    columns: Union[int, List[Column], None] = None
    name: Optional[Identifier] = None
    options: List[Option] = Field(default_factory=list, max_length=100)
    action: Optional[Identifier] = None
    variant: Literal["primary", "danger", "muted"] = "primary"
    chart_type: Literal["line", "bar"] = "line"
    x: Optional[Identifier] = None
    y: Optional[Identifier] = None

    @model_validator(mode="after")
    def validate_component(self) -> "WebUINode":
        common = {"type", "label"}
        allowed = {
            "stack": {"children"},
            "grid": {"children", "columns"},
            "card": {"children"},
            "tabs": {"children"},
            "text": {"value"},
            "stat": {"value"},
            "table": {"value", "columns"},
            "chart": {"value", "chart_type", "x", "y"},
            "input": {"name", "value"},
            "select": {"name", "value", "options"},
            "switch": {"name", "value"},
            "date": {"name", "value"},
            "button": {"action", "variant"},
        }[self.type]
        # RPC 的 model_dump 会带上默认字段；只允许非适用字段保持协议默认值。
        for name in self.model_fields_set - common - allowed:
            field = type(self).model_fields[name]
            default = field.default_factory() if field.default_factory is not None else field.default
            if self.__dict__[name] != default:
                raise ValueError(f"{self.type} 包含不支持的属性: {name}")
        if self.type == "grid" and (type(self.columns) is not int or not 1 <= self.columns <= 4):
            raise ValueError("grid.columns 必须为 1 到 4")
        if self.type == "table" and (not isinstance(self.columns, list) or not 1 <= len(self.columns) <= 20):
            raise ValueError("table 必须声明 1 到 20 列")
        if self.type in {"table", "chart"} and not isinstance(self.value, DataReference):
            raise ValueError("表格和图表必须绑定查询结果")
        if self.type == "chart" and (self.x is None or self.y is None):
            raise ValueError("chart 必须声明 x 和 y 字段")
        if self.type in {"input", "select", "switch", "date"}:
            if self.name is None or isinstance(self.value, DataReference):
                raise ValueError("输入组件必须声明 name，默认值必须为标量")
            if self.type == "switch" and type(self.value) is not bool:
                raise ValueError("switch 默认值必须为布尔值")
            if self.type in {"date", "select"} and self.value is not None and not isinstance(self.value, str):
                raise ValueError("日期和选择组件默认值必须为字符串")
            if self.type == "select" and (
                not self.options or self.value not in [None, *[o.value for o in self.options]]
            ):
                raise ValueError("select 默认值必须属于 options")
        if self.type == "button" and (self.action is None or self.label is None):
            raise ValueError("button 必须声明 action 和 label")
        if self.type == "tabs" and (not self.children or any(child.label is None for child in self.children)):
            raise ValueError("tabs 子节点必须有 label")
        return self


class WebUIPage(StrictModel):
    id: Identifier
    title: Label
    description: Text = ""
    placement: Literal["sidebar", "workspace"] = "sidebar"
    icon: Literal["puzzle", "chart", "settings", "database", "list"] = "puzzle"
    queries: Dict[Identifier, APIBinding] = Field(default_factory=dict, max_length=10)
    actions: Dict[Identifier, APIBinding] = Field(default_factory=dict, max_length=20)
    content: List[WebUINode] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def validate_references(self) -> "WebUIPage":
        inputs = set()
        pending = [(node, 1) for node in self.content]
        count = 0
        while pending:
            node, depth = pending.pop()
            count += 1
            if depth > 8 or count > 200:
                raise ValueError("页面超过 8 层或 200 个组件")
            if isinstance(node.value, DataReference) and node.value.source not in self.queries:
                raise ValueError(f"未声明的查询: {node.value.source}")
            if node.action is not None and node.action not in self.actions:
                raise ValueError(f"未声明的操作: {node.action}")
            if node.type == "button" and node.variant == "danger" and node.action is not None:
                if self.actions[node.action].confirmation is None:
                    raise ValueError("danger 操作必须声明 confirmation")
            if node.name is not None:
                if node.name in inputs:
                    raise ValueError(f"重复输入字段: {node.name}")
                inputs.add(node.name)
            pending.extend((child, depth + 1) for child in node.children)
        return self


class WebUIExtension(StrictModel):
    schema_version: Literal[1] = 1
    workspace_title: Optional[Label] = None
    pages: List[WebUIPage] = Field(min_length=1, max_length=20)

    @model_validator(mode="before")
    @classmethod
    def validate_size(cls, value: Any) -> Any:
        if isinstance(value, dict):
            # Host 同样校验 Runner 上报的总量，不能依赖插件目录读取时的限制。
            if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")) > 524288:
                raise ValueError("WebUI 注册声明超过 512 KiB")
        return value

    @model_validator(mode="after")
    def validate_pages(self) -> "WebUIExtension":
        if len({page.id for page in self.pages}) != len(self.pages):
            raise ValueError("页面 ID 重复")
        if any(page.placement == "workspace" for page in self.pages) and self.workspace_title is None:
            raise ValueError("顶部工作区必须声明 workspace_title")
        return self


def load_webui_extension(plugin_dir: str) -> Optional[WebUIExtension]:
    """在 Runner 的工作线程读取声明；无文件的插件不注册 WebUI 扩展。"""
    root = Path(plugin_dir).resolve()
    path = root / "webui.json"
    if not path.exists():
        return None
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise ValueError("webui.json 必须位于插件目录内，且不能是符号链接")
    with path.open("rb") as stream:
        raw = stream.read(MAX_DECLARATION_BYTES + 1)
    if len(raw) > MAX_DECLARATION_BYTES:
        raise ValueError("webui.json 超过 128 KiB")
    return WebUIExtension.model_validate(json.loads(raw))
