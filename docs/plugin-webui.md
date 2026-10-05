# 插件声明式 WebUI 扩展（协议版本 1）

插件在自身目录新增 `webui.json`，重载插件后生效。不需要安装 Node、打包前端或修改主程序。Runner 在工作线程读取声明，Host 校验并注册；卸载插件时页面和入口同步下线，浏览器导航最多 30 秒更新，也可以手动刷新。

在本仓库试用时，先构建 `dashboard`，启动主程序前设置环境变量 `MAIBOT_WEBUI_USE_LOCAL_DASHBOARD=1`，让 WebUI 使用 `dashboard/dist`；默认安装的 `maibot-dashboard` 包不会因为源码修改自动更新。后端路由首次接入需要重启主程序，之后修改插件声明只需重载插件。前端开发服务固定使用 7999 端口。

## 导航和权限

- `placement: "sidebar"`：进入麦麦工作区的“插件扩展”分组。
- `placement: "workspace"`：进入插件自己的顶部工作区；该工作区名称由 `workspace_title` 指定，侧边栏展示同一插件所有 workspace 页面，第一个是默认页面。
- 页面路径由宿主生成：`/extensions/{plugin_id}/{page_id}`。插件不能覆盖内置入口或提供任意路由。
- 内置图标只接受 `puzzle`、`chart`、`settings`、`database`、`list`。
- 顶部直接显示一个插件工作区，其他工作区放在“更多”；当前插件工作区保持可见。
- 用户可以在“插件管理”页面最下方的“管理插件页面”区域调整插件入口顺序或隐藏整个插件的入口。偏好保存在当前浏览器，只控制展示，不是后端授权。

页面只调用自身 `queries` / `actions` 中声明的 API。API 必须属于当前插件、以 SDK `@API` 静态注册、版本精确匹配、处于启用状态。`public=True` 不会自动开放给 WebUI，也不要求将页面 API 公开给其他插件。动态 API、跨插件调用、任意请求地址、HTML、JS、CSS、事件表达式均不支持。

这套协议限制 WebUI 扩展通道，不是 Python 插件进程的文件或网络沙箱。插件仍需对自己的业务行为负责；`queries` 应保持只读，宿主无法通过声明判断 Python 方法是否有副作用。

## 示例：顶部统计页和侧边管理页

下面是完整 `webui.json`。`summary` API 返回 `{"count": 123}`，`save_limit` API 接收 `limit: int`。

```json
{
  "schema_version": 1,
  "workspace_title": "消息统计",
  "pages": [
    {
      "id": "overview",
      "title": "统计概览",
      "placement": "workspace",
      "icon": "chart",
      "queries": { "summary": { "api": "summary", "version": "1" } },
      "content": [
        { "type": "stat", "label": "今日消息", "value": { "source": "summary", "field": "count" } }
      ]
    },
    {
      "id": "settings",
      "title": "统计设置",
      "placement": "sidebar",
      "actions": {
        "save": {
          "api": "save_limit",
          "version": "1",
          "parameters": {
            "limit": { "type": "integer", "required": true, "minimum": 1, "maximum": 1000 }
          },
          "confirmation": "确认修改统计上限？"
        }
      },
      "content": [
        {
          "type": "card",
          "label": "统计范围",
          "children": [
            { "type": "input", "name": "limit", "label": "上限", "value": 100 },
            { "type": "button", "label": "保存", "action": "save" }
          ]
        }
      ]
    }
  ]
}
```

在现有 `MaiBotPlugin` 子类上添加 API 方法即可（仍需实现 SDK 必需生命周期方法）：

```python
from maibot_sdk.components import API

# 以下方法放在你的插件类内部。
@API("summary", version="1")
async def summary(self):
    return {"count": self.today_count}

@API("save_limit", version="1")
async def save_limit(self, limit: int):
    self.limit = limit
    return {"saved": True}
```

## 组件

所有颜色、间距、字体、暗色模式和 dashboard 风格由宿主控制。组件没有 `className`、`style` 或 HTML 插槽。

| 类型 | 属性 | 说明 |
| --- | --- | --- |
| `stack` | `label`、`children` | 纵向排列 |
| `grid` | `label`、`columns`、`children` | 1–4 列，移动端自动单列 |
| `card` | `label`、`children` | 宿主卡片 |
| `tabs` | `children` | 每个子节点必须有 `label` |
| `text` | `label`、`value` | 纯文本，HTML 不执行 |
| `stat` | `label`、`value` | 统计卡片 |
| `table` | `label`、`value`、`columns` | `columns` 是 `[{"field":"name","label":"名称"}]`；绑定对象数组，每页 50 行 |
| `chart` | `label`、`value`、`chart_type`、`x`、`y` | `line` 或 `bar`；绑定对象数组，x 为字符串或数字，y 为数字，最多 2000 行 |
| `input` | `name`、`label`、`value` | 数字默认值对应数字输入框，其余为文本 |
| `date` | `name`、`label`、`value` | 字符串日期输入 |
| `select` | `name`、`label`、`value`、`options` | options 为 `[{"label":"一周","value":"week"}]`，value 必须为非空字符串 |
| `switch` | `name`、`label`、`value` | value 必须为布尔值 |
| `button` | `label`、`action`、`variant` | variant 为 `primary`、`danger` 或 `muted`；danger 操作必须声明 confirmation |

`value` 可为标量，展示组件也支持 `{"source":"查询别名","field":"totals.count"}`；空 field 表示查询的完整返回值，表格和图表必须使用数据引用。不支持计算表达式、动态脚本和任意链接。

输入字段的 `name` 在页面中必须唯一。API 参数按绑定中的 `parameters` 从当前表单字段取值；可选且未填写（null）的字段不发送。参数类型是 `string`、`integer`、`number`、`boolean`，可指定 `required`、`max_length`（最多 4000）、`minimum`、`maximum`、`choices`。参数不进行隐式类型转换，未知参数被拒绝；参数名不能使用内部双下划线名称。

页面打开和刷新时依次执行查询；操作成功后再次刷新查询，更新数据。查询失败、API 下线、结果不符合组件要求时显示错误，不使用伪造数据。写操作不会自动重试。需要确认的操作使用宿主确认弹窗，网关也要求确认标记；确认只是防误触机制，不是独立的授权或业务校验。

## 限制与生命周期

- 文件最多 128 KiB，Host 注册载荷最多 512 KiB；不接受越界路径或符号链接文件。
- 每插件最多 20 个页面，每页最多 200 个组件、8 层嵌套、10 个查询和 20 个操作。
- 每个绑定最多 30 个标量参数；每插件最多两个进行中的 WebUI 请求。
- API 调用超时为 10 秒，响应最多 512 KiB、12 层嵌套和 20000 个元素。
- 超时不代表插件写操作没有生效；核实结果后再尝试，插件可在自己的 API 中实现业务幂等。
- 无效声明或引用未注册 API 会使插件本次注册失败，并在日志中说明原因。现有无 `webui.json` 的插件不受影响。

修改声明后重载插件。`queries` 和 `actions` 内的 API 名是本插件 API 的短名，版本默认 `"1"`，字段、引用和类型错误会直接拒绝。协议 JSON Schema 可由 `WebUIExtension.model_json_schema()` 生成。
