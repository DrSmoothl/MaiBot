# 插件 WebUI 扩展（协议 v2）

插件在自身目录提供 `webui.json`，通过已有静态 `@API` 提供数据和操作。同一插件可以混用两种页面：

| 模式 | 适用范围 | 插件负责 | 宿主负责 |
| --- | --- | --- | --- |
| `declarative` 预置组件 | 概览、简单设置、表格单行操作 | 组件声明、数据、业务 API | 布局样式、表单、查询刷新、确认和详情 |
| `custom` HTML | 图库、批量编辑、复杂状态和任意布局 | 单文件 HTML/CSS/JS、交互和刷新 | 隔离容器、公共 API 桥接、确认、图片接收 |

预置模式不发展为前端框架。需要循环、多选、拖拽、复杂弹窗或业务专属布局时使用 HTML；宿主不识别插件 ID、业务字段或插件私有类名。两种模式共用身份、API 绑定、参数校验、调用限流和错误协议。

## 安装、注册与导航

声明在重载插件后生效；Runner 在线程中读取文件，Host 独立校验并保存快照。卸载插件后接口立即下线，浏览器入口随注册表更新。浏览器导航定期更新，也可手动刷新。

开发时同步部署主程序与 dashboard。在本仓库构建 `dashboard` 后，启动前设置 `MAIBOT_WEBUI_USE_LOCAL_DASHBOARD=1` 使用 `dashboard/dist`；源码变化不会自动更新默认安装的 `maibot-dashboard` 包。前端开发服务固定使用 7999 端口。

- `schema_version: 2`。v1 声明明确拒绝，不自动转换已移除的组件或字段。
- `required_capabilities` 可声明 `declarative_basic`、`custom_pages`、`image_upload`、`structured_arguments`。后三项分别在使用 HTML、图片上传、数组/对象参数时必填。注册表返回服务端能力，dashboard 也核对自身支持；单个扩展不兼容不会阻断其他扩展。
- `placement: "sidebar"` 放入麦麦工作区的插件扩展分组；`"workspace"` 放入插件自己的顶部工作区，此时必须提供 `workspace_title`。同插件的 workspace 页面在其侧边栏显示。
- 图标仅接受 `puzzle`、`chart`、`settings`、`database`、`list`。宿主生成 `/extensions/{plugin_id}/{page_id}`，插件不能覆盖内置路由。
- 用户可在插件管理页隐藏、排序入口；浏览器展示偏好不代表后端授权。
- 配置无效时仍可通过宿主配置页修正配置，但不注册插件业务页面，也不启动业务组件。

每插件最多 20 页；声明文件最多 128 KiB。未知字段、无效引用、类型错误直接拒绝。完整 JSON Schema 可通过 `WebUIExtension.model_json_schema()` 获取。

## 公共 API 协议

每页 `queries` / `actions` 都是「本页别名 → 本插件静态 API」的映射。API 必须属于当前插件、精确匹配版本且处于启用状态；不允许动态 API、跨插件调用和任意 URL。`public=True` 不会自动对 WebUI 开放，页面 API 也不必向其他插件公开。

```json
{
  "api": "save_limit",
  "version": "1",
  "parameters": {
    "limit": {"type": "integer", "required": true, "minimum": 1, "maximum": 1000}
  },
  "confirmation": "确认保存？"
}
```

- `version` 默认 `"1"`；参数默认可选，缺失必填参数或出现未知参数均报错。参数不进行类型转换，`null` 不等于未传。
- 标量类型为 `string`、`integer`、`number`、`boolean`；可声明 `max_length`（默认 4000，最多 65536）、`minimum`、`maximum`、`choices`。
- HTML 页还支持 `array`（必填 `items`，`max_items` 默认/最多 1000）和 `object`（`properties` 定义固定属性及各自的 `required`）。对象拒绝未知属性，不开放任意对象透传。
- 每个绑定最多 30 个参数。请求参数是有限 JSON，总量最多 64 KiB、嵌套深度最多 12、元素总数最多 20000；不支持非有限数字、文件对象或类实例。所有大小限制按 UTF-8 计算。
- API 返回值使用同样的 JSON 规则，总量最多 512 KiB。HTTP 成功响应为 `{"success":true,"result":插件返回值}`；浏览器调用函数直接返回 `result`。
- **宿主不解释业务返回值。** `result.success`、`message`、`navigate_page` 等都是普通数据，不触发宿主报错、提示或跳转。需要表示调用失败时让 API 抛出异常；HTML 页可自行处理业务结果。预置页不提供业务结果解释器。
- `confirmation` 只用于 action，由宿主显示确认，后端要求 `confirmed` 标记。它用于防误触，不是独立权限。query 不接受确认声明，插件必须保证其只读。
- 每插件最多两个进行中的 WebUI API 调用，单次预算 10 秒。长任务应启动后台工作后返回任务 ID，再由 query 查询。宿主不自动重试写操作；取消/超时不代表已提交的操作回滚。

结构化参数示例：

```json
{
  "type": "object",
  "required": true,
  "properties": {
    "ids": {"type": "array", "required": true, "max_items": 100, "items": {"type": "integer"}},
    "enabled": {"type": "boolean", "required": true}
  }
}
```

## 预置组件：简单页面

完整 `webui.json` 示例；`summary` 返回 `{"count":123}`，`save_limit` 接收整数 `limit`：

```json
{
  "schema_version": 2,
  "workspace_title": "统计",
  "pages": [{
    "id": "overview", "title": "统计概览", "placement": "workspace", "mode": "declarative",
    "queries": {"summary": {"api": "summary"}},
    "actions": {
      "save": {
        "api": "save_limit",
        "parameters": {"limit": {"type": "integer", "required": true, "minimum": 1, "maximum": 1000}},
        "confirmation": "确认保存统计上限？"
      }
    },
    "content": [
      {"type": "stat", "label": "今日消息", "value": {"source": "summary", "field": "count"}},
      {"type": "input", "name": "limit", "label": "上限", "value": 100},
      {"type": "button", "label": "保存", "action": "save"}
    ]
  }]
}
```

预置页只接受标量参数。输入 `name` 对应 API 参数名，未填写的 `null` 不发送。展示组件的数据引用格式为 `{"source":"查询别名","field":"totals.count"}`，空 `field` 读取整个结果；不支持表达式或脚本。

| 类型 | 关键字段 | 行为 |
| --- | --- | --- |
| `stack`、`card` | `children`、可选 `label` | 宿主纵向布局/卡片 |
| `grid` | `children`、`columns` | 1–4 列，移动端单列 |
| `tabs` | `children` | 每个子节点必填 `label` |
| `collapsible` | `label`、`children`、`default_open` | 默认收起的折叠容器 |
| `text` | `value`、`format` | 纯文本；`format: "log"` 为只读滚动日志 |
| `stat` | `label`、`value` | 标量统计值 |
| `progress` | `value` | 0–100 的有限数字或引用 |
| `image` | `value` 引用 | 单张 JPEG/PNG/WebP base64 data URL，最多 24000 字符；`null` 表示无图 |
| `table` | `value` 引用、`columns` | 对象数组；列为 `{field,label}`，每页 50 行 |
| `dialog` | `name`、`label`、`children` | 仅用于表格行详情，必须在 content 顶层 |
| `chart` | `value` 引用、`x`、`y`、`chart_type` | 折线/柱状图；x 是字符串或数字，y 是数字，最多 2000 行 |
| `pagination` | `name`、`value` 引用 | `{page,pages,total}`，page/pages 从 1 开始；修改同名表单参数 |
| `input` | `name`、`value` | 数字默认值为数字输入，其余为文本 |
| `date` | `name`、`value` | 日期字符串输入 |
| `select` | `name`、`value`、`options` | 标准下拉框，选项为 `{label,value}`，value 为非空字符串 |
| `switch` | `name`、`value` | 布尔开关 |
| `button` | `label`、`action`、`variant` | 固定文案；primary/danger/muted，danger 必须绑定确认操作 |

组件只接受自身定义的字段，不接受 `style`、`className`、HTML 插槽。每页最多 200 个组件、8 层深度，每个容器最多 50 个子节点。

页面打开/手动刷新时顺序执行 queries，全部成功后替换数据快照。操作成功后再次刷新。`auto_refresh: true` 在表单变化后查询，并把其他 pagination 输入重置为 1；`poll_interval_seconds` 为 3–60 秒，默认 0 关闭。后台轮询不锁定操作，写操作会取消旧查询；离页停止轮询。错误明确显示，不填入伪造默认数据。

### 表格单行操作

给表格声明 `selection: "record"` 即可选中一行；再声明 `detail: "details"` 会打开名为 details 的顶层 dialog。选中行引用为 `{"scope":"selection","source":"record","field":"id"}`。没有选中行时引用为 null，查询字段缺失则报错。

action 可通过 `arguments` 把已声明的参数绑定到 query/selection 引用，只投影标量。例如：

```json
{
  "api": "remove_record",
  "parameters": {"id": {"type": "integer", "required": true}},
  "arguments": {"id": {"scope": "selection", "source": "record", "field": "id"}},
  "confirmation": "确认删除这条记录？"
}
```

确认时冻结参数。数据快照替换或写操作成功后清除选择并关闭详情，后续刷新失败也不会保留已被操作的旧详情。query 不接受 `arguments`。

所有组件可用 `when`，输入/按钮可用 `disabled_when`；格式为 `{"reference":数据引用,"operator":"truthy|empty|not_empty|equals","expected":标量}`，仅 equals 接受 expected。按钮的 `disabled_reason` 在条件禁用时解释原因。这些条件只读取现有数据，不引入计算表达式。

## HTML：自由布局与脚本

插件包提供 UTF-8 单文件 HTML。原生 HTML/CSS/JS 可直接使用；React/Vue 等项目需预先打包，把脚本、样式和图片内联。安装插件不需要 Node。支持内联脚本和 blob 脚本，不支持 CDN、相对资源文件、eval 或直接联网。

```json
{
  "schema_version": 2,
  "required_capabilities": ["custom_pages"],
  "pages": [{
    "id": "studio", "title": "工作台", "mode": "custom",
    "entry": "webui/index.html", "layout": "document",
    "queries": {"summary": {"api": "summary"}},
    "actions": {
      "save": {
        "api": "save_limit",
        "parameters": {"limit": {"type": "integer", "required": true}},
        "confirmation": "确认保存？"
      }
    }
  }]
}
```

`entry` 是插件内的相对 `.html` 路径，不允许越界、符号链接和 junction。不允许在声明中内联宿主内部的 `document` 字段。每页 HTML 最多 2 MiB，所有 HTML 合计最多 8 MiB，注册元数据另限 512 KiB。

HTML 页不声明 `content`、`auto_refresh`、`poll_interval_seconds` 或 `arguments` 引用，交互与刷新由脚本掌控：

```html
<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><style>body { padding: 24px; }</style></head>
<body class="maibot-page">
  <p id="count"></p>
  <input class="maibot-input" id="limit" type="number" value="100">
  <button class="maibot-button" id="save">保存</button>
  <p id="message" role="status"></p>
  <script type="module">
    const message = document.querySelector('#message');
    async function refresh() {
      const summary = await maibot.query('summary');
      document.querySelector('#count').textContent = `消息数：${summary.count}`;
    }
    await maibot.ready;
    try { await refresh(); } catch (error) { message.textContent = error.message; }
    document.querySelector('#save').onclick = async event => {
      event.target.disabled = true;
      try {
        await maibot.action('save', {limit: Number(document.querySelector('#limit').value)});
        message.textContent = '已保存';
        await refresh();
      } catch (error) { message.textContent = error.message; }
      finally { event.target.disabled = false; }
    };
  </script>
</body>
</html>
```

宿主在插件脚本之前注入 `window.maibot`：

| 接口 | 说明 |
| --- | --- |
| `ready` | 桥接就绪 Promise，返回初始主题/语言上下文 |
| `query(alias, args?)` | 调用本页查询，返回原始 result |
| `action(alias, args?)` | 调用本页操作，声明 confirmation 时先由宿主确认 |
| `upload(alias, file, args?, options?)` | 单张图片；options 支持 signal、onProgress(percent)，返回接收 API 的 result |
| `navigate(pageId, state?)` | 显式导航到同插件已注册页面，不能传 URL 或插件 ID |
| `page` | 就绪后为 `{id, layout, state}` |
| `context` | `{theme, language, dashboardStyle, styles}`；变化时触发 `maibot:context` |

导航 state 是最多 4 KiB 的 JSON 对象，存于 URL 的 `state` 参数，不能放秘密。HTML 页自行解释 `maibot.page.state`；预置页仅接受已声明输入的标量初始值。导航不依赖 API 返回值，也不修改插件全局设置。

`layout: "document"` 为默认值，按正文高度展开，使用外层滚动，高度最多 30000 px；适合自然文档流，避免 100vh 反复撑高自身。`layout: "viewport"` 使用固定视口，插件自行管理内部滚动，适合 SPA、编辑器和复杂工具。可见区域以 `--maibot-viewport-top` / `--maibot-viewport-height` 提供。布局不符合预算会明确停止页面。

“重载页面”仅重新读取插件 HTML：Runner 读文件，Host 校验完整文档集合、内容预算和整个扩展后原子替换快照，再重建当前 iframe。失败保留旧快照并显示错误。不会整页刷新、卸载插件或中断后台任务。修改页面/API 声明仍需重载插件。

### 可选的主题外观

SDK 同步 dark 类、color-scheme、语言、data-dashboard-style，并提供宿主主题变量和公共 CSS；不传递宿主 DOM、React 实例或登录凭证。页面可以自由使用自己的样式，也可选用：

- `maibot-page`、`maibot-card`、`maibot-card-header`、`maibot-muted`、`maibot-inset`。
- `maibot-button`，可加 `maibot-button-primary` / `maibot-button-danger`，选中态用 `aria-pressed="true"`。
- `maibot-input`、`maibot-select`、`maibot-badge`、`maibot-table`、`maibot-number`。
- `--maibot-background`、`--maibot-surface`、`--maibot-foreground`、`--maibot-muted`、`--maibot-soft`、`--maibot-border`、`--maibot-primary`、`--maibot-primary-foreground`、`--maibot-danger`、`--maibot-radius`、`--maibot-font-body`、`--maibot-font-display`、`--maibot-font-mono`。

公共样式只识别 maibot-* 类，组件行为仍由插件实现。主题更新合并至动画帧，内容不变不发送消息。固定宿主字体资产经宿主读取，以 data 字体传入（每字体最多 2 MiB），不开放任意资源代理。

### 图片上传

这是图片接收协议，不是任意文件 API。声明 `image_upload` 能力，并在 HTML 页配置：

```json
{
  "actions": {
    "add": {
      "api": "receive_image",
      "parameters": {
        "upload_id": {"type": "string", "required": true},
        "label": {"type": "string"}
      },
      "confirmation": "确认接收这张图片？"
    }
  },
  "uploads": {"library": {"action": "add", "image_max_edge": 4000}}
}
```

调用 `maibot.upload('library', file, {label: 'sample'}, {signal, onProgress})`。绑定的是上传别名 library，宿主从声明解析其 action。允许确认；参数在确认前复制，文件接收前后均检查 API 状态和参数。

登录后的 multipart POST 使用 `/api/webui/plugins/runtime/webui/{plugin}/{page}/uploads/{alias}`，字段为 file、args(JSON)、confirmed。宿主签发 64 字符 upload_id，不接受客户端伪造。插件声明 `webui.claim_upload` 能力，通过 SDK `ctx.webui.claim_upload(upload_id)` 一次性领取到本插件 `data_dir/uploads`；文件字节不经过 JSON/RPC。

- 仅静态 JPEG/PNG/WebP；接收文件最多 20 MiB、4000 万像素，以实际解码验证，不能指定保存路径。
- 声明 image_max_edge 时，浏览器允许最多 64 MiB / 1 亿像素的源图片并缩放为上传副本；否则源文件最多 20 MiB。客户端处理不能绕过服务端校验，原文件不变。
- 在解析 multipart 前鉴权并验证绑定；从接收、解码、暂存到 API 调用，单插件最多 2 个、全局最多 4 个上传请求，超额立即 429，不建立等待队列。请求预算 120 秒，按实际接收字节限制总量。
- 取消不会释放仍在工作线程中解码/写盘的配额；等工作完成再释放，避免靠反复取消绕过并发限制。
- 未领取图片单插件最多 8 张、全局最多 32 张（对应至多 160 / 640 MiB），超过返回 429。凭证一小时过期，每分钟及暂存时回收；领取后的文件归插件管理。
- SDK 上传支持进度、取消，不负责批量队列、重试策略或业务提示。批处理由 HTML 页实现。

## 安全与故障边界

HTML 以 `sandbox="allow-scripts"` 运行，保留不透明来源，不开放 same-origin、顶层导航、弹窗、表单或下载权限。宿主先注入 CSP，再放插件内容：禁止直接网络请求、网络子资源、嵌套框架、worker 和对象嵌入，仅开放内联脚本/样式及受限的 data/blob 资源。插件无法放宽前置 CSP。

父窗口只接受当前 iframe 的 nonce 握手，再移交专属 MessagePort；桥接绑定本页和本插件，服务端继续校验登录、归属和静态 API。HTML 按需经 JSON 获取，使用 no-store/nosniff；不公开插件静态目录，不把 HTML 当宿主同源资源执行。

每页最多两个桥接请求，突发最多 20 条、持续每秒 2 条，布局消息单独限流；洪泛会停止页面。握手预算 10 秒，确认 45 秒，SDK 查询/操作 60 秒，上传 150 秒。离页、下线、声明变更或“停止页面”移除 iframe、关闭桥接并取消浏览器请求；导航后也不重新授予桥接。请求失败明确报错，不自动降级到旧 API。

这些边界保护宿主来源和接口权限，不是恶意插件的完整隔离环境。iframe 自身导航仍可能发出网络请求，导航后的停止处理不能撤回已发出的请求；任意 JavaScript 的死循环或内存耗尽也可能影响浏览器标签页。Python 插件进程不是操作系统沙箱。不要把 CSP 或停止按钮解释为完整的数据防泄漏/资源隔离保证。

## 从 v1 迁移

必须同时更新主程序、dashboard 和插件声明。已有 v1 会得到明确迁移错误；不做静默转换。

1. 设置 schema_version: 2，区分 declarative/custom；file_upload 改为 image_upload，移除 interactive_controls。使用结构化参数时声明 structured_arguments。
2. 预置页移除 repeat/item 上下文、gallery、upload、checkbox、多选、跨页选中、clear_selection、button.detail、动态 button.value、select.presentation 及旧组件专有字段。保留基础布局、单行详情与标量表单，复杂交互迁到 HTML。
3. HTML 页移除 content、组件式 arguments、auto_refresh、poll_interval_seconds；用自身脚本管理界面。按需要选择 document/viewport 布局。
4. 业务失败不再靠 result.success=false 触发宿主异常；API 抛错或由 HTML 解释结果。navigate_page/navigate_params 不触发跳转，改用显式 maibot.navigate(pageId, state)。
5. 图片上传使用 uploads 别名，业务参数必须真实符合声明，upload_id 留给宿主注入；支持宿主确认。

宿主不为旧插件名称、旧业务结果或失效声明增加专门 API 和兼容分支。
