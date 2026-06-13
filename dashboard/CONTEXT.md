# MaiBot WebUI（dashboard）

MaiBot 的管理界面：React + TanStack Router 单页应用，可运行在浏览器（同源部署 / Vite 代理）或 Electron 壳内。本文档记录 WebUI 上下文中的领域词汇，供架构评审与重构时统一用语。

## Language

### 请求层

**请求客户端（ApiClient）**：
由 `createApiClient` 实例化的 HTTP 请求深模块，收编 base URL 解析、认证、响应解析、错误格式化与诊断。
_Avoid_: fetcher、fetch 封装、axios 实例

**主后端**：
MaiBot 本体暴露的 HTTP API（`/api/webui/**`），使用 HttpOnly Cookie 认证，401 时跳转登录页。对应实例 `backendApi`。
_Avoid_: 服务器、API 服务

**统计服务**：
部署在 Cloudflare Workers 的外部服务，承载问卷提交与插件统计（点赞/评分/下载量），无 Cookie 认证。对应实例 `statsApi`。
_Avoid_: Workers、云端 API

**认证流程实例（authApi）**：
携带 Cookie 但不配置 401 跳转的请求客户端实例；登录验证、认证状态探测中 401 是正常业务结果，必须透传后端信息。

**ApiError**：
请求失败时由请求客户端抛出的错误；`message` 已格式化为可直接渲染的简体中文，并携带 HTTP `status` 与原始 `detail`。
_Avoid_: ApiResponse（迁移期遗留的判别联合，最终淘汰）

**诊断（路由未命中诊断）**：
请求客户端内置的检测：响应体为 HTML 页面时，判定为"未命中后端 API 路由"并在 ApiError 中报出请求 URL，而不是静默重试其他地址。

### 服务端状态

**查询（Query）**：
通过 TanStack Query 的 `useQuery` 管理的服务端数据读取；加载失败由页面**局部呈现**（错误文案 + 重试），不弹全局 toast。

**变更（Mutation）**：
通过 `useMutation` 管理的服务端写操作；失败**默认弹全局 toast**（`lib/query.ts` 的 MutationCache 统一处理，`meta.suppressErrorToast` 可关闭、`meta.errorTitle` 定制标题），成功后按 queryKey 前缀失效相关查询。

**查询键（queryKey）**：
服务端状态缓存的分层标识，以领域名开头（如 `['persons', 'list', 参数]`），写操作成功后按前缀整体失效。

**数据列表（DataList）**：
对服务端集合的「分页 + 搜索 + 筛选 + 多选」视图，由 `useDataList` hook 统一承载。hook 内部包一个列表 **查询**（queryKey 从分页/搜索/筛选状态派生），对外给出 items/total 与全部控件状态；筛选/搜索/翻页变化时自动重置页码并清空选中集。对话框与具体渲染（表格/卡片）留在各页面，不进 hook。
_Avoid_: 列表容器、表格组件（DataList 是状态/数据 hook，不是 UI 组件）

### 配置与设置

**配置（Config）**：
存放在后端、修改后通常需要重启 MaiBot 生效的内容（bot、模型、提示词等），由 schema 驱动的动态表单编辑。

**设置（Settings）**：
仅存放在浏览器本地（localStorage/IndexedDB）的用户偏好（外观、缓存、安全），即时生效，不需要重启。
_Avoid_: 与"配置"混用

## Relationships

- **请求客户端** 有三个适配器实例：`backendApi`（**主后端**，401 跳登录）、`statsApi`（**统计服务**）、`authApi`（**认证流程实例**，401 不跳转）；Pack 配置市场与统计服务同域，共用 `statsApi`
- **ApiError** 只由 **请求客户端** 抛出；调用方不再手写 `response.ok` 分支
- **配置** 走 **主后端** 读写；**设置** 不经过任何请求客户端

## Example dialogue

> **Dev**：「插件市场的点赞数加载失败，要在 `backendApi` 里查吗？」
> **Domain expert**：「不，点赞走的是**统计服务**（`statsApi`），那是外部 Workers，没有 Cookie 认证；**主后端**只管插件本体的安装和配置。」
> **Dev**：「那报错时页面拿到的是什么？」
> **Domain expert**：「两个实例都抛 **ApiError**，`message` 可以直接给 toast 渲染；如果是路由配错返回了 HTML，**诊断**信息会写明未命中的 URL。」

## Flagged ambiguities

- 错误契约曾有两套并存：多数 API 返回 `ApiResponse<T>` 判别联合，memory-api 直接 throw —— 已裁决：统一为 throw **ApiError**，`ApiResponse` 仅作迁移期兼容包装。
- memory-api 曾在主地址 404/返回 HTML 时静默重试硬编码的 `localhost:8001` —— 已裁决：删除该兜底，保留**诊断**，配错必须显式暴露（2026-06）。
