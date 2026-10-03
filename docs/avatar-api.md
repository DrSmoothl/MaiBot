# 独立头像接口

头像通过专门的接口获取，消息与聊天流数据不携带图片字节或 base64。

## 适配器协议

在现有适配器插件上声明公开 API `adapter.avatar.get`，版本 `1`：

```python
@API("adapter.avatar.get", version="1", public=True)
async def get_avatar(
    self, platform: str, target_id: str, target_type: str = "user",
    account_id: str = "", scope: str = "",
) -> Dict[str, Any]:
    return {"status": "available", "url": "https://example.com/avatar.png", "expires_in": 86400}
```

`target_type` 为 `user` 或 `group`。`account_id`、`scope` 用于同平台多个账号/连接的路由。
Host 根据 Platform IO 的路由找到所属适配器，再调用该插件的 API，不按 API 短名随机选择提供方。

返回状态：

| status | 含义 | 附加字段 |
| --- | --- | --- |
| available | 可以获取头像 | `url` 必填，`expires_in` 为正数秒数，默认 86400 |
| unsupported | 平台或目标类型不支持头像 | 不需要 URL |
| missing | 支持查询，但该目标没有头像 | 不需要 URL |

未声明此 API 的适配器自动视为 `unsupported`。查询异常、下载失败和协议错误应抛出错误，不返回旧头像或改用其他平台的头像。URL 必须是 Host 可以直接读取的 HTTP(S) 图片地址，不含凭据，不需要 Cookie 或跳转；本地文件和图片 base64 不属于此协议。

## SDK

插件在 manifest 的 `capabilities_required` 中声明 `chat.get_avatar`：

```python
avatar = await self.ctx.chat.get_avatar(
    platform="qq", target_id="1026294844", target_type="user",
    account_id="", scope="", force_refresh=False,
)
if avatar["status"] == "available":
    url = avatar["url"]
```

返回 `status`、`url`、`expires_at`；`expires_at` 是 Unix 时间戳。URL 为适配器提供的头像源地址，可以独立下载，不是 Host 文件路径。SDK 调用与 WebUI 共用 Host 头像缓存与过期规则，异常通过能力调用暴露。

## WebUI 与缓存

WebUI 图片入口继续使用 `GET /api/webui/avatar`，必须且只能指定 `user_id` 或 `group_id`。可选参数为 `account_id`、`scope`、`force_refresh=true`。远程头像由统一服务查询适配器，下载校验后返回缓存图片；`unsupported` / `missing` 返回带状态的 404，由界面显示默认头像；调用或下载失败返回 502。

- 图片上限 5 MB，支持 JPG、PNG、WebP、GIF、BMP。
- 正常缓存有效期为适配器的 `expires_in`，最多 24 小时；到期重新查询并下载。
- `unsupported` / `missing` 缓存 5 分钟；错误不缓存，也不使用过期图片兜底。
- 缓存键包含平台、账号、连接、目标类型和目标 ID。同一目标的刷新请求合并，文件与 JSON 元数据原子替换；格式变化时移除旧图片。
- 缓存存放于 `data/avatar/remote/`。以前 `data/avatar/qq/` 中没有有效期的缓存不再读取。
- 浏览器缓存最多 5 分钟，且不超过 Host 缓存剩余有效期；强制刷新响应使用 `no-store`。
- WebUI 用户自己上传的头像仍为本地持久资源，不适用远程缓存过期机制。

已打开页面里的图片不会自行定时重载，缓存过期在下一次图片请求时生效。
