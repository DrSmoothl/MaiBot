# 内置桌宠与插件 SDK

桌宠由主程序管理，不再需要安装 `sengokucola.desktop-pet` 插件。目前支持 Windows 桌面环境，需要 WebView2 Runtime。Windows 的 Python 依赖已包含 pywebview，源码环境更新依赖可运行 `uv sync --inexact`（保留插件额外依赖）。

在 WebUI 主程序配置的「实验性功能 / 桌宠」启用 `desktop_pet.enabled`，默认关闭。配置支持热重载，关闭即退出桌宠，主程序退出也会回收窗口。图形界面仍使用独立子进程，以免阻塞聊天和网页事件循环；界面、待办和启动管理代码都已随主程序分发。

旧桌宠插件会被明确阻止加载并显示内置替代提示，避免同时启动两个窗口；插件源码保留。继续读写 `data/plugins/sengokucola.desktop-pet/todos.json` 和 `window.json`，已有待办和窗口位置无需复制。临时 WebView 数据改用 `temp/desktop_pet`。

桌宠聊天继续使用 `internal / owner`，昵称由网页聊天室统一设置。麦麦给主人的回复在桌宠和网页同步展示；旧 `webui` 私聊不迁移。桌宠提交的消息保留界面来源标记，不具备受保护命令权限。

## 插件控制

使用 SDK 2.17.0+ 和带有本功能的 MaiBot 1.3.16+ Host。SDK 尚未发布时使用[本地 SDK 开发模式](internal-chat.md#sdk-开发联调)。内置桌宠本身不依赖新版 SDK。

清单按需声明能力：

```json
"capabilities": [
  "desktop_pet.status", "desktop_pet.show", "desktop_pet.hide",
  "desktop_pet.say", "desktop_pet.set_thinking"
]
```

```python
state = await self.ctx.desktop_pet.status()
if state["ready"]:
    await self.ctx.desktop_pet.show()
    await self.ctx.desktop_pet.set_thinking(True)
    try:
        await self.ctx.desktop_pet.say("待办已经整理好了")
    finally:
        await self.ctx.desktop_pet.set_thinking(False)
    # await self.ctx.desktop_pet.hide()
```

| 接口 | 行为 |
| --- | --- |
| `status()` | 返回 `enabled`、`supported`、`running`、`ready`、`visible`、`error` |
| `show()` / `hide()` | 显示或隐藏已启动的窗口；隐藏不退出进程 |
| `say(text)` | 显示 1～2000 字符纯文本提示气泡，不加入聊天记录、学习或记忆 |
| `set_thinking(active)` | 布尔值，开启或结束思考动画；聊天回复也会控制动画 |

所有控制接口返回当前状态，窗口回执成功后才返回；未就绪、窗口执行失败、通信超时会抛错。`status()` 在关闭或不支持的平台上也可调用。启动中的窗口可能 `running=True` 而 `ready=False`，插件应跳过本次桌面提示或稍后自行查询。`show()` 不会绕过用户的启用开关，隐藏状态下的 `say()` 也不会擅自显示窗口。

插件不能注入 JavaScript、HTML 或原生窗口代码。气泡提示与 `ctx.internal_chat.send(text)` 不同：后者以主人身份真正发起聊天，可能触发麦麦回复并进入聊天历史；前者只在桌面显示提示，不触发模型调用。
