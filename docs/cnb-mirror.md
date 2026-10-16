# MaiBot 的 CNB 镜像

`Sync MaiBot to CNB` 在本仓库独立运行，将 `https://github.com/Mai-with-u/MaiBot` 镜像到 CNB。插件和索引仍由 `plugin-repo` 的 `Sync CNB Mirrors` 管理，两条流程使用不同目标仓库。

## 配置

在 **MaiBot 仓库 → Settings → Secrets and variables → Actions** 添加：

| 类型 | 名称 | 值 |
| --- | --- | --- |
| Secret | `CNB_TOKEN` | 可复用插件镜像的 CNB 令牌，但需要在本仓库再次配置 |
| Variable | `CNB_GROUP` | `Mai-with-u` |
| Variable | `CNB_MAIBOT_REPO` | 可选，默认 `maibot` |

组织级 Secret / Variable 若已授权 MaiBot 仓库则无需重复添加；插件仓库自己的 Repository Secret / Variable 不会自动共享。CNB 令牌需覆盖目标组织，具备 `group-resource:rw`、`repo-basic-info:r`、`repo-manage:rw`、`repo-code:rw`。无需额外 GitHub PAT。

目标默认为 `https://cnb.cool/mai-with-u/maibot`。该路径必须专用于 MaiBot 镜像，不要填启动器或其他开发仓库的名称，因为镜像会强制更新分支和 Tag，并清理上游已删除的引用。

先将 `.github/workflows/cnb-mirror.yml`、`.github/scripts/sync_cnb_mirror.cjs` 和对应测试部署到 `main`，然后将工作流带入需要监听 push 的长期分支（例如 `dev`）。GitHub 的 push 事件使用对应分支或 Tag 中的工作流文件；默认分支上的工作流负责接收定时、手动、删除及 `workflow_run` 事件。执行脚本始终从 `main` 检出，且每次都重新读取 GitHub 当前全部分支和 Tag，不会用旧事件的提交回退镜像。

首次配置后，在 **Actions → Sync MaiBot to CNB → Run workflow** 手动执行一次，确认摘要中的目标、默认分支及 commit。

## 触发与范围

- 分支和 Tag 的 push、新建、删除触发同步。
- `Publish MaiBot`、`Prepare Main Release`、`Publish WebUI Dist`、`Update Dashboard Dependency`、`Ruff` 结束后触发，覆盖 `GITHUB_TOKEN` 生成的提交或 Tag 不再触发 push 工作流的情况。这些任务失败时也会检查当前 Git 源状态，避免遗漏失败前已推送的提交；镜像不以 CI 通过作为代码准入条件。
- 支持手动同步；每天北京时间 02:30、15:30 定时校准。GitHub 定时任务可能延迟；同时批量操作大量分支或 Tag 也可能不产生逐个事件。
- 所有触发共用并发组，不中断正在进行的推送；后续运行会读取最新源快照。

同步全部分支、Tag、完整 Git 历史和 Git LFS，保留 commit SHA 和注解 Tag。不会执行源代码，不同步 GitHub Release 页面或附件、Issue、PR，也不递归镜像 submodule 或外部依赖。CNB 自动构建在上传代码前关闭，设置失败则停止上传。

状态以 Action 摘要和附件 `maibot-cnb-mirror-status/maibot.json` 为准，包含成功时间、默认分支、commit 或失败原因。超时、权限不足和 LFS 失败会使任务标红，不会声称同步成功。没有配置 `CNB_GROUP` 时任务跳过。GitHub 的普通 push 触发仍受 `GITHUB_TOKEN`、提交跳过指令等平台规则影响；上述工作流联动和定时任务提供后续校准。

## 本地验证

```powershell
$env:CNB_GROUP = 'Mai-with-u'
node .github/scripts/sync_cnb_mirror.cjs --dry-run
node --test .github/scripts/sync_cnb_mirror.test.cjs
```

预览不需要令牌，不联网，也不修改 CNB。正式运行需要 Node.js 22、Git、Git LFS 和上述配置。正式 GitHub runner 使用 Linux；本地正式运行会实际修改目标镜像。

触发规则依据：[GitHub Actions 事件文档](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)。
