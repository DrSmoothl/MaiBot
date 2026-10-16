# 插件 CNB 镜像

插件索引、按 Tag 安装/更新、分支安装/更新支持使用 `https://cnb.cool/Mai-with-u` 下的公开镜像，无需用户填写 CNB 令牌。镜像由插件登记仓库的 GitHub Actions 维护。

## 选择获取方式

WebUI「插件商店设置」中可以分别决定列表数据源和代码下载源：

- 「使用 Git 仓库和镜像源获取数据」关闭：继续由插件中心 API 提供列表、版本和统计，安装、更新代码仍按镜像配置下载。
- 开启该选项：列表详情和版本索引使用镜像配置读取；CNB 默认优先，其后保留现有 GitHub 与代理源。
- 镜像表可以禁用、调整顺序或删除 CNB。明确指定某一个镜像时，失败直接返回该源错误；未指定时按启用源顺序尝试。

升级时只向旧镜像配置补入一次 CNB，保留其他源的开关和相对顺序。用户删除后不会在每次启动时重新添加；「恢复默认」可重新加入。

## 同步范围和校验

GitHub 原仓库地址继续用于插件身份与安装记录。下载时将其转换为 CNB 镜像地址：索引仓库为 `Mai-with-u/plugin-repo`，插件仓库使用与同步脚本相同的名称规范化及 SHA-256 路径摘要。

CNB Raw 文件路径使用 `/-/git/raw/<ref>/<path>`。返回 HTML 或不合法 JSON 的源不能被当作有效索引，HTTP 404 等确定性错误不重复请求。

发布版本继续克隆索引中的 Tag，并在替换现有目录前核对 `HEAD == release.commit`、完整 manifest、插件身份和依赖。commit 或 manifest 不一致直接拒绝安装/更新，不会因为使用镜像而跳过检查。

分支更新从选中的源 fetch，再执行本地 checkout 和 fast-forward merge；不改写 origin 地址，不强制覆盖本地修改，也不删除用户配置及数据。只有 fetch 网络失败可以切换源，本地冲突或分叉会明确报错。

索引镜像需要完成首次同步才能使用。源仓库新增版本与 CNB 完成同步之间存在时间差：缺少 Tag 会报下载失败，Tag 指向与索引不一致会报校验失败。Git Release 页面及附件不参与当前插件安装流程，无需同步。

## 实测与复现

测速覆盖所有内置源、已配置的自定义源，以及插件中心 API：

```powershell
uv run --no-sync python -m scripts.benchmark_plugin_sources `
  --direct --rounds 3 --timeout 15 --git-timeout 45 --concurrency 3 `
  --registry ../plugin_repo/plugin_versions.json `
  --output docs/plugin-source-direct-benchmark.json
```

脚本读取实际镜像配置，固定指定单源，不在测速中自动换源；每轮读取三个索引文件、克隆同一插件的已审核 Tag 和 main 分支，再执行一次无上游变化的分支更新。另测插件中心的聚合列表和详情 API。每次克隆使用临时目录，不安装或执行插件，不改动用户的数据源设置。

输出 JSON 保留逐次结果、内容摘要、commit、耗时和错误，Markdown 汇总成功率及成功请求的中位数、范围。超时是测试预算，不能算成功，也不代表永久不可用。插件中心聚合响应与原始 Git 索引内容不同，应分别比较。

HTTP 超时按连接、读取等阶段生效，不是整个请求的总时限；总耗时还包含域名校验和内容传输。Git 超时则限制整个克隆或更新操作。

`--direct` 强制 HTTP `trust_env=False`，Git 隔离系统/全局配置、代理环境变量及 URL 重写，仅保留 TLS CA 配置；不会修改系统或应用设置。报告记录 HTTP 实际连接地址。第三方 Git 代理站本身仍作为待测源访问。

本机无代理实测：[全部源汇总](./plugin-source-direct-benchmark.md)、[原始数据](./plugin-source-direct-benchmark.json)；[CNB 与插件中心 20 轮对比](./plugin-cnb-direct-stability.md)。早期的 `plugin-source-benchmark` 使用了应用默认代理路径，不能用于比较直连表现。短时测试受所在地、运营商、镜像缓存和同步进度影响，不代表长期可用性。

复现 CNB 与插件中心的连续探测：

```powershell
uv run --no-sync python -m scripts.benchmark_plugin_sources --direct `
  --sources cnb,market-service --http-only --rounds 20 --interval 10 --concurrency 1 `
  --output docs/plugin-cnb-direct-stability.json
```
