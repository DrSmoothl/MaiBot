# 使用 uv tool 安装 MaiBot（本地试用）

当前先提供本地构建和安装方式，尚未发布到 PyPI。

在源码仓库目录构建：

```bash
uv build
```

选择 `dist` 中生成的 wheel 安装，例如当前版本：

```bash
uv tool install --python 3.12 ./dist/maibot-1.3.8-py3-none-any.whl
```

如果命令目录还不在 PATH 中，按 uv 的提示执行 `uv tool update-shell`，然后重新打开终端。

初始化和运行实例：

```bash
maibot init --directory ./my-maibot
maibot run --directory ./my-maibot
```

`init` 创建 `config`、`data`、`logs`、`plugins`、`temp`，只生成缺失的配置文件，不覆盖已有配置，不创建数据库，也不代替用户确认协议。运行前在 `config/model_config.toml` 中配置模型；首次 `run` 按原有流程确认协议，并启动 WebUI。消息平台仍需通过适配器接入。

省略 `--directory` 时使用当前目录。实例目录可以包含空格和中文；需要在命令行中使用引号。多个实例可以使用不同目录，但同一台机器上的监听端口需要分别配置。同一个 uv 工具环境中的实例共享主程序版本和 Python 插件依赖。

程序源码、默认提示词、翻译和内置插件来自安装包；配置、数据库、第三方插件、自定义提示词及运行缓存写入实例目录。卸载工具或替换 wheel 不删除实例目录。

本地升级时，先停止所有使用该工具环境的 MaiBot 实例，再安装新 wheel：

```bash
uv tool install --force --reinstall-package maibot --python 3.12 ./dist/maibot-1.3.8-py3-none-any.whl
```

将上例中的文件名替换为新构建的 wheel。下次启动会按插件声明检查和补装缺失依赖。安装版不执行 Git 自更新；WebUI 会说明需要通过工具安装方式升级。未来发布到 PyPI 后可提供 `uv tool upgrade maibot`。

源码部署和 OneKey 继续运行原来的 `python bot.py`。没有设置 `MAIBOT_INSTANCE_DIR` 时，配置和数据仍放在仓库目录；该环境变量供 CLI 传递实例目录给子进程使用。

打包实现保留 `src.*` 导入路径。运行所需的仓库级只读资源由构建步骤复制到 `src/_resources`，不会复制现有 `config`、`data`、`logs` 或第三方插件。

A_Memorix 的 `paths.py` 包含最小安装兼容补丁，将宿主实例目录与包内 schema 资源分开定位。该耦合修改需后续同步上游 `MaiBot_branch`，遵循其修改归属约定。
