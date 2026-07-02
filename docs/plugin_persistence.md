# 插件持久化路径约定

插件需要保存运行时数据时，应使用运行时注入的标准路径：

```python
data_path = self.ctx.paths.data_dir / "records.json"
runtime_path = self.ctx.paths.runtime_dir / "render.png"
```

- `self.ctx.paths.data_dir`：持久化数据目录，默认位于 `data/plugins/<plugin_id>/`，用于保存需要长期保留的数据。
- `self.ctx.paths.runtime_dir`：非持久运行时目录，默认位于 `temp/plugins/<plugin_id>/`，用于保存缓存、临时文件和中间产物。

插件不应将运行时数据写入插件源码目录，也不应自行根据主程序根目录拼接持久化路径。路径由运行时按插件 ID 分配，插件只使用被授予的目录。

运行时会校验插件 ID 和最终路径，防止路径逃逸。如果检测到旧式 `plugins/<plugin>/data/` 目录，运行时会输出迁移提示；第一版不会自动迁移用户数据。
