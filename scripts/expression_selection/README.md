# 表达选择评测脚本

这个目录只保留表达选择评测相关的核心入口。

## 1. LLM 表达选择评测器

```powershell
uv run python scripts/expression_selection/llm_judge.py `
  --input-json data/analysis/expression_selection_batch_compare_full_pipeline_20260622_173330.json `
  --llm-task-name utils `
  --model-name deepseek-v4f `
  --max-tokens 512
```

作用：读取三方案 batch，随机盲化为 A/B/C，让 LLM 评估排序和打分。

## 2. 三种表达选择器离线运行器

```powershell
uv run python scripts/expression_selection/offline_runner.py `
  --input-json data/analysis/expression_selection_batch_compare_live_intent_20260622_164359.json `
  --limit 30 `
  --selector-task-name planner `
  --selector-max-tokens 4096 `
  --vector-pool-size 50
```

作用：构建完整链路 batch：

- `legacy_precise`：legacy 候选池 + 精细选择器
- `vector_no_intent_precise`：不带 intent 的向量候选池 + 精细选择器
- `vector_intent_online`：live-log 中真实线上 vector_intent 结果

## 3. 辅助工具

构建表达向量索引：

```powershell
uv run python scripts/expression_selection/vector_index_tools.py build-index --clusters 80
```

刷新与当前 embedding profile 不一致的表达：

```powershell
uv run python scripts/expression_selection/vector_index_tools.py refresh-profile --limit 200
```

不写子命令时默认执行 `build-index`。
