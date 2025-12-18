## DuckDB 分析 Catalog（metadata-only）

目标：交互分析时可以直接 `SELECT * FROM ods.xxx`，而不需要手写 `read_parquet('s3://...')`。

### 设计约束

- catalog 数据库只保存 `SCHEMA + VIEW + MACRO`，不保存任何数据
- 数据事实只在 MinIO（`lake/...`）

默认路径：

- `./.duckdb/catalog.duckdb`（可用环境变量 `DUCKDB_CATALOG_PATH` 覆盖）

### Migrations（版本化 DDL）

- 迁移目录：`catalog/migrations/*.sql`
- 记录表：`catalog_meta.schema_migrations`（filename + checksum + applied_at）

本地执行：

```bash
uv run python -m scripts.duckdb_catalog_migrate
```

### Refresh（从 MinIO 扫描表并生成 view/macro）

刷新 ODS（生产数据）：

```bash
uv run python -m scripts.duckdb_catalog_refresh --base-prefix lake/ods
```

刷新集成测试数据：

```bash
uv run python -m scripts.duckdb_catalog_refresh --base-prefix lake/_integration/ods
```

### 常用查询

- 探索（可能触发列举分区）：
  - `SELECT * FROM ods.some_table;`
- 精确分区（避免列举，推荐在任务/批处理里使用）：
  - `SELECT * FROM ods.some_table_dt('2024-01-15');`

### Airflow 维护

- `dw_catalog_dag`：在 Airflow 内应用 migrations（pool=`duckdb_catalog_pool`，单写者）
- refresh 目前通过脚本执行（如需全自动，可在未来把 refresh 加入 DAG，但建议仍保持单写者约束）

