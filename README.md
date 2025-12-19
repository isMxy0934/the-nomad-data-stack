## the-nomad-data-stack

一个面向个人/小团队的轻量 Lakehouse 骨架：用 **MinIO（S3）+ Parquet + DuckDB + Airflow** 跑通「采集 → ODS → DW 分层」的每日批处理链路。

### 快速开始（本地）

1) 准备配置：复制 `env.example` 为 `.env` 并按需修改（默认即可跑通本地 MinIO）。

2) 启动服务：

```bash
docker compose up -d
```

3) 打开控制台：
- Airflow：`http://localhost:8080`（用户名 `admin`，密码来自 `.env` 的 `AIRFLOW_ADMIN_PASSWORD`，默认 `admin`）
- MinIO：`http://localhost:9001`（默认 `minioadmin/minioadmin`）

4) 跑一条完整链路（推荐触发“总开关” DAG）：
- 日增：在 Airflow UI 手动触发 `dw_start_dag`
  - 它会触发 `dw_extractor_dag`
  - extractor 完成后触发 `dw_catalog_dag`（应用 DuckDB catalog migrations）
  - catalog 完成后触发 `dw_ods`，再按 `dags/dw_config.yaml` 的层依赖顺序触发 `dw_{layer}`
- 初始化/回填：触发 `dw_init_dag`（推荐使用 Trigger DAG w/ config 表单）
  - 必须填写 `start_date`
  - `targets` 留空则全量运行所有表，或指定特定表初始化

5) 验证产物：
- 在 MinIO bucket（默认 `stock-data`）里查看 `lake/ods/.../dt=YYYY-MM-DD/` 下是否有 `*.parquet`、`manifest.json`、`_SUCCESS`

> 如果上游当日没有数据（例如 extractor 未产出 CSV），对应表会按“无数据成功”语义 no-op。

### 关键约定（用户只需要知道这些）

- **分区**：Hive 风格 `dt=YYYY-MM-DD`
- **默认处理日期（T-1）**：任务默认处理“昨天”的分区（见 `dags/utils/time_utils.py`）
- **提交协议**：写入临时前缀 → 校验 → 删除旧分区 → 拷贝新分区 → 写入 `manifest.json` + `_SUCCESS`
- **完成标记**：下游依赖 `_SUCCESS` / `manifest.json`，避免读到中间态
- **事实数据只在对象存储**：Parquet 是唯一事实；DuckDB 只做临时计算（不作为共享数据库）

### 常用 DAG

- `dw_start_dag`：日增入口（触发 extractor → catalog → ods → dw）
- `dw_init_dag`：初始化/回填入口（有 UI 表单，触发 catalog → ods → dw，不触发 extractor）
- `dw_catalog_dag`：应用 catalog migrations（metadata-only）
- `dw_ods`：ODS 层（CSV → Parquet 分区）
- `dw_{layer}`：DW 各层 DAG（如 `dw_dwd`、`dw_ads`），由 `dags/dw_dags.py` 动态生成
- `dw_extractor_dag`：日增采集（配置驱动，写入 `lake/raw/daily/.../dt=.../data.csv`）
- `dw_extractor_backfill_dag`：历史回填（写入 `lake/raw/backfill/.../dt=.../symbol=.../data.csv` + `_SUCCESS`）
- `dw_extractor_compact_dag`：回填合并（扫描 backfill，全量覆盖写入 daily 的 `data.csv`）
- `dw_finish_dag`：链路结束占位

### DW 初始化与回填

- 日增（默认）：不传参数触发 `dw_start_dag`，处理 `get_partition_date_str()` 对应的分区（T-1）。
- 初始化/回填：触发 `dw_init_dag`（建议使用 Airflow UI 提供的参数表单）：
  - 必填：`start_date`
  - 可选：`end_date`（不填则默认等于 `start_date`）
  - 可选：`targets`（留空则运行所有表；填写则必须是 `layer.table` 形式，如 `ods.fund_price_akshare`）
  - 规则：`start_date <= end_date`，将按天遍历分区，并行触发 `dw_catalog_dag` 进而刷新下游

示例（如需手动传 JSON）：
```json
{"start_date":"2025-01-01","end_date":"2025-01-07","targets":[]}
```

### 目录速览

- `dags/dw_config.yaml`：层依赖 + ODS sources（表清单）
- `dags/ods/*.sql`：ODS 表 SQL
- `dags/{layer}/*.sql`：DW 分层 SQL（例如 `dags/dwd/*.sql`）
- `lake/...`：MinIO 上的数据目录（运行后在 bucket 里出现）
- `.duckdb/catalog.duckdb`：分析用 DuckDB catalog（只存 schema/view/macro，不存数据）

### 更多文档（按需阅读）

- 约定与目录结构：`docs/conventions.md`
- 提交协议与标记语义：`docs/commit-protocol.md`
- DuckDB catalog（migrate/refresh/查询方式）：`docs/catalog.md`
