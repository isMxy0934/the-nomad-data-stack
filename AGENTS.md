# AI Agent 开发指南（the-nomad-data-stack）

本项目当前架构以 **MinIO（S3）+ Parquet + DuckDB + Airflow** 为核心，强调“存储与计算分离 + 约定优于配置 + 单写者”。该文档用于指导 AI Agent 在仓库内做开发与改动，避免破坏关键约定。

## 核心原则

- **存储与计算分离**：数据事实只在 MinIO（`lake/...`）；ETL/验证阶段的 DuckDB 连接应使用内存库或临时文件，不作为共享数据库。
- **分析 Catalog（metadata-only）**：允许维护一个持久化 DuckDB 文件 `./.duckdb/catalog.duckdb`，仅保存 `SCHEMA + VIEW + MACRO`，不存数据；用于交互分析 `SELECT * FROM ods.xxx`。
- **约定优于配置**：目录结构即配置；
  - ODS：表清单见 `dags/ods/config.yaml`，SQL 文件按 `dags/ods/{dest}.sql` 命名。
  - DW：层依赖见 `dags/dw_config.yaml`，表通过扫描 `dags/{layer}/*.sql` 发现（空层/缺目录跳过，不生成占位）。
- **单写者原则**：同一 `(table, dt)` 同时只能一个任务写入；通过 Airflow `pool`/并发限制保证。

## 关键架构上下文 (Critical Architecture Context)

以下设计决策是基于业务场景（每日批处理 + 轻量级）的权衡，**在没有明确重构指令前，请勿修改**：

1.  **分区时间（T-1）**：
    *   代码逻辑：`dags/utils/time_utils.py` 中的 `get_partition_date_str()` 默认取 `now() - 1 day`。
    *   原因：简化了 DAG 的参数传递，任务总是处理“昨天的”数据。
    *   **禁止**：不要将其重构为 Airflow `{{ ds }}` 或 `{{ data_interval_start }}`，除非你准备重写所有 SQL 模板和回填逻辑。

2.  **Commit Protocol（非原子）**：
    *   代码逻辑：`dags/utils/partition_utils.py` 中的 `publish_partition` 执行顺序是：`_delete_prefix` -> `_copy_prefix`。
    *   原因：纯 S3 架构不支持原子重命名（Atomic Rename）。
    *   **禁止**：不要试图引入“先写后切指针”的复杂逻辑（除非引入 Hive Metastore/Iceberg），也不要试图用 `OVERWRITE_OR_IGNORE` 替代 Delete。我们接受极小概率的数据丢失风险（Delete 成功 Copy 失败）。

## 数据与分区约定

- 分区列：`dt=YYYY-MM-DD`（Hive 风格），不要修改分区命名约定。
- 建议路径：
  - ODS：`lake/ods/{table}/dt=YYYY-MM-DD/*.parquet`
  - DW（DWD/DIM/ADS 等）：`lake/{layer}/{table}/dt=YYYY-MM-DD/*.parquet`
  - 临时写入：`lake/ods/{table}/_tmp/run_{run_id}/dt=YYYY-MM-DD/*.parquet`
- 下游依赖：以 `_SUCCESS` / `manifest.json` 为完成标记，避免读到写入中间态；M2 约定“无 `_SUCCESS` 视为无数据”，由编排保证上游失败不触发下游。

## 写入与提交协议（Commit Protocol）

- 禁止 in-place overwrite 远端分区。
- 标准流程：写入临时前缀 → 校验（行数/文件数等）→ promote 到 canonical 分区 → 写入 `manifest.json` + `_SUCCESS` → 清理临时前缀。
- 相关实现：`dags/utils/partition_utils.py`、`dags/ods_loader_dag.py`。

## M2：DW（DWD/DIM/ADS）约定摘要

- 每层生成独立 DAG：`dw_{layer}`（例如 `dw_dwd`），由 `dags/dw_dags.py` 统一生成。
- 表命名必须带 layer 前缀：例如 `dags/dwd/dwd_daily_stock_price.sql` → `dwd.dwd_daily_stock_price`。
- 任务运行时只读 attach `./.duckdb/catalog.duckdb`（metadata-only），SQL 只引用逻辑表（如 `ods.xxx`），不硬编码 S3 路径。

## DuckDB 扩展与 S3 访问

- 通过 `httpfs` 访问 S3/MinIO；镜像构建阶段预装扩展（见 `infra_build/dockerfiles/Dockerfile.airflow`），运行时优先 `LOAD httpfs`，必要时 fallback `INSTALL`。
- 相关实现：`dags/utils/duckdb_utils.py`。

## Catalog（分析用）维护约定

- 迁移：`catalog/migrations/*.sql` → 写入 `catalog_meta.schema_migrations`（只记录文件名+checksum+时间）。
- 刷新：扫描 `lake/ods`（或 `lake/_integration/ods`）创建/更新 `ods.*` 视图与 `ods.<table>_dt()` 宏。
- 单写者：维护 catalog 的任务请使用 Airflow pool `duckdb_catalog_pool`（slots=1），或串行执行脚本，避免同一 DuckDB 文件被并发写入导致锁冲突。
- 相关入口：
  - 脚本：`scripts/duckdb_catalog_migrate.py`、`scripts/duckdb_catalog_refresh.py`
  - DAG：`dags/dw_catalog_dag.py`

## 开发模式（强制）

1. **增量开发**：按 `PLAN.md` 里程碑推进。
2. **测试驱动**：核心逻辑必须有单元测试（`tests/`）与必要的验证脚本（`scripts/validate_*.py`）。
3. **文档同步**：行为/约定变化要同步更新 `README.md` 和 `PLAN.md`。

## 禁止事项

- ❌ 不要绕过 commit protocol 直接写分区产物到 canonical 前缀。
- ❌ 不要修改分区命名约定（`dt=...`）及目录约定。
- ❌ 不要在任务执行中把 DuckDB 当作共享元数据库/共享存储（避免锁争用与状态漂移）。
- ❌ 不要添加不必要的配置项；优先沿用现有约定与工具函数。

## 工具与代码规范

- 优先复用 `dags/utils/*.py`，新功能尽量抽象为可复用工具。
- SQL 模板必须参数化，至少支持 `${PARTITION_DATE}`。
- 类型注解完整、错误处理充分、日志记录清晰。

## 质量保障（本地）

```bash
# 1) 安装依赖
uv sync --group dev

# 2) 运行测试
uv run python -m pytest tests/ -v

# 3) 代码质量检查
uv run ruff check .

# 4) 代码格式化
uv run ruff format .
```

## 质量保障（CI）

- GitHub Actions 会运行单元测试/集成测试与 ruff 检查；如需手动触发集成测试，可用 workflow dispatch 选择 `target_table`。
