## the-nomad-data-stack

一个面向个人/小团队的轻量数据仓库（Lakehouse）骨架：用 **MinIO（S3 协议）+ Parquet（列式存储）+ DuckDB（SQL 计算）+ Airflow（编排）** 跑通「采集 → 分层建模 → 指标计算」链路。

### 背景与目标

- 用尽量少的组件实现类似 **Hive（外部表/分区）+ Spark（SQL/ETL）** 的体验：存储与计算分离、SQL 驱动、分层清晰、可增量可回放。
- 数据统一落在对象存储（MinIO）上；计算统一由 DuckDB 执行 SQL；Airflow 负责把任务依赖、调度与分区参数（如 `PARTITION_DATE`）串起来。

### 系统架构（组件职责）

- **MinIO（对象存储）**：存放所有数据文件（原始 CSV、ODS/DWD/DIM/ADS 的 Parquet 产物），是“数据事实”的唯一来源。
- **DuckDB（SQL 计算引擎）**：每个任务运行时临时连接（内存库或临时 `.duckdb` 文件），读取 Parquet、执行 SQL、写回 Parquet；不把 DuckDB 当作共享元数据库/共享存储，避免多任务争抢同一 DuckDB 文件锁。
- **Airflow（编排与调度）**：负责任务依赖、调度、参数化（分区日期）、重跑与回放。
- **Postgres（元数据与审计）**：
  - 必选：Airflow 自身的 metadata DB（`docker-compose.yml` 已包含）。

### 数据流与分层

1. **Extractor（采集）**：从 AkShare/TuShare 等源拉取数据，写入 MinIO（CSV/原始格式）。
2. **ODS（落地/轻标准化）**：按表 SQL 做类型转换、补分区列 `dt`，写入 ODS 的 Parquet 分区目录。
3. **DWD / DIM（清洗建模）**：继续用 SQL 读取 ODS（逻辑表），写入 DWD/DIM 的 Parquet 产物目录（是否分区取决于数据形态）。
4. **ADS / Metrics（指标层）**：读取 DWD/DIM（逻辑表）进行指标计算；可先视图化，热点指标再物化落盘。

> 约定：**Parquet 文件是唯一事实**；DuckDB 只在任务运行时临时注册“逻辑表 → 物理路径”（视图/外部表）用于 SQL 读取与计算。

### 目录与分区规范

默认使用 `dt=YYYY-MM-DD`（Hive 风格）作为分区列：

- ODS：`dw/ods/{table}/dt=YYYY-MM-DD/*.parquet`
- DWD：`dw/dwd/{table}/dt=YYYY-MM-DD/*.parquet`
- DIM（默认不分区）：`dw/dim/{table}/*.parquet`（如需版本化：`as_of=YYYY-MM-DD` 或 `version=...`）
- ADS：`dw/ads/{metric_or_table}/dt=YYYY-MM-DD/*.parquet`

### SQL 驱动（配置 + 模板）

#### 1) 配置驱动（表清单）

ODS（以及后续层）通过配置文件声明“要处理哪些表”，并约定 `dest.sql` 为对应 SQL 文件名：

- 配置：`dags/ods/config.yaml`
- SQL：`dags/ods/{dest}.sql`

`config.yaml` 示例（节选）：

```yaml
- src:
    type: "s3"          # 指 S3 协议（MinIO）
    properties:
      path: "raw/daily/stock_price/akshare"  # 源数据（通常为 CSV）所在的 MinIO 前缀
  dest: "ods_daily_stock_price_akshare"
```

#### 2) SQL 模板与分区参数

SQL 通过 `dt` 固化分区，并使用 Airflow 传参（示例：`${PARTITION_DATE}`）：

```sql
SELECT
  CAST(symbol AS VARCHAR) AS symbol,
  ...
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_daily_stock_price_akshare;
```

#### 3) 任务内注册逻辑表（固定模板）

任务启动时，统一按下面模板启用 MinIO/S3 读取能力，并注册逻辑表 view（后续 SQL 只引用逻辑表名，不硬编码路径）：

```sql
INSTALL httpfs;
LOAD httpfs;

-- 凭证与 endpoint 推荐用 DuckDB Secrets（或用环境变量/参数注入，按部署方式选择）
-- 建议：生产镜像构建阶段预装扩展（httpfs/aws），任务运行时只执行 LOAD（必要时 INSTALL 作为 fallback）

CREATE SCHEMA IF NOT EXISTS ods;
```

**生产任务读（推荐）**：
```sql
CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare__dt AS
SELECT *
FROM read_parquet(
  's3://<bucket>/dw/ods/ods_daily_stock_price_akshare/dt=${PARTITION_DATE}/*.parquet',
  hive_partitioning=true
);
```

**交互分析读**：
```sql
CREATE OR REPLACE VIEW ods.ods_daily_stock_price_akshare AS
SELECT *
FROM read_parquet(
  's3://<bucket>/dw/ods/ods_daily_stock_price_akshare/dt=*/**/*.parquet',
  hive_partitioning=true
);
```

> **读取模式说明**：
> - **生产任务读**：使用精确分区路径，避免触发 S3 ListObjectsV2 列举，减少成本和延迟
> - **交互分析读**：使用 `dt=*` 通配符方便探索数据（会触发列举，适合 ad-hoc 查询）

#### 4) 分区写入（DuckDB 原生 partitioned writes + 幂等）

用 DuckDB 的 `COPY ... PARTITION_BY` 直接写 Hive 风格分区目录，避免手工拼目录并保证一致性。

**临时前缀提交协议（生产推荐）**：
```sql
-- 1. 写到临时前缀（只新增，不覆盖）
COPY (
  SELECT
    ...,
    CAST('${PARTITION_DATE}' AS DATE) AS dt
  FROM ...
) TO 's3://<bucket>/dw/ods/ods_daily_stock_price_akshare/_tmp/run_${RUN_ID}'
(FORMAT parquet, PARTITION_BY (dt), FILENAME_PATTERN 'file_{uuid}', WRITE_PARTITION_COLUMNS false, USE_TMP_FILE true);

-- 2. 校验产出质量（row_count/file_count/schema_hash）
-- 3. 写完成标记
-- 4. 切换/清理旧分区（S3 rename = copy+delete，评估成本）
```

> **USE_TMP_FILE true**：先写临时文件再替换，降低写入中断导致"坏文件覆盖"的风险

> **重要约束**：远端分区更新 = commit protocol，不允许 in-place overwrite。OVERWRITE_OR_IGNORE 在 S3/MinIO 上不可靠，可能导致部分文件覆盖失败。
>
> **兼容性注意**：
> - 如果只用 DuckDB 读写：`WRITE_PARTITION_COLUMNS false` 通常不影响
> - 如果未来要被 Spark/Polars/PyArrow/Trino 读：上线前用一个小分区做验收（检查 schema/列冲突/类型），必要时采用"落盘后 drop 分区列"的策略

运行约束（强约束）：

- **单写者**：同一 `(table, dt)` 在任一时刻只允许一个任务写入（通过 Airflow pool 或 concurrency 限制保证）。
  - 用 **pool**（按表或按分区写入资源建 pool），把写分区任务放进同一个 pool
  - 或用 DAG 的 `max_active_runs` / `concurrency` / task 的并发限制
- **幂等重跑**：
  - ✅ **主方案（强推荐）**：写临时前缀 → 校验（row_count/file_count/schema_hash）→ 写 `_SUCCESS`/manifest → 切换/清理旧分区前缀
  - ⚠️ `OVERWRITE_OR_IGNORE` 仅作为"某些环境可用的优化"，不能作为可靠语义（尤其在 S3/MinIO 上不支持 overwriting）
- **完成标记**：分区写完后写入 `_SUCCESS` 或 `manifest.json`；下游只依赖“标记存在”，避免读到写入一半的数据。
- **小文件治理**：长期运行会产生大量小 Parquet；增加周期性 compaction（按表/按月合并）提升读取性能与成本。
- **Schema 管理**：至少为每张表声明 schema（YAML/DDL），写入前校验（缺列/类型变化报警），避免无意 schema 演进让下游查询变脆。


### Airflow DAG 组织

当前 `dags/` 下的目录/职责：

- `dags/extractor/*`：采集 DAG（写入 MinIO，通常为 CSV）
- `dags/ods/*`：ODS 配置与 SQL
- `dags/utils/*`：通用工具（如 S3 上传、日期分区）
- `dags/ods_loader_dag.py`：ODS 加载 DAG（读取 `config.yaml`，按 SQL 落 Parquet，并写入 `_SUCCESS` 完成标记）

依赖关系：

- ODS：`extractor_*` → `ods_loader_*`
- DWD：等待对应 ODS 分区完成 → 执行 `dwd/{table}.sql` → 写入 `dw/dwd/...`

### 本地启动（Docker Compose）

本项目提供 `docker-compose.yml`，包含 Airflow、Postgres、MinIO。

准备 `.env`（按 `docker-compose.yml` 引用提供变量），常见包括：

- `AIRFLOW_METADATA_DB_URL`
- `AIRFLOW_CONN_MINIO_S3`
- `S3_BUCKET_NAME`（默认 `stock-data`）
- `TUSHARE_TOKEN`（如使用 TuShare）

启动：

```bash
docker compose up -d
```
