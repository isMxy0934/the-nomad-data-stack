# 开发计划与进度追踪（the-nomad-data-stack）

本文件用于追踪项目进度就把 `- [ ]` 勾成 `- [x]` 代表已经完成。

---

## 当前状态（基于仓库已有内容）

- [x] 文档：`README.md` 已补齐整体架构与约定
- [x] 计划：`PLAN.md` 已建立并可勾选追踪
- [x] 基础设施：`docker-compose.yml` 已包含 Airflow + Postgres + MinIO
- [x] Extractor DAG：`dags/extractor/*` 已存在（AkShare/TuShare → MinIO CSV）
- [x] ODS 配置与示例 SQL：`dags/ods/config.yaml`、`dags/ods/*.sql` 已存在
- [x] 工具类：`dags/utils/s3_utils.py`、`dags/utils/time_utils.py` 已存在

> 说明：以上是“文件/结构已存在”，不等价于“已跑通验证”。跑通验证会在对应任务项里单独勾选。

---

## 里程碑

- **M0 基础可运行**：本地 `docker compose up` 后 Airflow/MinIO/Postgres 可用，Extractor DAG 可写入 MinIO。
- **M1 ODS 打通**：ODS Loader 能从 MinIO 读原始数据（CSV）→ DuckDB 执行 SQL → 写 ODS 分区 Parquet → 写 `_SUCCESS/manifest` →（可选）写入 catalog。
- **M2 DWD/DIM 打通**：读取 ODS（逻辑表）→ 写 DWD/DIM 产物（分区/非分区策略清晰）→ 完成标记 → catalog 记录。
- **M3 ADS/Metrics 打通**：指标 SQL 驱动，按日运行并**物化落盘**到 ADS 分区（Parquet），可回放可审计。
- **M4 治理与运维**：小文件 compaction、schema 校验与演进、失败重试与回放、可观测与审计完善。

---

## M0：基础可运行

- [x] 本地启动：`docker compose up -d` 可启动 Airflow/MinIO/Postgres（完成健康检查）
- [x] Airflow 连接：Web UI 可访问（`localhost:8080`），scheduler 正常心跳
- [x] MinIO 可用：控制台可访问（`localhost:9001`），bucket/凭证可用
- [x] Airflow 连接串：`AIRFLOW_CONN_MINIO_S3` 可用（能 list/read/write）
- [x] Extractor 验证：手动触发任一 extractor DAG，确认 CSV 成功写入 MinIO

---

## M1：ODS Loader（优先级最高）

### 1.1 约定（保持简单、强约束）

- [x] 配置文件：`dags/ods/config.yaml`
- [x] SQL 约定：`dags/ods/{dest}.sql`
- [x] 分区参数：`PARTITION_DATE`（`YYYY-MM-DD`）
- [ ] ODS 产物路径：`dw/ods/{dest}/dt=YYYY-MM-DD/*.parquet`（实现落盘）
- [ ] 完成标记：`dw/ods/{dest}/dt=YYYY-MM-DD/_SUCCESS` 或 `manifest.json`（实现写入）
- [ ] 幂等策略：同一 `(dest, dt)` 单写者 + 重跑覆盖/切换（实现并验证）

### 1.2 通用工具（ODS Loader 会依赖）

- [ ] `dags/utils/duckdb_utils.py`：DuckDB 连接、`httpfs`、MinIO/S3 配置、执行 SQL、`COPY` 写 Parquet
- [ ] `dags/utils/sql_utils.py`：读取 `.sql`、变量替换（`${PARTITION_DATE}`）、基础校验（例如要求产出 `dt`）
- [ ] `dags/utils/partition_utils.py`：生成分区路径、写 `_SUCCESS/manifest`、统计文件/行数（可选）
- [ ] `dags/utils/catalog_client.py`（可选）：写入/更新 catalog（runs/partitions/tables）

### 1.3 ODS Loader DAG：`dags/ods_loader_dag.py`

- [ ] 从 `dags/ods/config.yaml` 读取表清单并动态生成任务
- [ ] 每表 TaskGroup：prepare → load → mark_success →（可选）catalog_upsert
- [ ] load：DuckDB 执行（注册源 → 跑 SQL → `COPY ... PARTITION_BY (dt)` 写 Parquet）
- [ ] 端到端验证：跑通 `ods_daily_stock_price_akshare` 的一个 `dt`

---

## M1.5：Catalog（Postgres schema，轻量元数据）

> 目标：让下游依赖从“扫目录”变成“查 catalog + 校验 `_SUCCESS`”，并提供审计/追溯能力。

- [ ] DDL：创建 `catalog` schema 与最小表结构
  - [ ] `catalog.tables(table_name, layer, partition_col, schema_hash, owner, created_at)`
  - [ ] `catalog.partitions(table_name, dt, location, file_count, row_count, status, run_id, produced_at)`
  - [ ] `catalog.runs(run_id, dag_id, task_id, partition_date, started_at, ended_at, status, error)`
- [ ] 约束：`(table_name, dt)` 唯一键
- [ ] 写入：ODS Loader 在成功后写 `runs` + upsert `partitions`
- [ ] 读取：下游读取前按 `(table_name, dt)` 查 `status='success'` + 校验 `_SUCCESS`

---

## M2：DWD / DIM 打通

### 2.1 统一渲染 DAG：`dw_dags.py`

- [ ] 配置入口：新增 `dags/dw/config.yaml`（或 `dags/dwd/config.yaml` + `dags/ads/config.yaml`）
- [ ] 每条配置字段：`layer, dest, sql, depends_on, partitioned, output_path`
- [ ] 依赖判定：等待上游 `_SUCCESS` 或查询 catalog（优先）
- [ ] 执行流程：注册上游逻辑表 → 执行 SQL → 写目标层 Parquet → 完成标记 → 更新 catalog

### 2.2 DIM 分区策略落地

- [ ] DIM 默认不分区：`dw/dim/{table}/*.parquet`
- [ ] DIM 版本化（可选）：`as_of=YYYY-MM-DD` 或 `version=...`

### 2.3 端到端验证

- [ ] 增加至少 1 张 DWD 表示例（SQL + 配置）
- [ ] 从 ODS → DWD 跑通一个 `dt`

---

## M3：ADS / Metrics 打通

- [ ] 指标层配置与 SQL 约定（例如 `dags/ads/config.yaml` + `dags/ads/{metric}.sql`）
- [ ] ADS 产物路径：`dw/ads/{metric_or_table}/dt=YYYY-MM-DD/*.parquet`
- [ ] 写入方式：DuckDB `COPY ... PARTITION_BY (dt)`（按 `PARTITION_DATE` 产出）
- [ ] 幂等与标记：同一 `(metric, dt)` 单写者 + 重跑覆盖 + `_SUCCESS/manifest`
- [ ] catalog 记录（如启用）：成功后 upsert `catalog.partitions`（layer=`ads`）
- [ ] 增加至少 1 个指标示例并跑通（日分区产出）

---

## M4：治理与运维

- [ ] Compaction：按表/按月合并小文件（独立 DAG 或按需触发）
- [ ] Schema 声明：每表提供 schema（YAML/DDL）
- [ ] Schema 校验：写入前校验缺列/类型变化（报警或阻断策略）
- [ ] 轻量数据质量：行数阈值波动、主键重复、空值比例（可选）
- [ ] 可观测：结构化日志输出（table/dt/run_id/location/file_count/row_count）
