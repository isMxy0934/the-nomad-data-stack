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
- **M1 ODS 打通**：ODS Loader 能从 MinIO 读原始数据（CSV）→ DuckDB 执行 SQL → 写 ODS 分区 Parquet → 写 `_SUCCESS` 完成标记。
- **M2 DWD/DIM 打通**：读取 ODS（逻辑表）→ 写 DWD/DIM 产物（分区/非分区策略清晰）→ 完成标记。
- **M3 ADS/Metrics 打通**：指标 SQL 驱动，按日运行并**物化落盘**到 ADS 分区（Parquet），可回放可审计。
- **M4 治理与运维**：小文件 compaction、schema 校验与演进、失败重试与回放、可观测与审计完善。

---

## M0：基础可运行

- [x] 本地启动：`docker compose up -d` 可启动 Airflow/MinIO/Postgres（完成健康检查）
- [x] Airflow 连接：Web UI 可访问（`localhost:8080`），scheduler 正常心跳
- [x] MinIO 可用：控制台可访问（`localhost:9001`），bucket/凭证可用
- [x] Airflow 连接串：`AIRFLOW_CONN_MINIO_S3` 可用（能 list/read/write）
- [x] Extractor 验证：手动触发任一 extractor DAG，确认 CSV 成功写入 MinIO
- [x] `make smoke`：集成验收脚本（docker compose up → 触发 extractor → 校验 CSV 成功写入 MinIO）

---

## M1：ODS Loader（优先级最高）

### 验收口径（Definition of Done）
- [ ] 指定 `dest + dt` 跑一次，产出 Parquet + `manifest.json`（可选 `_SUCCESS`）
- [ ] 下游能按精确分区路径读取，不触发全量列举（避免 S3 ListObjectsV2 成本）
- [ ] 重跑同一 `dt` 不会混入旧文件（commit protocol 通过）
- [ ] 结构化日志输出：`table/dt/run_id/location/file_count/row_count/status`
- [ ] `make smoke`：集成验收脚本验证完整链路

### 执行顺序（按优先级，按步骤验收）

> 原则：一次只做一小步、可验收、可回滚；每一步都要有最小验证（单测或脚本）。

#### P0：锁定约定（先写清楚，再写代码）

- [x] 配置文件：`dags/ods/config.yaml`
- [x] SQL 约定：`dags/ods/{dest}.sql`
- [x] 分区参数：`PARTITION_DATE`（`YYYY-MM-DD`）
- [ ] ODS 产物路径：`dw/ods/{dest}/dt=YYYY-MM-DD/*.parquet`（落盘）
- [ ] 完成标记：`dw/ods/{dest}/dt=YYYY-MM-DD/manifest.json`（可选 `_SUCCESS`）
- [ ] 幂等策略：同一 `(dest, dt)` 单写者 + 重跑覆盖（publish 前清空 canonical 分区）

#### P1：`sql_utils`（最小工具 + 单元测试）

- [ ] `dags/utils/sql_utils.py`：读取 `.sql`、变量替换（`${PARTITION_DATE}`）、基础校验（例如禁止空 SQL、缺参报错）
- [ ] `tests/utils/test_sql_utils.py`：覆盖变量渲染与错误分支（缺参/空 SQL）

验收：
- [ ] 本地运行单测通过（不依赖 Airflow/MinIO）

#### P2：`partition_utils`（commit protocol 的语义落地 + 单元测试）

> commit protocol 是 M1 的核心：禁止直接写 canonical；必须 tmp → validate → publish → 标记完成。

- [ ] `dags/utils/partition_utils.py`：生成 tmp/canonical 前缀、生成 manifest、写入完成标记
- [ ] `dags/utils/partition_utils.py`：publish 实现（清空 canonical dt 前缀 → copy tmp dt 前缀 → 写 manifest.json → 可选写 `_SUCCESS`）
- [ ] `tests/utils/test_partition_utils.py`：覆盖路径生成与 manifest 字段（不依赖真实 S3）

验收：
- [ ] 单测通过，且路径严格符合 `dt=YYYY-MM-DD`

#### P3：`duckdb_utils`（DuckDB 临时计算 + S3/MinIO 读写能力）

- [ ] `dags/utils/duckdb_utils.py`：创建临时 DuckDB 连接（禁止持久化共享 DB）
- [ ] `dags/utils/duckdb_utils.py`：启用 `httpfs` 并配置 MinIO/S3（endpoint、AK/SK、path style）
- [ ] `dags/utils/duckdb_utils.py`：执行 SQL、`COPY ... PARTITION_BY (dt)` 写 Parquet 到 tmp 前缀

验收：
- [ ] 至少有一个最小验证（单测或脚本）：能执行 `SELECT 1`，且 COPY 写入路径参数校验通过

#### P4：ODS Loader DAG（拼装层）

- [ ] `dags/ods_loader_dag.py`：从 `dags/ods/config.yaml` 读取表清单并动态生成任务
- [ ] `dags/ods_loader_dag.py`：每表 TaskGroup：prepare → load → validate → commit → cleanup
- [ ] `dags/ods_loader_dag.py`：load 使用 DuckDB 写 tmp 前缀（`COPY ... PARTITION_BY (dt)`）
- [ ] `dags/ods_loader_dag.py`：commit 调用 `partition_utils` publish + 写完成标记
- [ ] 单写者：为每张表创建 Airflow pool（slots=1），写分区任务必须指定该 pool

验收：
- [ ] 端到端跑通 `ods_daily_stock_price_akshare` 的一个 `dt`

#### P5：端到端 smoke（脚本化验收）

- [ ] `make smoke`（或 `scripts/smoke_m1.sh`）：extractor → ods_loader → 校验 ODS 分区产物与完成标记
- [ ] 校验项：parquet 存在 + 完成标记存在 + 支持精确分区路径读取

#### P6：schema（可延后，非阻塞 M1 最小闭环）

- [ ] `dags/utils/schema_utils.py`：表 schema 定义、类型校验（先从 1 张表开始）

---

---

## M2：DWD / DIM 打通

### 2.1 统一渲染 DAG：`dw_dags.py`

- [ ] 配置入口：新增 `dags/dw/config.yaml`（或 `dags/dwd/config.yaml` + `dags/ads/config.yaml`）
- [ ] 每条配置字段：`layer, dest, sql, depends_on, partitioned, output_path`
- [ ] 依赖判定：等待上游 `_SUCCESS` 文件存在
- [ ] 执行流程：注册上游逻辑表 → 执行 SQL → 写目标层 Parquet → 完成标记

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
- [ ] 增加至少 1 个指标示例并跑通（日分区产出）

---

## M4：治理与运维

- [ ] Compaction：按表/按月合并小文件（独立 DAG 或按需触发）
- [ ] Schema 声明：每表提供 schema（YAML/DDL）
- [ ] Schema 校验：写入前校验缺列/类型变化（报警或阻断策略）
- [ ] 轻量数据质量：行数阈值波动、主键重复、空值比例（可选）
- [ ] 可观测增强：监控面板、告警规则、性能指标收集（基于基础结构化日志）
