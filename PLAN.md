# 开发计划与进度追踪（the-nomad-data-stack）

本文件用于追踪项目进度就把 `- [ ]` 勾成 `- [x]` 代表已经完成。

---

## 当前状态（基于仓库已有内容）

- [x] 文档：`README.md` 已瘦身（用户向），细节下沉到 `docs/`
- [x] 计划：`PLAN.md` 已建立并可勾选追踪
- [x] 基础设施：`docker-compose.yml` 已包含 Airflow + Postgres + MinIO
- [x] Extractor DAG：`dags/extractor/*` 已存在（AkShare/TuShare → MinIO CSV）
- [ ] Extractor Backfill/Compact：新增 `dw_extractor_backfill_dag` / `dw_extractor_compact_dag`（历史回填 pieces → 合并到 daily）
- [x] ODS 配置与示例 SQL：`dags/dw_config.yaml` (sources)、`dags/ods/*.sql` 已存在
- [x] 工具类：`dags/utils/s3_utils.py`、`dags/utils/time_utils.py` 已存在

> 说明：以上是“文件/结构已存在”，不等价于“已跑通验证”。跑通验证会在对应任务项里单独勾选。

---

## 里程碑

- **M0 基础可运行**：本地 `docker compose up` 后 Airflow/MinIO/Postgres 可用，Extractor DAG 可写入 MinIO。
- **M1 ODS 打通**：ODS Loader 能从 MinIO 读原始数据（CSV）→ DuckDB 执行 SQL → 写 ODS 分区 Parquet → 写 `_SUCCESS` 完成标记。
- **M2 DW 打通（配置驱动）**：基于 `dags/dw_config.yaml` + `dags/{layer}/*.sql`（目录即配置）生成每层独立 DAG（`dw_{layer}`）；空层（无 SQL/目录不存在）跳过；任务运行时只读 attach DuckDB catalog 以支持 `SELECT * FROM ods.xxx`；像 ODS 一样执行（DuckDB 计算 → tmp 写入 → validate → publish → `_SUCCESS` → cleanup）。
- **M3 治理与运维**：小文件 compaction、schema 校验与演进、失败重试与回放、可观测与审计完善。

---

## M0：基础可运行

- [x] 本地启动：`docker compose up -d` 可启动 Airflow/MinIO/Postgres（完成健康检查）
- [x] Airflow 连接：Web UI 可访问（`localhost:8080`），scheduler 正常心跳
- [x] MinIO 可用：控制台可访问（`localhost:9001`），bucket/凭证可用
- [x] Airflow 连接串：`AIRFLOW_CONN_MINIO_S3` 可用（能 list/read/write）
- [x] Extractor 验证：手动触发任一 extractor DAG，确认 CSV 成功写入 MinIO
- [x] `make integration`：集成验收脚本（docker compose up → 触发 extractor → 校验 CSV 成功写入 MinIO）

---

## M1：ODS Loader（优先级最高）

### 验收口径（Definition of Done）
- [x] 指定 `dest + dt` 跑一次，产出 Parquet + `manifest.json`（可选 `_SUCCESS`）
- [x] 下游能按精确分区路径读取，不触发全量列举（避免 S3 ListObjectsV2 成本）
- [x] 重跑同一 `dt` 不会混入旧文件（commit protocol 通过）
- [x] 结构化日志输出：`table/dt/run_id/location/file_count/row_count/status`
- [x] `make integration`：集成验收脚本验证完整链路

### 执行顺序（按优先级，按步骤验收）

> 原则：一次只做一小步、可验收、可回滚；每一步都要有最小验证（单测或脚本）。

#### P0：锁定约定（先写清楚，再写代码）

- [x] 配置文件：`dags/dw_config.yaml` (sources)
- [x] SQL 约定：`dags/ods/{dest}.sql`
- [x] 分区参数：`PARTITION_DATE`（`YYYY-MM-DD`）
- [x] ODS 产物路径：`lake/ods/{dest}/dt=YYYY-MM-DD/*.parquet`（落盘）
- [x] 完成标记：`lake/ods/{dest}/dt=YYYY-MM-DD/manifest.json`（可选 `_SUCCESS`）
- [x] 幂等策略：同一 `(dest, dt)` 重跑覆盖（publish 前清空 canonical 分区）

#### P1：`sql_utils`（最小工具 + 单元测试）

- [x] `dags/utils/sql_utils.py`：读取 `.sql`、变量替换（`${PARTITION_DATE}`）、基础校验（例如禁止空 SQL、缺参报错）
- [x] `tests/utils/test_sql_utils.py`：覆盖变量渲染与错误分支（缺参/空 SQL）

验收：
- [x] 本地运行单测通过（不依赖 Airflow/MinIO）

#### P2：`partition_utils`（commit protocol 的语义落地 + 单元测试）

> commit protocol 是 M1 的核心：禁止直接写 canonical；必须 tmp → validate → publish → 标记完成。

- [x] `dags/utils/partition_utils.py`：生成 tmp/canonical 前缀、生成 manifest、写入完成标记
- [x] `dags/utils/partition_utils.py`：publish 实现（清空 canonical dt 前缀 → copy tmp dt 前缀 → 写 manifest.json → 可选写 `_SUCCESS`）
- [x] `tests/utils/test_partition_utils.py`：覆盖路径生成与 manifest 字段（不依赖真实 S3）

验收：
- [x] 单测通过，且路径严格符合 `dt=YYYY-MM-DD`

#### P3：`duckdb_utils`（DuckDB 临时计算 + S3/MinIO 读写能力）

- [x] `dags/utils/duckdb_utils.py`：创建临时 DuckDB 连接（禁止持久化共享 DB）
- [x] `dags/utils/duckdb_utils.py`：启用 `httpfs` 并配置 MinIO/S3（endpoint、AK/SK、path style）
- [x] `dags/utils/duckdb_utils.py`：执行 SQL、`COPY ... PARTITION_BY (dt)` 写 Parquet 到 tmp 前缀

验收：
- [x] 至少有一个最小验证（单测或脚本）：能执行 `SELECT 1`，且 COPY 写入路径参数校验通过

#### P4：ODS Loader DAG（拼装层）


验收：
- [x] 端到端跑通 `ods_daily_stock_price_akshare` 的一个 `dt`

#### P5：端到端 integration test （脚本化验收）

- [x] `make integration`：extractor → ods_loader → 校验 ODS 分区产物与完成标记
- [x] 校验项：parquet 存在 + 完成标记存在 + 支持精确分区路径读取

#### P6：schema（可延后，非阻塞 M1 最小闭环）

- [x] `dags/utils/schema_utils.py`：表 schema 定义、类型校验（先从 1 张表开始）

#### P7：DuckDB 分析 Catalog

> 目标：分析时不需要手写 `read_parquet('s3://...')`，可以直接 `SELECT * FROM ods.xxx`。

- [x] `scripts/duckdb_catalog_refresh.py`：扫描 `lake/ods`（或 `lake/_integration/ods`）并创建/更新 `ods.*` 视图与 `ods.<table>_dt()` 宏
- [x] `scripts/validate_duckdb_catalog.py`：本地验证脚本（不依赖 MinIO）
- [x] 单元测试：`tests/utils/test_catalog_utils.py` 覆盖 SQL 生成与宏语法
- [x] 迁移机制：`catalog/migrations/*.sql` + `catalog_meta.schema_migrations`（用于“基础 schema / 公共 macro”等演进）
- [x] `scripts/duckdb_catalog_migrate.py` + `scripts/validate_duckdb_catalog_migrations.py`
- [x] 单元测试：`tests/utils/test_catalog_migrations.py`

---

## M2：DW 打通（配置驱动：DWD/DIM/DWM/DWS/ADS）

### 2.0 约定（先写清楚，再写代码）

- [x] 配置文件：`dags/dw_config.yaml`（层依赖 + 可选同层表依赖）
- [x] 目录即配置：按 layer 扫描 `dags/{layer}/*.sql`；目录不存在或无 SQL 文件则跳过该层（不生成空层占位）
- [x] SQL 约定：`dags/{layer}/{table}.sql`（表名强约束带 layer 前缀，例如 `dwd_daily_stock_price`）
- [x] 分区参数：`${PARTITION_DATE}`（`YYYY-MM-DD`，与 ODS 一致）
- [x] 产物路径：
  - 分区表：`lake/{layer}/{table}/dt=YYYY-MM-DD/*.parquet`（遵循 commit protocol）
  - 非分区表（DIM full / 快照类）：`lake/{layer}/{table}/*.parquet`（同样走 tmp → promote，禁止原地覆盖）
- [x] DAG 生成：每层生成一个独立 DAG：`dw_{layer}`（例如 `dw_dwd`、`dw_ads`）
- [x] 完成标记：写入 `_SUCCESS`（核心约定）；无数据视为成功但不写 `_SUCCESS`，只记录日志

### 2.1 解析与依赖建模（核心：可测试）

- [x] `dw_config.yaml` 解析：读取层依赖/同层表依赖，并做严格校验（未知 layer、循环依赖等）
- [x] 目录扫描：按约定扫描 `dags/{layer}/*.sql`，产出“待执行表清单”（layer/table/sql_path）
- [x] 命名校验：`{table}` 必须以 `{layer}_` 开头；否则报错（避免 catalog/落盘命名漂移）
- [x] 依赖建模：同层表依赖按 `table_dependencies` 做拓扑排序；跨层顺序由编排（按 `layer_dependencies`）保证
- [x] 单元测试：覆盖解析、扫描、拓扑排序与错误分支

### 2.2 DW 执行与提交（复用 M1 组件）

- [x] 统一执行函数：复用 `duckdb_utils` + `partition_utils`（tmp 写入 → validate → publish → 标记完成 → cleanup）
- [x] 分区表写入：DuckDB `COPY ... PARTITION_BY (dt)`，dt 由 `${PARTITION_DATE}` 注入
- [x] 非分区表写入：仍按“临时前缀 → promote 到 canonical”落地（避免读到中间态）
- [x] 校验口径：至少文件数/行数与路径一致性校验（对齐 ODS）
- [x] Catalog 接入：任务运行时只读 attach `./.duckdb/catalog.duckdb`，SQL 只引用 `ods.*`/上游层逻辑表（不硬编码 S3 路径）

### 2.3 Airflow DAG 生成：`dags/dw_dags.py`

- [x] 逐层生成 DAG：仅当该层发现 `*.sql` 才生成 `dw_{layer}` DAG（空层跳过）
- [x] 层内按目录扫描结果创建 tasks（可选用 TaskGroup 按表分组）
- [x] 与 ODS 对齐的流程：prepare → load → validate → commit → cleanup

### 2.4 端到端验证（最小闭环）

- [x] 增加至少 1 个 DWD 表（SQL + 目录约定）并从 ODS 跑通一个 `dt`
- [x] Demo 表：`dwd.dwd_daily_stock_price`（AkShare/TuShare 合并；按 `symbol, trade_date` 粒度，产出 `open_avg/close_avg`）

---

## M3：治理与运维

- [ ] Compaction：按表/按月合并小文件（独立 DAG 或按需触发）
- [ ] Schema 声明：每表提供 schema（YAML/DDL）
- [ ] Schema 校验：写入前校验缺列/类型变化（报警或阻断策略）
- [ ] 轻量数据质量：行数阈值波动、主键重复、空值比例（可选）
- [ ] 可观测增强：监控面板、告警规则、性能指标收集（基于基础结构化日志）
