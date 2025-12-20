## lakehouse_core 分层重构（可执行 Checklist）

目标：建立一个稳定的 `lakehouse_core` 核心层，使 **调度层（Airflow/Prefect/脚本）** 与 **存储层（S3/本地/其他）** 都通过适配器接入，核心逻辑不再与任何具体调度器/存储协议绑定。

本仓库强约束（重构期间必须保持不变）：
- 分区时间默认 T-1：`dags/utils/time_utils.py#get_partition_date_str()` 不重构为 Airflow `{{ ds }}`。
- Commit Protocol（非原子）：`delete_prefix -> copy_prefix` 顺序不变（见 `docs/commit-protocol.md`）。
- 分区命名：`dt=YYYY-MM-DD` 不变，完成标记 `manifest.json` + `_SUCCESS` 语义不变。
- Catalog：`.duckdb/catalog.duckdb` 为 metadata-only，单写者约束保留（并发控制属于调度层职责）。

---

### 0. 术语与边界（必须先对齐）

#### 0.1 分层
- **Core（`lakehouse_core`）**：业务规则/协议/状态机；不得 `import airflow`，不得使用 `S3Hook`，不得读 Airflow context/XCom。
- **Adapters**：把 core 需要的抽象能力落地到具体实现（S3/本地/凭证）；可依赖 Airflow 或 boto3 等。
- **Orchestrators**：编排（Airflow DAG/Prefect flow/脚本）；只负责取参、并发/重试、可观测性，调用 core 用例函数。

#### 0.2 Planner / RunSpec（表定义与执行解耦）

重构目标之一是让 core **不关心“表怎么定义/怎么解析出来”**，只关心“给定一份可执行规格，如何按协议产出并提交”。

- **Planner（计划适配器）**：把“用户的表定义体系”转换为统一的执行规格列表。
  - 输入：配置/目录/元数据/用户自定义规则 + `RunContext`
  - 输出：`list[RunSpec]`
- **RunSpec（执行规格）**：core 执行用的最小信息集合，建议包含：
  - `name`（逻辑名，如 `ods.ods_daily_fund_price_akshare`）
  - `partition_date`、`is_partitioned`
  - `sql`（或可执行 query/plan）
  - `paths`（tmp/canonical/manifest/_SUCCESS；通常由 core 按约定生成，也可由 planner 提供 base_prefix + flags 让 core 生成）
  - `inputs`（可选：需要注册哪些 source view、需要哪些上游前缀等；由 orchestrator/adapter 实现注册）

第一阶段默认 Planner 可以继续沿用当前“目录即配置”（`dags/dw_config.yaml` + `dags/{layer}/*.sql`），但应将其视为“默认 planner”，而不是 core 的固定依赖。

#### 0.2 Phase 划分
- **Phase 1（本阶段重点）**：调度与存储解耦（核心逻辑从 Airflow/S3Hook 脱离）。
- **Phase 2（后续）**：执行（DuckDB）下沉 + 脚本入口（为 Prefect/脚本调度铺路）。

---

## Phase 0：基线锁定（必须）

- [x] `uv run python -m pytest tests/ -v` 通过，作为重构前基线。
- [ ] 记录当前关键入口与依赖：
  - `dags/dw_start_dag.py`（日常入口）
  - `dags/dw_init_dag.py`（回填入口）
  - `dags/dw_catalog_dag.py`（catalog migrations）
  - `dags/dw_dags.py`（DW 分层 DAG 动态生成）
  - `lakehouse_core/commit.py` + `dags/adapters/airflow_s3_store.py` + `dags/utils/etl_utils.py`（commit protocol）
  - `dags/utils/etl_utils.py`（prepare/validate/commit/cleanup）
  - `dags/utils/s3_utils.py`、`dags/utils/duckdb_utils.py`

验收标准：
- [x] 重构开始前测试全绿（否则先修复/隔离失败用例，避免把问题带入重构）。

---

## Phase 1：调度与存储解耦（Airflow 仍保留）

### 1.1 新增目录与包骨架

- [x] 新增包：`lakehouse_core/`
  - [x] `lakehouse_core/__init__.py`
  - [x] `lakehouse_core/models.py`：`RunContext`、`RunSpec` 等核心数据结构（建议 dataclass）
  - [x] `lakehouse_core/planning.py`：`Planner` 协议（可选，但建议先定接口，避免后续反复改）
  - [x] `lakehouse_core/storage.py`：定义 `ObjectStore`（Protocol/ABC）
  - [x] `lakehouse_core/paths.py`：路径规划（canonical/tmp/manifest/_SUCCESS）
  - [x] `lakehouse_core/manifest.py`：manifest 结构与构建
  - [x] `lakehouse_core/commit.py`：publish（delete+copy）与完成标记
  - [x] `lakehouse_core/validate.py`：校验（file_count/row_count/存在性）
  - [x] `lakehouse_core/api.py`：统一入口（prepare/validate/publish/cleanup）
  - [x] `lakehouse_core/errors.py`：统一异常类型（可选，但建议）

验收标准：
- [x] 在 `lakehouse_core/` 目录执行 `rg -n \"\\bairflow\\b|S3Hook|dag_run|xcom\" lakehouse_core` 无命中。

### 1.1.1 动态 DAG 生成如何处理（明确边界）

- [x] `dags/dw_dags.py` 这类“动态生成 DAG/TaskGroup/expand/TriggerDagRun”的逻辑属于 orchestrator 层，**第一阶段继续保留在 `dags/`**。
- [x] 第一阶段的改造重点是：把这些 DAG 里每个 task 最终调用的“业务动作”替换为 core 用例函数（通过 adapter 注入 `ObjectStore`），而不是迁移 Airflow 的 DSL。
- 后续若迁 Prefect/脚本：可重用同一个 Planner 产出的 `RunSpec` 列表，用不同 orchestrator 将其映射为任务图（而不要求复刻 Airflow UI 结构）。

### 1.2 定义核心存储抽象：`ObjectStore`

Checklist：
- [x] `ObjectStore` 最小接口（建议）：
  - [x] `list_keys(prefix: str) -> list[str]`
  - [x] `exists(key: str) -> bool`
  - [x] `read_bytes(key: str) -> bytes`
  - [x] `write_bytes(key: str, data: bytes) -> None`
  - [x] `delete_prefix(prefix: str) -> None`
  - [x] `copy_prefix(src_prefix: str, dst_prefix: str) -> None`
- [x] 约定 key 形式：
  - [x] 统一使用“URI 或 bucket/key”其中一种（建议沿用现有 `s3://bucket/prefix` URI 形式，减少改动）。
  - [x] 在 core 中集中解析与规范化，避免散落在各处。

验收标准：
- [x] Phase 1 的 core 逻辑只依赖 `ObjectStore`，不直接依赖 S3/本地 API。

### 1.2.1 定义 core 的最小用例函数边界（建议写死在文档里）

> 目的：让 orchestrator 只做“编排”，所有关键语义都集中在 core，且对未来调度器迁移友好。

- [x] `prepare_paths(base_prefix, run_id, partition_date, is_partitioned, store_namespace=...) -> Paths`
- [x] `validate_output(store, paths, metrics) -> metrics`
- [x] `publish_output(store, paths, manifest, write_success_flag=True) -> result`
- [x] `cleanup_tmp(store, paths) -> None`

说明：
- Phase 1 允许 “DuckDB 执行 + 产出 metrics” 仍由 DAG 层函数完成（如当前 `dags/dw_dags.py#load_table`），但其输出 metrics 必须能被 core 的 validate/publish 接收。

### 1.3 提供两个存储适配器（一个给测试，一个给 Airflow）

#### 1.3.1 本地适配器（用于 core 单测）
- [x] 新增：`lakehouse_core/testing/local_store.py`（或 `tests/helpers/local_store.py`）
  - [x] 用临时目录模拟 `lake/...` 前缀
  - [x] 支持前缀删除、前缀拷贝、写入 bytes、列举 keys

#### 1.3.2 Airflow S3 适配器（用于现有 DAG，不改变行为）
- [x] 新增：`dags/adapters/airflow_s3_store.py`
  - [x] 内部使用 `airflow.providers.amazon.aws.hooks.s3.S3Hook`
  - [x] 将 `S3Hook` 的 list/get/put/copy/delete 映射到 `ObjectStore`
  - [x] 读取连接（endpoint/ak/sk 等）仍通过 Airflow Connection（adapter 责任）

验收标准：
- [x] DAG 层仍可读写 MinIO/S3（行为与当前一致）。
- [x] core 层完全不 import Airflow。
 

### 1.4 抽离 commit protocol 到 core（最关键）

目标：将与存储提交相关的“规则”迁到 `lakehouse_core`，并在 Airflow 层通过 `AirflowS3Store` 直接调用 core（不再保留 `dags/utils/partition_utils.py`）。

Checklist（按顺序做，避免大爆炸）：
- [x] 在 `lakehouse_core/paths.py` 复刻并稳定化以下概念：
  - [x] canonical 前缀：`lake/{layer}/{table}/dt=.../`（分区表）或 `lake/{layer}/{table}/`（非分区）
  - [x] tmp 前缀：`.../_tmp/run_{run_id}/...`
  - [x] `manifest.json` 与 `_SUCCESS` 的位置规则
- [x] 在 `lakehouse_core/manifest.py` 提供 `build_manifest(...)`
- [x] 在 `lakehouse_core/commit.py` 提供 publish 函数：
  - [x] 先 `delete_prefix(canonical_partition_prefix)` 再 `copy_prefix(tmp_partition_prefix, canonical_partition_prefix)`
  - [x] 写入 `manifest.json`、`_SUCCESS`
- [x] 在 `lakehouse_core/validate.py` 提供 validate 函数：
  - [x] 有数据：tmp 下 parquet 文件数与 metrics 一致
  - [x] 无数据：确认 tmp 前缀无残留；并按现有语义清理 canonical 分区

Airflow 层改造（最小侵入策略）：
- [x] 移除 `dags/utils/partition_utils.py`；Airflow 层直接使用 `AirflowS3Store` + `lakehouse_core` 用例函数
  - [x] 保持原函数签名一段时间（兼容现有调用）
  - [x] 内部改为：`store = AirflowS3Store(s3_hook)` → 调 `lakehouse_core.*`

验收标准：
- [x] `docs/commit-protocol.md` 描述与实现一致（如有差异，先修文档/实现对齐，再继续拆）。
- [x] 现有 DAG 正常 import/生成（`tests/dags/*` 通过）。

### 1.5 抽离 ETL 通用逻辑到 core（保留 Airflow bridge）

目标：`dags/utils/etl_utils.py` 中的“纯逻辑”下沉到 core，XCom/context 相关逻辑留在 Airflow 层。

Checklist：
- [x] 将以下“纯逻辑”迁到 core：
  - [x] `prepare_dataset` 的路径规划（调用 `lakehouse_core/paths.py`）
  - [x] `validate_dataset`（调用 `lakehouse_core/validate.py`）
  - [x] `commit_dataset`（调用 `lakehouse_core/commit.py`）
  - [x] `cleanup_dataset`（调用 `ObjectStore.delete_prefix`）
- [x] `build_etl_task_group` 继续留在 `dags/utils/etl_utils.py`（Airflow 专属），但其内部调用 core 函数，不再承载业务规则。

验收标准：
- [x] core 单测覆盖：prepare/validate/commit/cleanup（至少覆盖分区表 + 非分区表 + 空数据分支）。
- [x] `dags/utils/etl_utils.py` 仍可用于 DAG（只做桥接）。

### 1.5.1 默认 Planner（可选：Phase 1 先“保留现状”，Phase 2 再迁）

第一阶段建议不动“表发现/依赖解析”的位置（继续在 `dags/utils/dw_config_utils.py` + `dags/dw_dags.py`），但先把接口写清楚，避免未来迁 Prefect/脚本时无从下手：

- [x] 新增 `lakehouse_core/planning.py`，定义：
  - [x] `class Planner(Protocol): def build(self, context: RunContext) -> list[RunSpec]: ...`
- [x] 新增一个默认实现（位置二选一）：
  - [ ] **方案 A**：默认 planner 仍留在 `dags/`（Airflow scope 内），只负责把现有扫描结果转换成 `RunSpec`（不引入 core 对 dags 的依赖）。
  - [x] **方案 B**：把“目录即配置”的 planner 下沉到 `lakehouse_core`（依赖纯文件系统读取，不依赖 Airflow）。
    - [x] `lakehouse_core/dw_planner.py`：`DirectoryDWPlanner`

验收标准：
- [x] orchestrator 只依赖 `Planner`/`RunSpec`（默认实现可替换）；`dags/` 与 `scripts/` 都可复用同一套 planner 输出。

### 1.6 测试策略（Phase 1 必须落地）

新增/调整测试：
- [x] 新增 `tests/lakehouse_core/test_commit_protocol.py`（示例命名）
  - [x] 使用 `LocalStore` 构造 tmp 前缀若干 parquet 文件（可用空 bytes 代表文件存在）
  - [x] 调用 core publish，断言：
    - [x] canonical 下出现目标文件
    - [x] `manifest.json` 内容字段齐全
    - [x] `_SUCCESS` 存在
    - [x] publish 顺序语义可验证（至少能证明 delete 在 copy 前发生；可通过 LocalStore 记录操作日志）
- [x] 现有 DAG import/generation 测试保持通过（`tests/dags/*`）

Phase 1 总验收（必须全部满足）：
- [x] `lakehouse_core/` 无 Airflow 依赖（grep/rg 校验）
- [x] `uv run python -m pytest tests/ -v` 全绿
- [x] 产物路径与完成标记与重构前一致（`dt=...`、`manifest.json`、`_SUCCESS`）

---

## Phase 2：执行下沉（DuckDB）+ 脚本入口（后续规划）

> Phase 2 不在第一阶段实施范围，但建议把目标与验收写清楚，避免接口漂移。

### 2.1 DuckDB 执行抽象（Execution Protocol）

目标：core 可复用地完成 “读上游 → DuckDB SQL → 写 parquet 到 tmp”。

Checklist：
- [x] 定义 `lakehouse_core/execution.py`（或 `lakehouse_core/duckdb/` 子包）：
  - [x] `Executor` 抽象：`run_query_to_parquet(...)`、`run_query_to_partitioned_parquet(...)`
  - [x] 连接生命周期由 core 管理，但 S3 配置由 adapter 注入（避免 core 读 Airflow Connection）
- [x] 将 `dags/utils/duckdb_utils.py` 的可复用执行逻辑迁到 core（保留 Airflow 侧的配置获取）

验收标准：
- [x] Airflow 与脚本（或 Prefect）调用相同的执行用例函数，结果一致。

补充（Phase 2 执行下沉进度）：
- [x] `dags/dw_dags.py#load_table` 的 “query → parquet → (file_count,row_count)” 已收口到 `lakehouse_core.materialize_query_to_tmp_and_measure`，编排层只负责组装参数与上游 view 注册。

### 2.2 脚本入口（可选，但强烈建议）

目标：提供最轻量的运行方式，为后续替换调度器铺路。

Checklist：
- [x] 新增 `scripts/run_dw.py`：
  - [x] `--layer/--table/--dt/--start-date/--end-date/--targets`
  - [x] `--store=local|s3`（选择对应 adapter）
  - [x] 复用 `lakehouse_core.DirectoryDWPlanner` + `lakehouse_core.api`（prepare/validate/publish/cleanup）+ `lakehouse_core.execution`
- [x] 编写最小 end-to-end 验证（pytest smoke）：`tests/scripts/test_run_dw_smoke.py`

验收标准：
- [x] 无 Airflow 的情况下可跑通最小链路（至少 1 张表 1 个 dt；本地 store smoke 覆盖）。

---

## 回滚与风险控制（强制）

- [x] 完成收口与测试后，删除旧入口/过渡代码（以 pytest 全绿作为保护网）。
- [x] 任意阶段失败通过 git 回滚（而非保留双实现并行）来保障可恢复性。
- [ ] 任何涉及路径/完成标记语义变更都必须同步更新 `docs/commit-protocol.md` 与 `README.md`/`PLAN.md`（如适用）。

---

## 💡 改进建议与潜在风险提示（建议写进开发备忘）

> 目的：把容易“实现时分歧/散落”的细节提前固化，确保 core 真正可复用、可迁移、可测试。

### A. URI vs Key 约定（ObjectStore 与 DuckDB 的桥接）

背景：DuckDB 读写通常需要可识别的 URI（如 `s3://bucket/path/file.parquet` 或本地绝对路径），而存储抽象常见形态是 “bucket+key / key-only”。

约定（Phase 1 必须选定其一，禁止混用）：
- [x] **方案 1（推荐）**：`ObjectStore` 全程使用 “URI” 作为资源标识（入参/返回值都是 URI）。Local/S3 都实现对 URI 的解析与操作。
- [ ] **方案 2**：`ObjectStore` 使用 “key” 作为资源标识，但必须提供 `get_uri(key) -> str`（或 `to_uri(...)`）用于 DuckDB/外部系统；禁止在 core 以外零散拼接 `s3://...`。

验收标准：
- [ ] core 中不存在手写拼接 `s3://{bucket}/...` 的散落逻辑；URI 生成集中在 `ObjectStore` 或 `Paths` 单点。

### B. `copy_prefix` 的原子性、性能与幂等性（Commit Protocol）

风险：S3/MinIO 的 prefix copy 是逐对象 CopyObject，非原子、可能很慢；中途失败会产生“半拷贝”结果。

建议（Phase 1 必须明确）：
- [x] publish 语义必须 **可安全重试（幂等）**：重试时允许先清理目标前缀，再执行 copy；不得因为“目标已存在”而失败。
- [x] `AirflowS3Store.copy_prefix` 必须使用 **服务端 copy**（S3 CopyObject / boto3 copy）实现；严禁 download→upload。
- [ ] 如未来文件量大，可在 adapter 内增加并发 copy（受限于单写者/资源）但保持逻辑顺序不变：`delete_prefix` 完成后才开始 copy。

验收标准：
- [ ] publish 过程任意阶段失败后重跑，不会留下不可恢复状态（最多是目标被清空但可再次成功写入）。

### C. 日志与可观测性（Core 仍要“可运维”）

约定：
- [x] `lakehouse_core` 统一使用 Python 标准库 `logging.getLogger(__name__)`。
- [x] Airflow 层负责把标准 logging 输出接入 Airflow task log（通常 Airflow 已处理 root handler；如需额外格式化，在 DAG 镜像/配置侧完成）。
- [x] core 日志字段建议结构化：`dest/table/dt/run_id/file_count/row_count/status`（保持与现有日志约定一致）。

验收标准：
- [x] 在 Airflow UI 的 task log 中能看到 core 的关键日志（无需依赖 `airflow.utils.log`）。

### D. RunSpec 预留连接/存储选项（为 Phase 2 做接口稳定）

背景：Phase 2 下沉 DuckDB 执行时，需要 S3 endpoint/AK/SK/url_style 等配置；这些不应由 core 去读环境变量或 Airflow Connection。

建议：
- [x] 在 `RunSpec` 或 `RunContext` 预留可选字段（不要求 Phase 1 使用，但要定好形状）：
  - [x] `storage_options: dict[str, object] | None`（由 adapter 注入，core 透传给执行层/duckdb 配置）
  - [ ] 或拆分为 `io_config`/`credentials_ref`（如果你们后续要做更强的 secrets 管理）

验收标准：
- [ ] Phase 2 实施时，不需要改动 Phase 1 的 core 函数签名（最多是启用已有可选字段）。

### E. LocalStore（测试替身）必须模拟 S3 prefix 行为

风险：本地文件系统有“目录”概念，而 S3 prefix 是字符串匹配；如果 LocalStore 行为不一致，单测会给出虚假的安全感。

建议（Phase 1 必须写清楚并在实现中遵守）：
- [x] `list_keys(prefix)` 语义：按“字符串前缀匹配”返回所有对象 keys（递归），不引入目录层级推断。
- [x] 统一规范化：prefix 是否补 `/`、是否允许空 prefix、返回 keys 是否包含 leading `/`，都要在实现里固定并写测试覆盖。
- [x] 覆盖边界用例：`prefix="lake/a"` 与 `prefix="lake/a/"` 的行为一致性。

验收标准：
- [x] 同一组对象 keys 下，LocalStore 与 S3Store 对 `list_keys/delete_prefix/copy_prefix` 的行为一致（除性能差异外）。
