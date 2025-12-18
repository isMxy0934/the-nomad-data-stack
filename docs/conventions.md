## 约定与目录结构（the-nomad-data-stack）

这份文档描述“约定优于配置”的规则：目录结构即配置，减少额外参数与运行时分歧。

### 分层与数据流

- Extractor：写入 RAW（CSV/原始格式）到 MinIO
- ODS：按表 SQL 做轻标准化，落 Parquet 分区
- DW（DWD/DIM/DWM/DWS/ADS）：按层 SQL 读取上游逻辑表，落 Parquet（分区或非分区）

Extractor DAG 当前为单一 DAG：`dags/dw_extractor.py`（`dw_extractor`），按 `dags/extractor/config.yaml` 生成多个 TaskGroup（例如 fund_price_akshare / fund_price_tushare）。

### 分区与路径

- 分区列：`dt=YYYY-MM-DD`（Hive 风格）
- 建议路径（canonical）：
  - RAW：`lake/raw/{domain}/dt=YYYY-MM-DD/*.csv`
  - ODS：`lake/ods/{table}/dt=YYYY-MM-DD/*.parquet`
  - DW：`lake/{layer}/{table}/dt=YYYY-MM-DD/*.parquet`
- 临时写入（tmp）：
  - `lake/{layer}/{table}/_tmp/run_{run_id}/dt=YYYY-MM-DD/*.parquet`

### SQL 与模板变量

- SQL 文件按目录发现（“目录即配置”）：
  - ODS：`dags/ods/{dest}.sql`
  - DW：`dags/{layer}/{table}.sql`
- 模板变量：
  - `${PARTITION_DATE}`：`YYYY-MM-DD`（分区日期）
- 分区表判断规则：
  - SQL 中包含 `${PARTITION_DATE}` 视为分区表（需要写 `dt`）
  - 不包含则视为非分区表（全量/快照类），仍走 tmp→publish（禁止原地覆盖）

### 配置文件

- `dags/dw_config.yaml`
  - `layer_dependencies`：层依赖（决定 `dw_{layer}` 的触发顺序）
  - `table_dependencies`：同层表依赖（同一个 `dw_{layer}` 内部做拓扑排序）
  - `sources`：ODS seeds（ODS 表名 → RAW 源前缀与格式）

### 命名规则

- DW 层 SQL 文件名必须带 layer 前缀：
  - 例如：`dags/dwd/dwd_daily_stock_price.sql` → 逻辑表 `dwd.dwd_daily_stock_price`
