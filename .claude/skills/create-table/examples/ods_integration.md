# ODS 层接入完整示例

## 场景说明

我们需要将一个新的数据源 `fund_etf_spot`（ETF 现货数据）接入到 ODS 层。

**前提条件**：
- 数据已经在 `lake/raw/daily/fund_etf_spot` 目录
- 数据格式为 CSV
- 字段包括：代码、名称、成交额

---

## 步骤 1：配置数据源

编辑 `dags/dw_config.yaml`，添加到 `sources` 部分：

```yaml
sources:
  ods_fund_etf_spot:
    path: "lake/raw/daily/fund_etf_spot"
    format: "csv"
```

**说明**：
- `ods_fund_etf_spot`：配置名称
- `path`：原始数据路径
- `format`：数据格式（csv）

---

## 步骤 2：创建 ODS SQL 文件

创建文件：`dags/ods/ods_fund_etf_spot.sql`

```sql
SELECT
  symbol,
  name,
  amount,
  CAST(STRPTIME(CAST(trade_date AS VARCHAR), '%Y%m%d') AS DATE) AS trade_date,
  '${PARTITION_DATE}' AS dt
FROM tmp_ods_fund_etf_spot;
```

**说明**：
- 文件名必须包含 `ods_` 前缀
- 从 `tmp_ods_fund_etf_spot` 读取（Airflow 自动创建临时表）
- 字段映射：symbol（代码）、name（名称）、amount（成交额）
- 日期转换：`STRPTIME` 将字符串（YYYYMMDD）转换为 DATE
- 添加分区列：`${PARTITION_DATE}`

---

## 步骤 3：创建 Migration 文件

查找下一个可用的 migration 编号：

```bash
ls catalog/migrations/
# 输出: 0001_schemas.sql  0002_fund_etf_spot_*.sql  ...
```

下一个编号是 `0007`。

创建文件：`catalog/migrations/0007_ods_fund_etf_spot_table.sql`

```sql
-- Schema Macro
CREATE OR REPLACE MACRO ods._schema_fund_etf_spot() AS
MAP {
    0: {name: 'symbol', type: 'VARCHAR', default_value: NULL},
    1: {name: 'name', type: 'VARCHAR', default_value: NULL},
    2: {name: 'amount', type: 'DOUBLE', default_value: NULL},
    3: {name: 'trade_date', type: 'DATE', default_value: NULL}
};

-- Seed Parquet
COPY (
    SELECT
        CAST(NULL AS VARCHAR) AS symbol,
        CAST(NULL AS VARCHAR) AS name,
        CAST(NULL AS DOUBLE) AS amount,
        CAST(NULL AS DATE) AS trade_date
    WHERE 1=0
) TO 's3://stock-data/lake/ods/ods_fund_etf_spot/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- View
CREATE OR REPLACE VIEW ods.ods_fund_etf_spot AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/ods/ods_fund_etf_spot/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Date Macro
CREATE OR REPLACE MACRO ods.ods_fund_etf_spot_dt(p_date) AS TABLE
SELECT * FROM ods.ods_fund_etf_spot
WHERE dt = p_date;
```

**说明**：
- 定义表结构（4 个字段）
- 创建空 seed 文件
- 创建查询视图
- 创建日期查询宏

---

## 步骤 4：验证和运行

### 1. 检查文件结构

```bash
# 确认配置文件
cat dags/dw_config.yaml | grep fund_etf_spot

# 确认 SQL 文件
cat dags/ods/ods_fund_etf_spot.sql

# 确认 migration 文件
ls catalog/migrations/0007_ods_fund_etf_spot_table.sql
```

### 2. 运行 DAG

```bash
# 在 Airflow UI 中运行 dw_ods DAG
# 或使用命令行
airflow dags trigger dw_ods
```

### 3. 验证数据

```sql
-- 查询新表
SELECT * FROM ods.ods_fund_etf_spot LIMIT 10;

-- 查看特定日期
SELECT * FROM ods.ods_fund_etf_spot_dt('2024-01-01') LIMIT 10;
```

---

## 完整的文件清单

创建的文件：

```
dags/
├── dw_config.yaml (已修改)
│   └── 添加了 ods_fund_etf_spot 配置
├── ods/
│   └── ods_fund_etf_spot.sql (新建)
└── catalog/migrations/
    └── 0007_ods_fund_etf_spot_table.sql (新建)
```

---

## 常见问题

### Q: 数据已经在 raw 层，如何确定 path？
A: 查看数据实际存放位置，如 `lake/raw/daily/{target}`

### Q: 字段名称不一致怎么办？
A: 在 SQL 文件中使用 `AS` 重命名，如 `代码 AS symbol`

### Q: 日期格式不是 YYYYMMDD？
A: 调整 `STRPTIME` 的格式参数，如 `'%Y-%m-%d'`

### Q: 为什么需要 tmp_ 前缀？
A: Airflow 会创建临时表 `tmp_ods_{target}`，SQL 从这里读取

---

## 总结

ODS 层接入的核心步骤：

1. ✅ 配置 `sources`（指定 raw 数据路径）
2. ✅ 创建 SQL 文件（从 tmp 表读取，添加分区列）
3. ✅ 创建 migration 文件（定义 schema、view、macro）
4. ✅ 运行 DAG 验证

跨层依赖和其他配置由框架自动处理。
