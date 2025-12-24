# DIM 层接入完整示例

## 场景说明

我们需要创建一个交易日历维度表 `dim_trade_calendar_full`，从 ODS 层的交易日期数据生成各种时间维度字段。

**前提条件**：
- SQL 查询语句已准备好
- 数据来源：`ods.ods_trade_date_hist`

---

## 步骤 1：分析 SQL 和依赖

准备好的 SQL：

```sql
SELECT
    trade_date,
    YEAR(trade_date) AS trade_year,
    MONTH(trade_date) AS trade_month,
    QUARTER(trade_date) AS trade_quarter,
    DAYOFWEEK(trade_date) AS trade_day_of_week,
    '${PARTITION_DATE}' AS dt
FROM ods.ods_trade_date_hist;
```

**分析依赖**：
- 数据来源：`ods.ods_trade_date_hist`（ODS 层）
- 同层依赖：无（没有引用其他 DIM 表）
- 跨层依赖：自动处理（ODS → DIM）

---

## 步骤 2：创建 SQL 文件

创建文件：`dags/dim/dim_trade_calendar_full.sql`

```sql
SELECT
    trade_date,
    YEAR(trade_date) AS trade_year,
    MONTH(trade_date) AS trade_month,
    QUARTER(trade_date) AS trade_quarter,
    DAYOFWEEK(trade_date) AS trade_day_of_week,
    DAY(trade_date) AS trade_day_of_month,
    DAYOFYEAR(trade_date) AS trade_day_of_year,
    WEEKOFYEAR(trade_date) AS trade_week_of_year,
    DAYOFWEEK(trade_date) NOT IN (1, 7) AS is_weekday,
    (trade_date == (DATE_TRUNC('MONTH', trade_date + INTERVAL '1 MONTH') - INTERVAL '1 DAY')) AS is_month_end,
    (trade_date == (DATE_TRUNC('QUARTER', trade_date + INTERVAL '3 MONTH') - INTERVAL '1 DAY')) AS is_quarter_end,
    (MONTH(trade_date) == 12 AND DAY(trade_date) == 31) AS is_year_end,
    '${PARTITION_DATE}' AS dt
FROM ods.ods_trade_date_hist;
```

**说明**：
- 从 `ods.ods_trade_date_hist` 读取数据
- 使用标准 SQL 生成时间维度字段
- 添加分区列 `${PARTITION_DATE}`

---

## 步骤 3：检查同层依赖

由于 SQL 只引用了 ODS 层的表，没有同层依赖，因此不需要配置 `table_dependencies`。

**如果 SQL 中有类似这样的引用**：
```sql
FROM dim.other_dim_table
```
则需要配置同层依赖。

---

## 步骤 4：创建 Migration 文件

查找下一个可用的 migration 编号：

```bash
ls catalog/migrations/
# 输出: ... 0005_trade_date_hist_table.sql
```

下一个编号是 `0006`。

创建文件：`catalog/migrations/0006_dim_trade_calendar_full_table.sql`

```sql
-- Schema Macro
CREATE OR REPLACE MACRO dim._schema_dim_trade_calendar_full() AS
MAP {
    0: {name: 'trade_date', type: 'DATE', default_value: NULL},
    1: {name: 'trade_year', type: 'INTEGER', default_value: NULL},
    2: {name: 'trade_month', type: 'INTEGER', default_value: NULL},
    3: {name: 'trade_quarter', type: 'INTEGER', default_value: NULL},
    4: {name: 'trade_day_of_week', type: 'INTEGER', default_value: NULL},
    5: {name: 'trade_day_of_month', type: 'INTEGER', default_value: NULL},
    6: {name: 'trade_day_of_year', type: 'INTEGER', default_value: NULL},
    7: {name: 'trade_week_of_year', type: 'INTEGER', default_value: NULL},
    8: {name: 'is_weekday', type: 'BOOLEAN', default_value: NULL},
    9: {name: 'is_month_end', type: 'BOOLEAN', default_value: NULL},
    10: {name: 'is_quarter_end', type: 'BOOLEAN', default_value: NULL},
    11: {name: 'is_year_end', type: 'BOOLEAN', default_value: NULL}
};

-- Seed Parquet
COPY (
    SELECT
        CAST(NULL AS DATE) AS trade_date,
        CAST(NULL AS INTEGER) AS trade_year,
        CAST(NULL AS INTEGER) AS trade_month,
        CAST(NULL AS INTEGER) AS trade_quarter,
        CAST(NULL AS INTEGER) AS trade_day_of_week,
        CAST(NULL AS INTEGER) AS trade_day_of_month,
        CAST(NULL AS INTEGER) AS trade_day_of_year,
        CAST(NULL AS INTEGER) AS trade_week_of_year,
        CAST(NULL AS BOOLEAN) AS is_weekday,
        CAST(NULL AS BOOLEAN) AS is_month_end,
        CAST(NULL AS BOOLEAN) AS is_quarter_end,
        CAST(NULL AS BOOLEAN) AS is_year_end
    WHERE 1=0
) TO 's3://stock-data/lake/dim/dim_trade_calendar_full/dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- View
CREATE OR REPLACE VIEW dim.dim_trade_calendar_full AS
SELECT * FROM read_parquet(
    's3://stock-data/lake/dim/dim_trade_calendar_full/dt=*/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'dt': 'VARCHAR'},
    union_by_name = true
) WHERE dt <> '1900-01-01';

-- Date Macro
CREATE OR REPLACE MACRO dim.dim_trade_calendar_full_dt(p_date) AS TABLE
SELECT * FROM dim.dim_trade_calendar_full
WHERE dt = p_date;
```

---

## 步骤 5：配置同层依赖（如有）

此示例无同层依赖，跳过此步。

**如果有同层依赖**，在 `dags/dw_config.yaml` 添加：

```yaml
table_dependencies:
  dim:
    dim_trade_calendar_full: [dim_other_table]
```

---

## 步骤 6：验证和运行

### 1. 检查文件结构

```bash
# 确认 SQL 文件
cat dags/dim/dim_trade_calendar_full.sql

# 确认 migration 文件
ls catalog/migrations/0006_dim_trade_calendar_full_table.sql
```

### 2. 运行 DAG

```bash
# 在 Airflow UI 中运行 dw_dim DAG
# 或使用命令行
airflow dags trigger dw_dim
```

### 3. 验证数据

```sql
-- 查询新表
SELECT * FROM dim.dim_trade_calendar_full LIMIT 10;

-- 查看特定日期
SELECT * FROM dim.dim_trade_calendar_full_dt('2024-01-01') LIMIT 10;

-- 验证时间维度计算
SELECT
    trade_date,
    trade_year,
    trade_month,
    is_weekday,
    is_month_end
FROM dim.dim_trade_calendar_full
WHERE trade_date = '2024-01-31';
```

---

## 完整的文件清单

创建的文件：

```
dags/
└── dim/
    └── dim_trade_calendar_full.sql (新建)
└── catalog/migrations/
    └── 0006_dim_trade_calendar_full_table.sql (新建)
```

修改的文件（如有同层依赖）：
```
dags/
└── dw_config.yaml (可选修改)
```

---

## 示例 2：有同层依赖的情况

### 场景
创建 `dim_stock_info`，依赖 `dim_trade_calendar_full`。

### SQL
```sql
SELECT
    s.symbol,
    s.name,
    s.industry,
    t.trade_year,
    '${PARTITION_DATE}' AS dt
FROM ods.ods_stock_info s
LEFT JOIN dim.dim_trade_calendar_full t
    ON s.list_date = t.trade_date;
```

### 配置依赖

在 `dags/dw_config.yaml` 添加：

```yaml
table_dependencies:
  dim:
    dim_stock_info: [dim_trade_calendar_full]
```

**说明**：
- `dim_stock_info` 依赖 `dim_trade_calendar_full`
- Airflow 会先运行 `dim_trade_calendar_full`，再运行 `dim_stock_info`

---

## 常见问题

### Q: 跨层依赖需要配置吗？
A: 不需要。框架自动处理 ODS → DIM 的依赖关系。

### Q: 如何确定是否有同层依赖？
A: 检查 SQL 中 `FROM`、`JOIN` 是否引用了同层的表。

### Q: 可以引用 ADS 层的表吗？
A: 不推荐。应该从最近的下层读取数据。

### Q: 依赖配置错了怎么办？
A: 修改 `dw_config.yaml`，重新运行 DAG。

---

## 总结

DIM/DWD/ADS 层接入的核心步骤：

1. ✅ 分析 SQL，确定依赖（跨层依赖自动处理）
2. ✅ 创建 SQL 文件（使用标准 SQL）
3. ✅ 配置同层依赖（如有）
4. ✅ 创建 migration 文件
5. ✅ 运行 DAG 验证

与 ODS 的区别：
- 不需要配置 `sources`
- 需要检查同层依赖
- 直接使用标准 SQL，无需 tmp 表
