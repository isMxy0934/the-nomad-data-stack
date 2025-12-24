# 快速参考指南

本指南提供接入新表的快速参考。

---

## 文件位置速查

| 层级 | SQL 文件位置 | Migration 命名格式 |
|------|-------------|-------------------|
| **ODS** | `dags/ods/{target}.sql` | `00XX_{target}_table.sql` |
| **DWD** | `dags/dwd/{table}.sql` | `00XX_dwd_{table}_table.sql` |
| **DIM** | `dags/dim/{table}.sql` | `00XX_dim_{table}_table.sql` |
| **ADS** | `dags/ads/{table}.sql` | `00XX_ads_{table}_table.sql` |

---

## 接入流程速查

### ODS 层
```
1. 配置 sources (dw_config.yaml)
2. 创建 SQL 文件 (dags/ods/{target}.sql)
3. 创建 Migration 文件
4. 运行 dw_ods DAG
```

### DIM/DWD/ADS 层
```
1. 分析 SQL，确定同层依赖
2. 创建 SQL 文件 (dags/{layer}/{table}.sql)
3. 配置 table_dependencies (如有)
4. 创建 Migration 文件
5. 运行 dw_{layer} DAG
```

---

## 配置模板速查

### ODS Sources 配置
```yaml
sources:
  ods_{table_name}:
    path: "lake/raw/daily/{target}"
    format: "csv"
```

### 同层依赖配置
```yaml
table_dependencies:
  {layer}:
    {table_name}: [dependency_table1, dependency_table2]
```

---

## SQL 模板速查

### ODS 层 SQL
```sql
SELECT
  field1,
  field2,
  '${PARTITION_DATE}' AS dt
FROM tmp_{target};
```

### 上层 SQL (DIM/DWD/ADS)
```sql
SELECT
  field1,
  field2,
  '${PARTITION_DATE}' AS dt
FROM ods.ods_source_table;
```

---

## Migration 模板速查

```sql
-- 1. Schema Macro
CREATE OR REPLACE MACRO {layer}._schema_{table_name}() AS
MAP {
    0: {name: 'field1', type: 'VARCHAR', default_value: NULL},
    1: {name: 'field2', type: 'INTEGER', default_value: NULL}
};

-- 2. Seed Parquet
COPY (SELECT ... WHERE 1=0)
TO 's3://.../dt=1900-01-01/__seed__.parquet' (FORMAT PARQUET);

-- 3. View
CREATE OR REPLACE VIEW {layer}.{layer}_{table_name} AS
SELECT * FROM read_parquet(...) WHERE dt <> '1900-01-01';

-- 4. Date Macro
CREATE OR REPLACE MACRO {layer}.{layer}_{table_name}_dt(p_date) AS TABLE
SELECT * FROM {layer}.{layer}_{table_name} WHERE dt = p_date;
```

---

## 数据类型速查

| 类型 | 说明 | 示例 |
|------|------|------|
| `VARCHAR` | 字符串 | `symbol VARCHAR` |
| `INTEGER` | 整数 | `volume INTEGER` |
| `DOUBLE` | 浮点数 | `price DOUBLE` |
| `DATE` | 日期 | `trade_date DATE` |
| `BOOLEAN` | 布尔值 | `is_active BOOLEAN` |

---

## 常用 SQL 函数速查

### 日期转换
```sql
-- 字符串转日期（YYYYMMDD → DATE）
STRPTIME(CAST(date_str AS VARCHAR), '%Y%m%d')

-- 字符串转日期（YYYY-MM-DD → DATE）
CAST(date_str AS DATE)
```

### 日期提取
```sql
YEAR(date)    -- 年份
MONTH(date)   -- 月份
DAY(date)     -- 日
QUARTER(date) -- 季度
```

### 日期计算
```sql
DATE_TRUNC('MONTH', date)           -- 月初
DATE_TRUNC('QUARTER', date)         -- 季初
date + INTERVAL '1 DAY'             -- 加一天
date - INTERVAL '1 MONTH'           -- 减一月
```

---

## 命令速查

### 查看 Migration
```bash
ls catalog/migrations/
```

### 查看配置
```bash
cat dags/dw_config.yaml
```

### 查看现有表
```bash
ls dags/ods/
ls dags/dim/
ls dags/dwd/
```

### 触发 DAG
```bash
airflow dags trigger dw_ods
airflow dags trigger dw_dim
airflow dags trigger dw_dwd
```

### 查询验证
```sql
-- 查看表结构
DESCRIBE {layer}.{layer}_{table_name};

-- 查询数据
SELECT * FROM {layer}.{layer}_{table_name} LIMIT 10;

-- 查询特定日期
SELECT * FROM {layer}.{layer}_{table_name}_dt('2024-01-01');
```

---

## 问题排查速查

| 问题 | 可能原因 | 解决方法 |
|------|---------|---------|
| DAG 找不到表 | SQL 文件名错误 | 检查文件名是否正确 |
| DAG 顺序错误 | 同层依赖未配置 | 配置 `table_dependencies` |
| 找不到数据源 | sources 未配置 | 配置 `sources` |
| Migration 失败 | SQL 语法错误 | 检查 SQL 语法 |
| 数据加载失败 | 临时表名错误 | 检查 `tmp_{table}` 名称 |

---

## 命名规范速查

### 表名
- 格式：`{layer}_{business_entity}`
- 示例：`ods_fund_etf_spot`, `dim_trade_calendar_full`
- 规则：小写 + 下划线

### 文件名
- ODS SQL：`{target}.sql`（不含层级前缀）
- 其他层 SQL：`{layer}_{table}.sql`（含层级前缀）
- Migration：`00XX_{table}_table.sql`

### 配置名称
- sources：`ods_{table_name}`
- table_dependencies：`{table_name}`（不含层级前缀）

---

## 检查清单

### ODS 层接入检查清单
- [ ] 配置 `sources`
- [ ] 创建 SQL 文件（从 tmp 表读取）
- [ ] 添加 `${PARTITION_DATE}` 分区列
- [ ] 创建 Migration 文件
- [ ] 验证数据路径正确

### 上层接入检查清单
- [ ] 分析 SQL，确定依赖
- [ ] 创建 SQL 文件
- [ ] 添加 `${PARTITION_DATE}` 分区列
- [ ] 配置同层依赖（如有）
- [ ] 创建 Migration 文件
- [ ] 验证跨层依赖（自动处理）

---

## 相关文件

| 文件 | 说明 |
|------|------|
| `skill.md` | 主要技能文档（接入流程和规范） |
| `examples/ods_integration.md` | ODS 接入完整示例 |
| `examples/dim_integration.md` | DIM 接入完整示例 |

---

## 快速开始

### 步骤 1：确定层级
```
ODS  → 数据来自 raw 层
DIM  → 维度表
DWD  → 明细数据
ADS  → 应用数据
```

### 步骤 2：准备 SQL
```
ODS: 从 tmp_{table} 读取
其他: 从下层读取
```

### 步骤 3：配置文件
```
ODS: 配置 sources
其他: 配置 table_dependencies（如有）
```

### 步骤 4：创建文件
```
SQL 文件 + Migration 文件
```

### 步骤 5：运行验证
```
运行 DAG + 查询验证
```

---

## 获取帮助

告诉 Claude：
1. 接入哪一层？
2. SQL 是什么？
3. 有什么依赖？

Claude 会引导您完成接入流程。
