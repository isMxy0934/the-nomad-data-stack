## 提交协议（Commit Protocol）与完成标记

本项目的数据写入遵循“临时前缀提交协议”，避免下游读到写入中间态，并提供幂等重跑能力。

实现位置：

- 核心逻辑：`lakehouse_core/domain/commit_protocol.py`（delete → copy → markers）
- 编排封装：`lakehouse_core/pipeline/commit.py`、`dags/utils/etl_utils.py`

### 核心原则

- 禁止直接写 canonical 分区（禁止 in-place overwrite）
- 标准流程：tmp 写入 → 校验 → publish → 标记完成 → 清理 tmp

### 分区表（dt=...）标准流程

1. 写入 tmp：`.../_tmp/run_{run_id}/dt=YYYY-MM-DD/*.parquet`
2. 校验：至少校验 `row_count` / `file_count` 与期望一致
3. publish（非原子）：
   - 删除 canonical 分区前缀：`.../dt=YYYY-MM-DD/`
   - 从 tmp 拷贝到 canonical 分区前缀
4. 写入标记：
   - `manifest.json`：记录本次写入的元信息
   - `_SUCCESS`：分区完成标记
5. 清理：删除 tmp 前缀

> 非原子性说明：S3/MinIO 不支持目录原子重命名，本项目接受 “delete 成功但 copy 失败” 的极小概率风险。

### 非分区表标准流程

- canonical：`.../{layer}/{table}/*.parquet`
- tmp：`.../{layer}/{table}/_tmp/run_{run_id}/*.parquet`
- publish：删除 canonical 前缀后从 tmp 拷贝到 canonical，并写入 `manifest.json` + `_SUCCESS`

### `manifest.json` 字段

`manifest.json` 是便于审计/排障的 metadata（不作为数据血缘系统）。

- `dest`：表名
- `partition_date`：分区日期（非分区表也会记录运行时的 partition_date）
- `run_id`：Airflow run id
- `file_count` / `row_count`
- `source_prefix`：tmp 前缀
- `target_prefix`：canonical 前缀
- `generated_at`：生成时间

### 空数据语义

- 如果上游当日无数据：任务视为成功，会清空 canonical 分区前缀（避免残留旧数据），且不会写 `_SUCCESS` 或 `manifest.json`（仅记录日志）
- 下游以编排依赖保证“上游失败不触发下游”；读不到分区即视为空输入
