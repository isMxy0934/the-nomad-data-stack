# Architecture Notes / Open Questions

本文件记录“代码已实现但文档/架构仍需澄清或后续改进”的点，方便后续迭代时对齐共识。

## 1) Ingestion 与 DW 链路未强绑定

- 当前 `ingestion_{target}`（`dags/ingestion_dags.py`）与 `dw_start_dag -> dw_catalog_dag -> dw_ods -> dw_{layer}...`（`dags/dw_*.py`）是两条独立链路。
- 如果希望“一键日常跑批=先采集再建模”，需要一个 master DAG（或在 `dw_start_dag` 前增加触发并等待 ingestion 的编排），并明确失败/重试语义。

## 2) Catalog：migrations vs refresh 的职责边界

- `dw_catalog_dag` 目前只负责 apply migrations（`catalog/migrations/*.sql`），然后触发 `dw_ods`。
- 已移除 “refresh by scanning storage” 的脚本入口，catalog 的来源统一为 migrations（显式声明、可审计）。

## 3) Ingestion 的 dt 语义：运行日期 vs 数据日期

- Ingestion DAG 的 prepare 阶段默认使用 `get_partition_date_str()`（T-1）作为“运行分区日期”。
- 若 `compactor.kwargs.partition_column` 配置存在（例如 `trade_date`），`StandardS3Compactor` 会按列值写入多个 `dt=...` 分区（数据日期驱动），并可能覆盖/补齐历史分区。
- 这对“按 run 的 dt 推导路径/回溯”不完全一致，建议在新增 ingestion target 时明确：
  - 是“每日快照写当天 dt”，还是“按数据日期分区写多天”。
  - 对回填/重跑的影响（尤其是 delete→copy 的 publish 语义）。

## 4) Commit Protocol 的非原子性风险

- 当前核心 publish 语义是 `delete_prefix -> copy_prefix -> markers`（`lakehouse_core/domain/commit_protocol.py`）。
- 在 delete 成功但 copy 失败时，会产生 canonical 分区为空的风险（已接受的权衡）。
- 若未来需要更强原子性，需要引入外部元数据层（Hive Metastore/Iceberg 等），否则不建议在纯 S3 上做“指针切换”复杂化。

## 5) Catalog migration 里的 seed dt 值一致性（可选优化）

- 现有 migrations 的 seed parquet 生成语句里会写入一个 `dt` 列（部分文件使用占位符字符串）。
- 由于视图读取使用了 `hive_partitioning=true`，最终查询的 `dt` 会以目录分区值为准，因此 seed 文件通常仍会被 `dt <> '1900-01-01'` 过滤掉。
- 为减少歧义，可以在后续迭代中把 seed 的 `dt` 列固定为 `'1900-01-01'`（仅提升可读性，不改变行为）。
