# the-nomad-data-stack

## 简介 (Introduction)

这是一个专为个人开发者和小团队设计的 **轻量级无服务器湖仓（Serverless Lakehouse）** 骨架。

抛弃沉重的 Hadoop/Spark/Kafka 架构，回归极简。利用 **Airflow + DuckDB + MinIO (S3)**，在一台笔记本上即可跑通现代化的「采集 → 清洗 (ODS) → 数仓建模 (DW)」全链路 ETL。

## 技术架构 (Architecture)

核心设计理念：**存算分离 (Separation of Storage and Compute)**

*   **调度与编排 (Orchestration)**: **Apache Airflow**
    *   系统的“大脑”。自动根据配置生成 DAG，管理任务依赖、重试和并发。
*   **计算引擎 (Compute)**: **DuckDB**
    *   系统的“肌肉”。作为易逝计算资源 (Ephemeral Compute) 运行在 Worker 中，负责执行高性能 SQL 转换。无状态，随用随走。
*   **存储层 (Storage)**: **MinIO (S3)**
    *   系统的“血液”。所有数据均以 **Parquet** 开放格式存储，是系统唯一的“事实来源 (Single Source of Truth)”。

## 核心能力 (Features)

*   **分层 ETL 链路**: 内置标准化的 Raw -> ODS -> DW（DWD/DIM/.../ADS）分层处理流程。
*   **约定优于配置**:
    *   DW：目录即配置（`dags/{layer}/*.sql` 自动发现），并由 `dags/dw_config.yaml` 定义层依赖。
    *   ODS：通过 `dags/dw_config.yaml:sources` 将 RAW 的 CSV 源映射到 `ods_*` 表。
*   **初始化/回填入口**: `dw_init_dag` 支持按日期范围回放（可选 targets）。
*   **一致性写入协议（非原子）**: 采用 “tmp → validate → delete→copy publish → markers → cleanup” 模式；S3/MinIO 不支持原子重命名，接受极小概率风险（详见 commit protocol 文档）。

## 快速开始 (Quick Start)

### 1. 启动服务

```bash
# 1. 准备配置 (按需修改 .env)
cp env.example .env

# 2. 启动容器 (Airflow + MinIO + Postgres)
docker compose up -d
```

### 2. 访问控制台

*   **Airflow**: [http://localhost:8080](http://localhost:8080) (账号/密码见 `.env`，默认 `admin`/`admin`)
*   **MinIO**: [http://localhost:9001](http://localhost:9001) (默认 `minioadmin`/`minioadmin`)

### 3. 运行任务

在 Airflow UI 中，你主要关注两类 DAG：

*   **采集（Ingestion）**：
    *   `dags/ingestion_dags.py` 会扫描 `dags/ingestion/configs/*.yaml` 并生成 `ingestion_{target}` DAG（按配置 schedule 运行或手动触发）。
    *   采集写入 RAW（CSV），默认路径形如：`lake/raw/daily/{target}/dt=YYYY-MM-DD/data.csv`（可由 compactor 配置覆盖）。
*   **数仓跑批（DW Orchestration）**：
    *   **日常跑批 (Daily Run)**：手动触发 **`dw_start_dag`**，自动处理“昨天”（T-1）的分区日期，并触发 `dw_catalog_dag -> dw_ods -> dw_{layer}...`。
    *   **历史初始化 (History Init)**：
    *   点击 **`dw_init_dag`** -> Trigger DAG w/ config。
    *   在表单中填写 `start_date` (开始日期) 和 `end_date` (结束日期)。
    *   系统将按日期范围回放 `dw_ods` 与后续各层（可选 `targets`）。

---

### 更多文档
*   [目录结构与开发约定](docs/conventions.md)
*   [提交协议（Commit Protocol）与完成标记](docs/commit-protocol.md)
*   [DuckDB Catalog 使用指南](docs/catalog.md)
*   [`lakehouse_core` Public API](docs/lakehouse_core_api.md)
*   [Architecture Notes / Open Questions](docs/architecture-notes.md)
