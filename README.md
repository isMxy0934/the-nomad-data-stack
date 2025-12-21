# the-nomad-data-stack

## 简介 (Introduction)

这是一个专为个人开发者和小团队设计的 **轻量级无服务器湖仓（Serverless Lakehouse）** 骨架。

抛弃沉重的 Hadoop/Spark/Kafka 架构，回归极简。利用 **Airflow + DuckDB + MinIO (S3)**，在一台笔记本上即可跑通现代化的「采集 → 清洗 (ODS) → 数仓建模 (DW)」全链路 ETL。

## 技术架构 (Architecture)

核心设计理念：**存算分离 (Separation of Storage and Compute)**

*   **调度与编排 (Orchestration)**: **Apache Airflow**
    *   系统的“大脑”。自动根据配置生成 DAG，管理任务依赖、重试和并发。
    *   Prefect (self-hosted) flows live under `flows/` for parallel adoption; logic stays compatible with Airflow DAGs.
*   **计算引擎 (Compute)**: **DuckDB**
    *   系统的“肌肉”。作为易逝计算资源 (Ephemeral Compute) 运行在 Worker 中，负责执行高性能 SQL 转换。无状态，随用随走。
*   **存储层 (Storage)**: **MinIO (S3)**
    *   系统的“血液”。所有数据均以 **Parquet** 开放格式存储，是系统唯一的“事实来源 (Single Source of Truth)”。

## 核心能力 (Features)

*   **全自动 ETL 链路**: 内置标准化的 Raw -> ODS -> DWD -> ADS 分层处理流程。
*   **配置驱动开发 (Config-Driven)**: 新增一张表只需编写 SQL 和简单的 YAML 配置，无需编写 Python 代码。
*   **完备的初始化/回填**: 针对历史数据提供专用的回填机制 (`dw_init_dag`)，防止数据污染。
*   **原子性保证**: 采用 "Write-Audit-Publish" 模式，确保数据写入的一致性，失败无残留。

## 快速开始 (Quick Start)

### 1. 启动服务

```bash
# 1. 准备配置 (按需修改 .env)
cp env.example .env

# 2. 启动容器 (Airflow + MinIO + Postgres)
docker compose up -d

# 3. Prefect work pool (first time only)
docker compose exec prefect-server prefect work-pool create default

# 4. Register Prefect deployments (uses flows/prefect.yaml)
docker compose exec prefect-worker bash -c "cd /opt/prefect/flows && prefect deploy --all"
```

### 2. 访问控制台

*   **Airflow**: [http://localhost:8080](http://localhost:8080) (账号/密码见 `.env`，默认 `admin`/`admin`)
*   **MinIO**: [http://localhost:9001](http://localhost:9001) (默认 `minioadmin`/`minioadmin`)
*   **Prefect**: [http://localhost:4200](http://localhost:4200)

### 3. 运行任务

在 Airflow UI 中，你主要关注两个入口 DAG：

*   **日常跑批 (Daily Run)**: 
    *   手动触发 **`dw_start_dag`**。
    *   它会自动计算“昨天”的日期，并依次执行：数据采集 -> Catalog 刷新 -> ODS 加载 -> 数仓分层计算。
*   **历史初始化 (History Init)**: 
    *   点击 **`dw_init_dag`** -> Trigger DAG w/ config。
    *   在表单中填写 `start_date` (开始日期) 和 `end_date` (结束日期)。
    *   系统将并行处理历史分区的元数据和模型计算。

---

### 更多文档
*   [目录结构与开发约定](docs/conventions.md)
*   [DuckDB Catalog 使用指南](docs/catalog.md)
