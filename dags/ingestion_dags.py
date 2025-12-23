"""
Ingestion DAG Factory.
Scans dags/ingestion/configs/*.yaml and generates Airflow DAGs.
"""

import logging
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.ingestion.core.interfaces import IngestionJob
from dags.ingestion.core.loader import instantiate_component
from lakehouse_core.api import prepare_paths
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "ingestion" / "configs"
DEFAULT_ARGS = {
    "owner": "ingestion",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET = "stock-data"
TMP_PREFIX_BASE = "tmp/ingestion"

def _load_yaml_configs() -> list[Path]:
    if not CONFIG_DIR.exists():
        logger.warning(f"Config directory not found: {CONFIG_DIR}")
        return []
    return list(CONFIG_DIR.glob("*.yaml"))

def create_dag(config_path: Path):
    try:
        with open(config_path, encoding="utf-8") as f:
            conf = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config {config_path}: {e}")
        return None

    target = conf["target"]
    dag_id = f"ingestion_{target}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=conf.get("schedule_interval", "@daily"),
        start_date=datetime.fromisoformat(conf.get("start_date", "2024-01-01")),
        catchup=conf.get("catchup", False),
        tags=["ingestion", target],
        max_active_runs=1,
    )

    with dag:
        # --- 1. Prepare Phase ---
        @task
        def prepare_ingestion_paths(**context) -> dict:
            dag_run_conf = context.get("dag_run").conf or {}
            partition_date = dag_run_conf.get("start_date") or get_partition_date_str()

            # Sanitize run_id for S3/MinIO compatibility (remove :, +, . etc)
            raw_run_id = context["run_id"]
            safe_run_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in raw_run_id)

            # 使用 core API 生成标准路径
            base_prefix = f"lake/raw/daily/{target}"
            paths = prepare_paths(
                base_prefix=base_prefix,
                run_id=safe_run_id,
                partition_date=partition_date,
                is_partitioned=True,
                store_namespace=DEFAULT_BUCKET
            )
            return {
                "partition_date": partition_date,
                "partitioned": True,
                "base_prefix": base_prefix,
                "tmp_prefix": paths.tmp_prefix,
                "tmp_partition_prefix": paths.tmp_partition_prefix,
                "canonical_prefix": paths.canonical_prefix,
                "manifest_path": paths.manifest_path,
                "success_flag_path": paths.success_flag_path,
            }

        # --- 2. Plan Phase ---
        @task
        def plan(paths_dict: dict, **context) -> list[dict]:
            partitioner = instantiate_component(conf["partitioner"])
            start = paths_dict["partition_date"]

            jobs = []
            for job in partitioner.generate_jobs(start_date=start, end_date=start):
                jobs.append({"params": job.params, "meta": job.meta})
            return jobs

        # --- 3. Execute Phase (Map) ---
        @task(map_index_template="{{ job_dict['params'] }}")
        def execute(job_dict: dict, paths_dict: dict, **context) -> dict:
            job = IngestionJob(params=job_dict["params"], meta=job_dict["meta"])
            extractor = instantiate_component(conf["extractor"])
            df = extractor.extract(job)

            if df is None or df.empty:
                return {"row_count": 0, "status": "skipped"}

            # 写入到 prepare 阶段定义的临时目录
            import uuid

            from dags.adapters.airflow_s3_store import AirflowS3Store
            from lakehouse_core.io.uri import join_uri

            file_id = str(uuid.uuid4())[:8]
            # paths_dict['tmp_prefix'] 已经是 s3://bucket/path 格式
            # 直接使用 join_uri 拼接子路径
            uri = join_uri(paths_dict['tmp_prefix'], f"results/{file_id}.parquet")

            s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
            store = AirflowS3Store(s3_hook)

            buf = BytesIO()
            df.to_parquet(buf, index=False)
            store.write_bytes(uri, buf.getvalue())

            return {
                "uri": uri,
                "row_count": len(df),
                "file_count": 1,
                "status": "success"
            }

        # --- 4. Compact & Commit Phase (Reduce) ---
        @task
        def commit_ingestion(task_results: list[dict], paths_dict: dict, **context):
            success_results = [r for r in task_results if r.get("status") == "success"]
            if not success_results:
                logger.info("No data to commit.")
                return {"status": "skipped", "reason": "no_data"}

            total_rows = sum(r["row_count"] for r in success_results)
            logger.info(f"Total rows from all extraction jobs: {total_rows}")

            # 使用 StandardS3Compactor 处理最终合并和提交
            # compact() 内部包含: Load → Validate → Commit → Cleanup
            compactor = instantiate_component(conf["compactor"])

            publish_result = compactor.compact(
                results=success_results,
                target=target,
                partition_date=paths_dict["partition_date"],
                paths_dict=paths_dict,
                run_id=context["run_id"]
            )
            logger.info(f"Compaction and commit completed: {publish_result}")

            return {
                "status": "success",
                "manifest_path": publish_result.get("manifest_path", ""),
                "total_rows": total_rows,
            }

        # --- Workflow ---
        p_paths = prepare_ingestion_paths()
        p_jobs = plan(p_paths)
        results = execute.partial(paths_dict=p_paths).expand(job_dict=p_jobs)
        commit_ingestion(results, p_paths)

    return dag

    return dag


# Scan and Generate
for config_file in _load_yaml_configs():
    dag_object = create_dag(config_file)
    if dag_object:
        globals()[dag_object.dag_id] = dag_object
