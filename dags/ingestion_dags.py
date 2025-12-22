"""
Ingestion DAG Factory.
Scans dags/ingestion/configs/*.yaml and generates Airflow DAGs.
"""

import logging
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.ingestion.core.interfaces import IngestionJob
from dags.ingestion.core.loader import instantiate_component
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "ingestion" / "configs"
DEFAULT_ARGS = {
    "owner": "ingestion",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
TMP_BUCKET = "stock-data"  # Should come from env
TMP_PREFIX = "tmp/ingestion"


def _load_yaml_configs() -> list[Path]:
    if not CONFIG_DIR.exists():
        logger.warning(f"Config directory not found: {CONFIG_DIR}")
        return []
    return list(CONFIG_DIR.glob("*.yaml"))


def create_dag(config_path: Path):
    """
    Creates a DAG from a YAML config file.
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            conf = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config {config_path}: {e}")
        return None

    dag_id = f"ingestion_{conf['target']}"
    schedule = conf.get("schedule_interval", "@daily")
    start_date_str = conf.get("start_date", "2024-01-01")
    start_date = datetime.fromisoformat(start_date_str)

    # Catchup logic: Usually false for ingestion to avoid storming
    catchup = conf.get("catchup", False)

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["ingestion", conf["target"]],
        max_active_runs=1,
    )

    with dag:
        # --- 1. Plan Phase ---
        @task(task_id="plan")
        def plan(**context) -> list[dict]:
            # Instantiate Partitioner
            partitioner = instantiate_component(conf["partitioner"])

            # Determine logic date range
            # Priority: 1. dag_run.conf, 2. get_partition_date_str()
            dag_run_conf = context.get("dag_run").conf or {}
            default_date = get_partition_date_str()

            start = dag_run_conf.get("start_date") or default_date
            end = dag_run_conf.get("end_date") or default_date

            logger.info(f"Planning jobs for {start} to {end}")

            # Generate Jobs
            # We serialize IngestionJob objects to dicts for XCom
            jobs = []
            for job in partitioner.generate_jobs(start_date=start, end_date=end):
                jobs.append({"params": job.params, "meta": job.meta})

            logger.info(f"Generated {len(jobs)} jobs")
            return jobs

        # --- 2. Execute Phase (Map) ---
        @task(task_id="execute", map_index_template="{{ job_dict['params'] }}")
        def execute(job_dict: dict, **context) -> str:
            """
            Executes extraction and saves result to a temp S3 file.
            Returns the S3 Key.
            """
            # Rehydrate Job
            job = IngestionJob(params=job_dict["params"], meta=job_dict["meta"])

            # Instantiate Extractor
            extractor = instantiate_component(conf["extractor"])

            # Run
            df = extractor.extract(job)

            if df is None or df.empty:
                logger.info("No data extracted.")
                return ""  # Skip

            # Save to Tmp S3 to avoid XCom limit
            # Path: tmp/ingestion/{dag_run_id}/{task_id}/{index}.parquet
            run_id = context["run_id"]
            # We use a hash or just random string for uniqueness if index is tricky
            import uuid

            fname = f"{uuid.uuid4()}.parquet"
            key = f"{TMP_PREFIX}/{dag_id}/{run_id}/{fname}"

            s3_hook = S3Hook(aws_conn_id="MINIO_S3")
            # Write Parquet for efficiency
            buf = BytesIO()
            df.to_parquet(buf, index=False)
            s3_hook.load_bytes(buf.getvalue(), key=key, bucket_name=TMP_BUCKET, replace=True)

            logger.info(f"Wrote {len(df)} rows to s3://{TMP_BUCKET}/{key}")
            return key

        # --- 3. Compact Phase (Reduce) ---
        @task(task_id="compact")
        def compact(s3_keys: list[str], **context):
            """
            Reads temp files from S3 and compacts them.
            """
            valid_keys = [k for k in s3_keys if k and isinstance(k, str)]
            if not valid_keys:
                logger.info("No valid keys to compact. Skipping.")
                return

            s3_hook = S3Hook(aws_conn_id="MINIO_S3")
            frames = []

            # Read all temp files
            # Note: For massive scale, we wouldn't load all into memory here.
            # We would use Spark or specialized tools. But for this scale, it's fine.
            for key in valid_keys:
                obj = s3_hook.get_key(key, bucket_name=TMP_BUCKET)
                if obj:
                    buf = BytesIO(obj.get()["Body"].read())
                    frames.append(pd.read_parquet(buf))

            # Instantiate Compactor
            compactor = instantiate_component(conf["compactor"])

            # Determine partition date (align with plan phase logic)
            dag_run_conf = context.get("dag_run").conf or {}
            partition_date = dag_run_conf.get("start_date") or get_partition_date_str()

            # Run Compact
            metrics = compactor.compact(
                results=frames, target=conf["target"], partition_date=partition_date
            )

            logger.info(f"Compaction finished: {metrics}")

            # Clean up temp files
            if valid_keys:
                s3_hook.delete_objects(bucket=TMP_BUCKET, keys=valid_keys)

        # --- Graph Definition ---
        plan_task = plan()
        execute_task = execute.expand(job_dict=plan_task)
        compact(s3_keys=execute_task)

    return dag


# Scan and Generate
for config_file in _load_yaml_configs():
    dag_object = create_dag(config_file)
    if dag_object:
        globals()[dag_object.dag_id] = dag_object
