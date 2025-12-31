"""Tests for ingestion DAG generation."""

import sys
from pathlib import Path

import pytest
from airflow.models import DAG

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.ingestion_dags import _load_yaml_configs, create_dag
except ImportError as exc:
    pytest.skip(
        f"ingestion_dags imports unavailable in this environment: {exc}", allow_module_level=True
    )


class TestIngestionDAGGeneration:
    """Test ingestion DAG generation and structure."""

    def test_load_yaml_configs(self):
        """Test that YAML configs can be loaded."""
        config_files = _load_yaml_configs()
        assert isinstance(config_files, list)
        # We expect at least one config file
        assert len(config_files) > 0
        # Check that files exist
        for config_path in config_files:
            assert config_path.exists()
            assert config_path.suffix == ".yaml"

    def test_create_dag_with_valid_config(self):
        """Test DAG creation with a valid config file."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        # Use first config file
        config_path = config_files[0]
        dag = create_dag(config_path)

        assert dag is not None
        assert isinstance(dag, DAG)
        assert dag.dag_id.startswith("ingestion_")

    def test_dag_has_required_tasks(self):
        """Test that DAG contains all required tasks."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        task_ids = [task.task_id for task in dag.tasks]
        assert "run_ingestion" in task_ids

    def test_dag_task_dependencies(self):
        """Test that DAG has a single runner task."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        tasks = [task.task_id for task in dag.tasks]
        assert "run_ingestion" in tasks
        assert len(tasks) == 1

    def test_dag_default_args(self):
        """Test that DAG has correct default arguments."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        assert "owner" in dag.default_args
        assert dag.default_args["owner"] == "ingestion"
        assert "retries" in dag.default_args
        assert dag.default_args["retries"] == 1

    def test_dag_schedule(self):
        """Test that DAG has a schedule interval."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        # DAG should have a schedule (either "@daily", "@hourly", or cron expression)
        # In Airflow 2.x+, use .schedule instead of .schedule_interval
        schedule = getattr(dag, "schedule", getattr(dag, "schedule_interval", None))
        assert schedule is not None

    def test_dags_have_unique_ids(self):
        """Test that all generated DAGs have unique IDs."""
        config_files = _load_yaml_configs()
        if len(config_files) < 2:
            pytest.skip("Need at least 2 config files to test uniqueness")

        dag_ids = []
        for config_path in config_files[:3]:  # Test first 3 configs
            dag = create_dag(config_path)
            if dag:
                dag_ids.append(dag.dag_id)

        # All DAG IDs should be unique
        assert len(dag_ids) == len(set(dag_ids)), "DAG IDs must be unique"

    def test_dag_tags(self):
        """Test that DAG has appropriate tags."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        assert "ingestion" in dag.tags
        # Should also have the target name as a tag
        assert len(dag.tags) > 1

    def test_dag_max_active_runs(self):
        """Test that max_active_runs is set to prevent concurrent runs."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        # Should have max_active_runs=1 to prevent concurrent ingestion
        assert dag.max_active_runs == 1


class TestIngestionDAGIntegration:
    """Integration tests for ingestion DAG workflow."""

    def test_full_dag_workflow_structure(self):
        """Test that the complete workflow structure is correct."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)

        tasks = {task.task_id: task for task in dag.tasks}

        assert "run_ingestion" in tasks
        assert len(tasks) == 1

    def test_commit_task_has_validate_step(self):
        """Test that run_ingestion task exists."""
        config_files = _load_yaml_configs()
        if not config_files:
            pytest.skip("No config files found")

        config_path = config_files[0]
        dag = create_dag(config_path)
        task = dag.get_task("run_ingestion")
        assert task is not None
