"""
Unit Tests for ETL Pipeline DAG
===============================
"""

import pytest
from unittest.mock import Mock, patch

# Note: DAGs are loaded at import time, so we need to import carefully
try:
    from src.ownlens.processing.airflow.dags.etl_pipeline_dag import dag
except ImportError:
    # If Airflow is not available in test environment, skip tests
    pytest.skip("Airflow not available in test environment", allow_module_level=True)


class TestETLPipelineDAG:
    """Test ETL Pipeline DAG."""

    def test_dag_initialization(self):
        """Test DAG initialization."""
        assert dag is not None
        assert dag.dag_id == "etl_pipeline"

    def test_dag_schedule(self):
        """Test DAG schedule."""
        assert dag.schedule_interval == "@daily"

    def test_dag_tasks(self):
        """Test DAG tasks."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "run_etl_pipeline" in task_ids

    def test_dag_tags(self):
        """Test DAG tags."""
        assert "etl" in dag.tags
        assert "extraction" in dag.tags

