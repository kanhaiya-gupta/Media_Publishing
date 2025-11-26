"""
Unit Tests for ML Workflow DAG
==============================
"""

import pytest

# Note: DAGs are loaded at import time, so we need to import carefully
try:
    from src.ownlens.processing.airflow.dags.ml_workflow_dag import dag
except ImportError:
    # If Airflow is not available in test environment, skip tests
    pytest.skip("Airflow not available in test environment", allow_module_level=True)


class TestMLWorkflowDAG:
    """Test ML Workflow DAG."""

    def test_dag_initialization(self):
        """Test DAG initialization."""
        assert dag is not None
        assert dag.dag_id == "ml_workflow"

    def test_dag_schedule(self):
        """Test DAG schedule."""
        assert dag.schedule_interval == "0 2 * * *"

    def test_dag_tasks(self):
        """Test DAG tasks."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "ml_workflow_customer" in task_ids
        assert "ml_workflow_editorial" in task_ids

    def test_dag_tags(self):
        """Test DAG tags."""
        assert "ml" in dag.tags
        assert "machine-learning" in dag.tags

