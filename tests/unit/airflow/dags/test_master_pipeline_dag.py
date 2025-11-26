"""
Unit Tests for Master Pipeline DAG
===================================
"""

import pytest

# Note: DAGs are loaded at import time, so we need to import carefully
try:
    from src.ownlens.processing.airflow.dags.master_pipeline_dag import dag
except ImportError:
    # If Airflow is not available in test environment, skip tests
    pytest.skip("Airflow not available in test environment", allow_module_level=True)


class TestMasterPipelineDAG:
    """Test Master Pipeline DAG."""

    def test_dag_initialization(self):
        """Test DAG initialization."""
        assert dag is not None
        assert dag.dag_id == "master_pipeline"

    def test_dag_schedule(self):
        """Test DAG schedule."""
        assert dag.schedule_interval == "@daily"

    def test_dag_tasks(self):
        """Test DAG tasks."""
        task_ids = [task.task_id for task in dag.tasks]
        assert "start" in task_ids
        assert "etl_pipeline" in task_ids
        assert "ml_workflow_customer" in task_ids
        assert "ml_workflow_editorial" in task_ids

    def test_dag_task_dependencies(self):
        """Test DAG task dependencies."""
        # ETL should run before ML workflows
        etl_task = dag.get_task("etl_pipeline")
        ml_customer_task = dag.get_task("ml_workflow_customer")
        ml_editorial_task = dag.get_task("ml_workflow_editorial")
        
        # Check that ML tasks depend on ETL
        assert etl_task in ml_customer_task.upstream_list or \
               any(etl_task in dep.upstream_list for dep in ml_customer_task.upstream_list)
        assert etl_task in ml_editorial_task.upstream_list or \
               any(etl_task in dep.upstream_list for dep in ml_editorial_task.upstream_list)

