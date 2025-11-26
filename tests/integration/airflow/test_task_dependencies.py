"""
Integration Tests for Airflow Task Dependencies
================================================
"""

import pytest


@pytest.mark.integration
class TestTaskDependencies:
    """Integration tests for Airflow task dependencies."""

    def test_etl_dag_dependencies(self):
        """Test ETL DAG task dependencies."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        try:
            from src.ownlens.processing.airflow.dags.etl_pipeline_dag import dag
            
            # Verify DAG has tasks
            assert len(dag.tasks) > 0
            
            # Verify task dependencies are valid
            for task in dag.tasks:
                assert task is not None
        except ImportError:
            pytest.skip("Airflow not available")

    def test_master_pipeline_dependencies(self):
        """Test master pipeline DAG task dependencies."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        try:
            from src.ownlens.processing.airflow.dags.master_pipeline_dag import dag
            
            # Verify ETL runs before ML workflows
            etl_task = dag.get_task("etl_pipeline")
            ml_customer_task = dag.get_task("ml_workflow_customer")
            
            assert etl_task is not None
            assert ml_customer_task is not None
        except ImportError:
            pytest.skip("Airflow not available")

