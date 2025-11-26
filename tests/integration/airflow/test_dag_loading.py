"""
Integration Tests for Airflow DAG Loading
==========================================
"""

import pytest


@pytest.mark.integration
class TestDAGLoading:
    """Integration tests for Airflow DAG loading."""

    def test_etl_dag_loading(self):
        """Test ETL DAG loading and parsing."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        try:
            from src.ownlens.processing.airflow.dags.etl_pipeline_dag import dag
            
            assert dag is not None
            assert dag.dag_id == "etl_pipeline"
            assert len(dag.tasks) > 0
        except ImportError:
            pytest.skip("Airflow not available")

    def test_ml_workflow_dag_loading(self):
        """Test ML workflow DAG loading and parsing."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        try:
            from src.ownlens.processing.airflow.dags.ml_workflow_dag import dag
            
            assert dag is not None
            assert dag.dag_id == "ml_workflow"
            assert len(dag.tasks) > 0
        except ImportError:
            pytest.skip("Airflow not available")

    def test_master_pipeline_dag_loading(self):
        """Test master pipeline DAG loading and parsing."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        try:
            from src.ownlens.processing.airflow.dags.master_pipeline_dag import dag
            
            assert dag is not None
            assert dag.dag_id == "master_pipeline"
            assert len(dag.tasks) > 0
        except ImportError:
            pytest.skip("Airflow not available")

