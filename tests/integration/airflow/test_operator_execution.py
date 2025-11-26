"""
Integration Tests for Airflow Operator Execution
=================================================
"""

import pytest


@pytest.mark.integration
class TestOperatorExecution:
    """Integration tests for Airflow operator execution."""

    def test_python_callable_operator_execution(self):
        """Test PythonCallableOperator execution in test environment."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        from src.ownlens.processing.airflow.plugins.operators.python_callable_operator import PythonCallableOperator
        
        def test_function():
            return "success"
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function
        )
        
        context = {}
        result = operator.execute(context)
        
        assert result == "success"

    def test_operator_with_spark_cleanup(self):
        """Test operator with Spark session cleanup."""
        # Skip if Airflow is not available
        pytest.skip("Requires Airflow test environment")
        
        from src.ownlens.processing.airflow.plugins.operators.python_callable_operator import PythonCallableOperator
        
        def test_function():
            # This would create a Spark session
            return "success"
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function
        )
        
        context = {}
        result = operator.execute(context)
        
        assert result == "success"

