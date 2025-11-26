"""
Unit Tests for PythonCallableOperator
======================================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.ownlens.processing.airflow.plugins.operators.python_callable_operator import (
    PythonCallableOperator,
)


class TestPythonCallableOperator:
    """Test PythonCallableOperator class."""

    def test_init(self):
        """Test operator initialization."""
        def test_function(arg1, arg2):
            return arg1 + arg2
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function,
            op_kwargs={"arg1": 1, "arg2": 2}
        )
        
        assert operator.task_id == "test_task"
        assert operator.python_callable == test_function
        assert operator.op_kwargs == {"arg1": 1, "arg2": 2}

    def test_execute_success(self):
        """Test successful execution."""
        def test_function(arg1, arg2):
            return arg1 + arg2
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function,
            op_kwargs={"arg1": 1, "arg2": 2}
        )
        
        context = {}
        result = operator.execute(context)
        
        assert result == 3

    def test_execute_with_context_variables(self):
        """Test execution with context variables."""
        def test_function(arg1, execution_date=None):
            return f"{arg1}_{execution_date}"
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function,
            op_kwargs={"arg1": "test"}
        )
        
        context = {"ds": "2024-01-01"}
        result = operator.execute(context)
        
        assert "test" in result
        assert "2024-01-01" in result

    def test_execute_filters_unknown_kwargs(self):
        """Test that unknown kwargs are filtered out."""
        def test_function(arg1):
            return arg1
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function,
            op_kwargs={"arg1": 1, "unknown_arg": 2}
        )
        
        context = {}
        result = operator.execute(context)
        
        assert result == 1

    @patch('src.ownlens.processing.airflow.plugins.operators.python_callable_operator.SparkSessionManager')
    def test_execute_cleans_up_spark_sessions(self, mock_spark_manager):
        """Test that Spark sessions are cleaned up after execution."""
        def test_function():
            return "success"
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function
        )
        
        context = {}
        result = operator.execute(context)
        
        assert result == "success"
        mock_spark_manager.stop_session.assert_called_once()

    def test_execute_error_handling(self):
        """Test error handling during execution."""
        def test_function():
            raise ValueError("Test error")
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function
        )
        
        context = {}
        
        with pytest.raises(ValueError, match="Test error"):
            operator.execute(context)

    def test_execute_with_data_interval(self):
        """Test execution with data interval context."""
        def test_function(data_interval_start=None, data_interval_end=None):
            return f"{data_interval_start}_{data_interval_end}"
        
        operator = PythonCallableOperator(
            task_id="test_task",
            python_callable=test_function
        )
        
        context = {
            "data_interval_start": "2024-01-01T00:00:00",
            "data_interval_end": "2024-01-02T00:00:00"
        }
        result = operator.execute(context)
        
        assert "2024-01-01T00:00:00" in result
        assert "2024-01-02T00:00:00" in result

