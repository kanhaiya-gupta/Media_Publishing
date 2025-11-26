"""
Unit Tests for BaseETLDAG
==========================
"""

import pytest
from unittest.mock import Mock, patch
from datetime import timedelta

# Note: DAGs are loaded at import time, so we need to import carefully
try:
    from src.ownlens.processing.airflow.plugins.base.base_dag import BaseETLDAG
    from airflow import DAG
except ImportError:
    # If Airflow is not available in test environment, skip tests
    pytest.skip("Airflow not available in test environment", allow_module_level=True)


class TestBaseETLDAG:
    """Test BaseETLDAG class."""

    def test_init(self):
        """Test DAG initialization."""
        base_dag = BaseETLDAG(
            dag_id="test_dag",
            description="Test DAG",
            schedule_interval="@daily"
        )
        
        assert base_dag.dag_id == "test_dag"
        assert base_dag.description == "Test DAG"
        assert base_dag.schedule_interval == "@daily"
        assert base_dag.dag is not None

    def test_init_with_custom_args(self):
        """Test DAG initialization with custom arguments."""
        custom_args = {
            "retries": 5,
            "retry_delay": timedelta(minutes=10)
        }
        
        base_dag = BaseETLDAG(
            dag_id="test_dag",
            description="Test DAG",
            default_args=custom_args
        )
        
        assert base_dag.default_args["retries"] == 5
        assert base_dag.default_args["retry_delay"] == timedelta(minutes=10)

    def test_init_with_tags(self):
        """Test DAG initialization with tags."""
        tags = ["test", "etl", "pipeline"]
        
        base_dag = BaseETLDAG(
            dag_id="test_dag",
            description="Test DAG",
            tags=tags
        )
        
        assert base_dag.tags == tags
        assert base_dag.dag.tags == tags

    def test_get_dag(self):
        """Test getting DAG instance."""
        base_dag = BaseETLDAG(
            dag_id="test_dag",
            description="Test DAG"
        )
        
        dag = base_dag.get_dag()
        
        assert dag is not None
        assert isinstance(dag, DAG)
        assert dag.dag_id == "test_dag"

    def test_default_args(self):
        """Test default arguments."""
        base_dag = BaseETLDAG(
            dag_id="test_dag",
            description="Test DAG"
        )
        
        default_args = base_dag.default_args
        
        assert "owner" in default_args
        assert "retries" in default_args
        assert "retry_delay" in default_args
        assert default_args["owner"] == "ownlens"
        assert default_args["retries"] == 3

