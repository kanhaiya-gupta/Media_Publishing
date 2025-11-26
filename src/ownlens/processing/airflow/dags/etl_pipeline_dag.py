"""
ETL Pipeline DAG
================

Airflow DAG for running ETL pipeline: Extract → Transform → Load.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from base.base_dag import BaseETLDAG
from operators.python_callable_operator import PythonCallableOperator
from utils.config import get_airflow_config

# Import the orchestration function
from src.ownlens.processing.etl.orchestration import run_etl_pipeline

# Get Airflow configuration
config = get_airflow_config()

# Create base DAG
base_dag = BaseETLDAG(
    dag_id="etl_pipeline",
    description="ETL pipeline: Extract → Transform → Load from PostgreSQL, Kafka, and S3",
    schedule_interval="@daily",  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "extraction", "transformation", "loading", "postgresql", "kafka", "s3"]
)

dag = base_dag.get_dag()

# Define task - uses the orchestration function directly
etl_pipeline = PythonCallableOperator(
    task_id="run_etl_pipeline",
    python_callable=run_etl_pipeline,
    op_kwargs={
        "load": True,
        "load_destinations": ["s3", "postgresql", "clickhouse"]
    },
    dag=dag
)

# Task dependencies
etl_pipeline

