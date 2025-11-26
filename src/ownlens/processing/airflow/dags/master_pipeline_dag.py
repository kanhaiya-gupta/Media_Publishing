"""
Master Pipeline DAG
===================

Master DAG that orchestrates the complete data pipeline:
1. ETL Pipeline (Extract → Transform → Load)
2. ML Workflow (Train → Predict → Monitor)

ML workflows run after ETL completes.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
try:
    from airflow.operators.empty import EmptyOperator as DummyOperator
except ImportError:
    # Fallback for older Airflow versions
    from airflow.operators.dummy import DummyOperator

from base.base_dag import BaseETLDAG
from operators.python_callable_operator import PythonCallableOperator
from utils.config import get_airflow_config

# Import orchestration functions
from src.ownlens.processing.etl.orchestration import run_etl_pipeline
from src.ownlens.ml.orchestration import run_ml_workflow

# Get Airflow configuration
config = get_airflow_config()

# Create base DAG
base_dag = BaseETLDAG(
    dag_id="master_pipeline",
    description="Master data pipeline: ETL → ML workflow",
    schedule_interval="@daily",  # Run daily at midnight
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args={
        **BaseETLDAG.default_args,
        "execution_timeout": timedelta(hours=6),  # Complete pipeline may take longer
    },
    tags=["master", "etl", "ml", "data-pipeline"]
)

dag = base_dag.get_dag()

# Define start task
start = DummyOperator(
    task_id="start",
    dag=dag
)

# ETL Pipeline - Extract → Transform → Load
etl_pipeline = PythonCallableOperator(
    task_id="etl_pipeline",
    python_callable=run_etl_pipeline,
    op_kwargs={
        "load": True,
        "load_destinations": ["s3", "postgresql", "clickhouse"]
    },
    dag=dag
)

# ML Workflow - Customer Domain
ml_workflow_customer = PythonCallableOperator(
    task_id="ml_workflow_customer",
    python_callable=run_ml_workflow,
    op_kwargs={
        "domain": "customer"
    },
    dag=dag
)

# ML Workflow - Editorial Domain
ml_workflow_editorial = PythonCallableOperator(
    task_id="ml_workflow_editorial",
    python_callable=run_ml_workflow,
    op_kwargs={
        "domain": "editorial"
    },
    dag=dag
)

# Define end task
end = DummyOperator(
    task_id="end",
    dag=dag
)

# Task dependencies
# ETL runs first, then ML workflows run in parallel after ETL completes
start >> etl_pipeline >> [ml_workflow_customer, ml_workflow_editorial] >> end

