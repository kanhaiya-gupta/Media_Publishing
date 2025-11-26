"""
ML Workflow DAG
===============

Airflow DAG for running complete ML workflow: Train → Predict → Monitor.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from base.base_dag import BaseETLDAG
from operators.python_callable_operator import PythonCallableOperator
from utils.config import get_airflow_config

# Import the orchestration function
from src.ownlens.ml.orchestration import run_ml_workflow

# Get Airflow configuration
config = get_airflow_config()

# Create base DAG
base_dag = BaseETLDAG(
    dag_id="ml_workflow",
    description="Complete ML workflow: Train → Predict → Monitor",
    schedule_interval="0 2 * * *",  # Run daily at 2 AM (after ETL completes at midnight)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args={
        **BaseETLDAG.default_args,
        "execution_timeout": timedelta(hours=4),  # ML workflow may take longer
    },
    tags=["ml", "machine-learning", "training", "predictions", "monitoring"]
)

dag = base_dag.get_dag()

# Define tasks for customer domain
ml_workflow_customer = PythonCallableOperator(
    task_id="ml_workflow_customer",
    python_callable=run_ml_workflow,
    op_kwargs={
        "domain": "customer"
    },
    dag=dag
)

# Define tasks for editorial domain
ml_workflow_editorial = PythonCallableOperator(
    task_id="ml_workflow_editorial",
    python_callable=run_ml_workflow,
    op_kwargs={
        "domain": "editorial"
    },
    dag=dag
)

# Task dependencies - run customer and editorial in parallel
ml_workflow_customer
ml_workflow_editorial

