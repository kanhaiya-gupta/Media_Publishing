"""
Base ETL DAG
============

Base class for ETL pipeline DAGs.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from typing import Dict, Any, Optional, List
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


class BaseETLDAG:
    """
    Base class for ETL pipeline DAGs.
    
    Provides common functionality for all ETL DAGs:
    - Default arguments
    - Retry configuration
    - Email notifications
    - Common task dependencies
    """
    
    # Default arguments for all DAGs
    default_args = {
        "owner": "ownlens",
        "depends_on_past": False,
        "email": ["admin@ownlens.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    }
    
    def __init__(
        self,
        dag_id: str,
        description: str,
        schedule_interval: str = "@daily",
        start_date: Optional[Any] = None,
        catchup: bool = False,
        max_active_runs: int = 1,
        default_args: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None
    ):
        """
        Initialize base ETL DAG.
        
        Args:
            dag_id: DAG ID
            description: DAG description
            schedule_interval: Schedule interval (cron expression or preset)
            start_date: Start date for DAG
            catchup: Whether to catch up on missed runs
            max_active_runs: Maximum number of active runs
            default_args: Additional default arguments
            tags: DAG tags
        """
        self.dag_id = dag_id
        self.description = description
        self.schedule_interval = schedule_interval
        self.start_date = start_date or days_ago(1)
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        
        # Merge default args
        merged_args = {**self.default_args}
        if default_args:
            merged_args.update(default_args)
        
        self.default_args = merged_args
        self.tags = tags or []
        
        # Create DAG
        self.dag = DAG(
            dag_id=self.dag_id,
            description=self.description,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            default_args=self.default_args,
            tags=self.tags
        )
    
    def get_dag(self) -> DAG:
        """Get the DAG instance."""
        return self.dag

