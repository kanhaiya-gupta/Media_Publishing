"""
OwnLens - ML Module: Metadata Manager

Metadata management for training runs.
"""

from typing import Any, Dict, Optional
from datetime import datetime
import uuid
import json
import logging
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class MetadataManager:
    """
    Metadata manager for training runs.
    
    Handles:
    - Saving training runs to ml_model_training_runs
    - Tracking training history
    - Managing training metadata
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize metadata manager.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized metadata manager")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def save_training_run(
        self,
        model_id: str,
        run_metadata: Dict[str, Any],
        metrics: Dict[str, Any],
        **kwargs
    ) -> str:
        """
        Save training run to ml_model_training_runs.
        
        Args:
            model_id: Model ID
            run_metadata: Run metadata (run_name, run_type, dataset paths, etc.)
            metrics: Training metrics (training_metrics, validation_metrics, etc.)
            **kwargs: Additional parameters
        
        Returns:
            run_id (UUID string)
        """
        client = self._get_client()
        
        try:
            # Generate run_id
            run_id = str(uuid.uuid4())
            
            # Prepare data
            run_name = run_metadata.get('run_name', f'Training run for {model_id}')
            run_type = run_metadata.get('run_type', 'TRAINING')
            run_status = run_metadata.get('run_status', 'COMPLETED')
            started_at = run_metadata.get('started_at', datetime.now())
            completed_at = run_metadata.get('completed_at', datetime.now())
            duration_sec = int((completed_at - started_at).total_seconds()) if isinstance(completed_at, datetime) and isinstance(started_at, datetime) else 0
            
            # Prepare JSON fields
            hyperparameters = json.dumps(run_metadata.get('hyperparameters', {}))
            training_metrics = json.dumps(metrics.get('training_metrics', metrics))
            validation_metrics = json.dumps(metrics.get('validation_metrics', {}))
            test_metrics = json.dumps(metrics.get('test_metrics', {}))
            feature_importance = json.dumps(metrics.get('feature_importance', {}))
            
            # Insert into ml_model_training_runs
            # Format values directly in query (ClickHouse parameterized INSERT queries can be problematic)
            # Helper function to escape single quotes for SQL
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    return value.replace("'", "''")
                return str(value)
            
            started_at_str = started_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(started_at, datetime) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            completed_at_str = completed_at.strftime('%Y-%m-%d %H:%M:%S') if isinstance(completed_at, datetime) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_message_str = 'NULL' if run_metadata.get('error_message') is None else f"'{escape_sql(run_metadata.get('error_message'))}'"
            
            query = f"""
            INSERT INTO ml_model_training_runs (
                run_id, model_id, run_name, run_type, run_status,
                started_at, completed_at, duration_sec,
                training_dataset_path, training_dataset_size,
                validation_dataset_path, validation_dataset_size,
                test_dataset_path, test_dataset_size,
                hyperparameters, training_metrics, validation_metrics, test_metrics,
                feature_importance, model_path, model_storage_url,
                error_message, created_by,
                created_at, updated_at
            ) VALUES (
                '{escape_sql(run_id)}', '{escape_sql(model_id)}', '{escape_sql(run_name)}', '{escape_sql(run_type)}', '{escape_sql(run_status)}',
                '{started_at_str}', '{completed_at_str}', {duration_sec},
                '{escape_sql(run_metadata.get('training_dataset_path', ''))}',
                {run_metadata.get('training_dataset_size', 0)},
                '{escape_sql(run_metadata.get('validation_dataset_path', ''))}',
                {run_metadata.get('validation_dataset_size', 0)},
                '{escape_sql(run_metadata.get('test_dataset_path', ''))}',
                {run_metadata.get('test_dataset_size', 0)},
                '{escape_sql(hyperparameters)}', '{escape_sql(training_metrics)}', '{escape_sql(validation_metrics)}', '{escape_sql(test_metrics)}',
                '{escape_sql(feature_importance)}',
                '{escape_sql(run_metadata.get('model_path', ''))}',
                '{escape_sql(run_metadata.get('model_storage_url', ''))}',
                {error_message_str},
                '{escape_sql(run_metadata.get('created_by', 'system'))}',
                now(), now()
            )
            """
            
            client.execute(query)
            
            self.logger.info(f"Saved training run: {run_id} for model {model_id}")
            return run_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def get_training_runs(
        self,
        model_id: Optional[str] = None,
        run_type: Optional[str] = None,
        run_status: Optional[str] = None,
        limit: Optional[int] = None
    ) -> list:
        """
        Get training runs.
        
        Args:
            model_id: Filter by model_id
            run_type: Filter by run_type
            run_status: Filter by run_status
            limit: Limit number of results
        
        Returns:
            List of training run dictionaries
        """
        import pandas as pd
        
        client = self._get_client()
        
        try:
            query = """
            SELECT 
                run_id, model_id, run_name, run_type, run_status,
                started_at, completed_at, duration_sec,
                training_dataset_size, validation_dataset_size, test_dataset_size,
                hyperparameters, training_metrics, validation_metrics, test_metrics,
                error_message, created_at, updated_at
            FROM ml_model_training_runs
            WHERE 1=1
            """
            
            params = []
            
            if model_id:
                query += " AND model_id = ?"
                params.append(model_id)
            
            if run_type:
                query += " AND run_type = ?"
                params.append(run_type)
            
            if run_status:
                query += " AND run_status = ?"
                params.append(run_status)
            
            query += " ORDER BY started_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            results = client.execute(query, params)
            
            if not results:
                return []
            
            runs = []
            for row in results:
                runs.append({
                    'run_id': row[0],
                    'model_id': row[1],
                    'run_name': row[2],
                    'run_type': row[3],
                    'run_status': row[4],
                    'started_at': row[5],
                    'completed_at': row[6],
                    'duration_sec': row[7],
                    'training_dataset_size': row[8],
                    'validation_dataset_size': row[9],
                    'test_dataset_size': row[10],
                    'hyperparameters': json.loads(row[11]) if row[11] else {},
                    'training_metrics': json.loads(row[12]) if row[12] else {},
                    'validation_metrics': json.loads(row[13]) if row[13] else {},
                    'test_metrics': json.loads(row[14]) if row[14] else {},
                    'error_message': row[15],
                    'created_at': row[16],
                    'updated_at': row[17]
                })
            
            return runs
            
        finally:
            if not self.client:
                client.disconnect()

