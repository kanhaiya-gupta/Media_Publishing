"""
OwnLens - ML Module: Model Registry

Model registry client for saving and loading models from ClickHouse.
"""

from typing import Any, Dict, Optional, List
from datetime import datetime
import uuid
import json
import logging
import pandas as pd
from pathlib import Path

from clickhouse_driver import Client
from ownlens.ml.utils.config import get_ml_config
from ownlens.ml.utils.serialization import save_model, load_model


logger = logging.getLogger(__name__)


class ModelRegistry:
    """
    Model registry client for managing ML models in ClickHouse.
    
    Handles:
    - Registering models to ml_model_registry
    - Saving model features to ml_model_features
    - Loading models from registry
    - Listing models
    - Updating model status
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize model registry.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized model registry")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def register_model(
        self,
        model: Any,
        metadata: Dict[str, Any],
        metrics: Dict[str, Any],
        model_path: Optional[str] = None,
        save_to_storage: bool = True,
        **kwargs
    ) -> str:
        """
        Register model to ml_model_registry.
        
        Args:
            model: ML model object
            metadata: Model metadata (model_code, model_type, algorithm, etc.)
            metrics: Training metrics (accuracy, AUC, feature_importance, etc.)
            model_path: Path to save model (if None, auto-generates)
            save_to_storage: If True, save model to S3/MinIO
            **kwargs: Additional parameters
        
        Returns:
            model_id (UUID string)
        """
        client = self._get_client()
        
        try:
            # Generate model_id
            model_id = str(uuid.uuid4())
            
            # Generate model_path if not provided
            if model_path is None:
                model_code = metadata.get('model_code', 'unknown')
                model_version = metadata.get('model_version', '1.0.0')
                model_dir = Path(self.config.ml_models_dir) / metadata.get('model_type', 'unknown') / model_code
                model_dir.mkdir(parents=True, exist_ok=True)
                model_path = str(model_dir / f"{model_code}_{model_version}.pkl")
            
            # Save model to storage
            if save_to_storage:
                save_success = save_model(model, model_path, metadata)
                if not save_success:
                    raise RuntimeError(f"Failed to save model to {model_path}")
                self.logger.info(f"Model saved to {model_path}")
            
            # Prepare registry data
            model_name = metadata.get('model_name', metadata.get('model_code', 'Unknown Model'))
            model_code = metadata.get('model_code', 'unknown')
            model_version = metadata.get('model_version', '1.0.0')
            model_type = metadata.get('model_type', 'unknown')
            domain = metadata.get('domain', model_type)
            algorithm = metadata.get('algorithm', 'Unknown')
            model_framework = metadata.get('model_framework', 'scikit-learn')
            model_format = metadata.get('model_format', 'pickle')
            
            # Prepare JSON fields
            performance_metrics = json.dumps(metrics.get('performance_metrics', metrics))
            feature_importance = json.dumps(metrics.get('feature_importance', {}))
            evaluation_metrics = json.dumps(metrics.get('evaluation_metrics', {}))
            tags = metadata.get('tags', [])
            
            # Insert into ml_model_registry
            # Format values directly in query (ClickHouse parameterized INSERT queries can be problematic)
            # Helper function to escape single quotes for SQL
            def escape_sql(value):
                if value is None:
                    return 'NULL'
                if isinstance(value, str):
                    return value.replace("'", "''")
                return str(value)
            
            training_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            deployed_at_str = 'NULL' if metadata.get('deployed_at') is None else f"'{escape_sql(str(metadata.get('deployed_at')))}'"
            parent_model_id_str = 'NULL' if metadata.get('parent_model_id') is None else f"'{escape_sql(metadata.get('parent_model_id'))}'"
            
            # Format tags as ClickHouse array (Array(String)) - not JSON string
            if tags and len(tags) > 0:
                tags_array = "[" + ", ".join([f"'{escape_sql(str(tag))}'" for tag in tags]) + "]"
            else:
                tags_array = "[]"
            
            query = f"""
            INSERT INTO ml_model_registry (
                model_id, model_name, model_code, model_version,
                model_type, domain, algorithm, model_framework, model_format,
                model_path, model_storage_type, model_storage_url,
                description, tags, metadata,
                training_dataset_path, training_dataset_size,
                training_date, training_duration_sec, training_environment,
                performance_metrics, feature_importance, evaluation_dataset_metrics,
                model_status, is_active, deployed_at, deployment_environment,
                company_id, brand_id, parent_model_id, is_latest_version,
                created_at, updated_at, created_by
            ) VALUES (
                '{escape_sql(model_id)}', '{escape_sql(model_name)}', '{escape_sql(model_code)}', '{escape_sql(model_version)}',
                '{escape_sql(model_type)}', '{escape_sql(domain)}', '{escape_sql(algorithm)}', '{escape_sql(model_framework)}', '{escape_sql(model_format)}',
                '{escape_sql(model_path)}', 'local', '{escape_sql(model_path)}',
                '{escape_sql(metadata.get('description', ''))}',
                {tags_array},
                '{escape_sql(json.dumps(metadata.get('metadata', {})))}',
                '{escape_sql(metadata.get('training_dataset_path', ''))}',
                {metrics.get('n_samples', 0)},
                '{training_date_str}',
                {metrics.get('training_duration_sec', 0)},
                '{escape_sql(metadata.get('training_environment', 'local'))}',
                '{escape_sql(performance_metrics)}',
                '{escape_sql(feature_importance)}',
                '{escape_sql(evaluation_metrics)}',
                '{escape_sql(metadata.get('model_status', 'TRAINING'))}',
                1,
                {deployed_at_str},
                '{escape_sql(metadata.get('deployment_environment', 'development'))}',
                '{escape_sql(metadata.get('company_id', ''))}',
                '{escape_sql(metadata.get('brand_id', ''))}',
                {parent_model_id_str},
                1,
                now(), now(), '{escape_sql(metadata.get('created_by', 'system'))}'
            )
            """
            
            client.execute(query)
            
            # Save features to ml_model_features
            if 'feature_importance' in metrics:
                self._save_model_features(model_id, metrics['feature_importance'], metadata.get('feature_names', []))
            
            self.logger.info(f"Model registered: {model_id} ({model_code} v{model_version})")
            return model_id
            
        finally:
            if not self.client:
                client.disconnect()
    
    def _save_model_features(
        self,
        model_id: str,
        feature_importance: Dict[str, float],
        feature_names: List[str] = None
    ):
        """Save model features to ml_model_features table."""
        client = self._get_client()
        
        try:
            # Sort features by importance
            sorted_features = sorted(
                feature_importance.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Prepare feature data
            features_data = []
            for rank, (feature_name, importance) in enumerate(sorted_features, 1):
                feature_id = str(uuid.uuid4())
                feature_code = feature_name
                feature_type = 'numerical'  # Default, can be enhanced
                
                features_data.append((
                    feature_id, model_id, feature_name, feature_code,
                    feature_type, 'FLOAT', '', 1, '',  # description, is_required, default_value
                    float(importance), rank,
                    0.0, 0.0, 0.0, 0.0,  # min, max, mean, std (can be enhanced)
                    datetime.now(), datetime.now()
                ))
            
            # Insert features
            if features_data:
                query = """
                INSERT INTO ml_model_features (
                    feature_id, model_id, feature_name, feature_code,
                    feature_type, feature_data_type, description, is_required, default_value,
                    feature_importance, feature_rank,
                    min_value, max_value, mean_value, std_value,
                    created_at, updated_at
                ) VALUES
                """
                
                client.execute(query, features_data)
                self.logger.info(f"Saved {len(features_data)} features for model {model_id}")
        
        finally:
            if not self.client:
                client.disconnect()
    
    def get_model(
        self,
        model_code: str,
        model_version: Optional[str] = None,
        status: str = 'PRODUCTION'
    ) -> tuple:
        """
        Load model from registry.
        
        Args:
            model_code: Model code (e.g., 'churn_prediction')
            model_version: Model version (if None, gets latest)
            status: Model status filter (default: 'PRODUCTION')
        
        Returns:
            Tuple of (model, metadata)
        """
        client = self._get_client()
        
        try:
            # Build query
            query = """
            SELECT 
                model_id, model_name, model_code, model_version,
                model_type, domain, algorithm, model_framework, model_format,
                model_path, model_storage_url, model_storage_type,
                description, tags, metadata,
                performance_metrics, feature_importance, evaluation_dataset_metrics,
                model_status, is_active, deployed_at, deployment_environment,
                company_id, brand_id, parent_model_id, is_latest_version,
                training_date, training_duration_sec,
                created_at, updated_at
            FROM ml_model_registry
            WHERE model_code = ? AND is_active = 1
            """
            
            params = [model_code]
            
            if model_version:
                query += " AND model_version = ?"
                params.append(model_version)
            else:
                query += " AND is_latest_version = 1"
            
            if status:
                query += " AND model_status = ?"
                params.append(status)
            
            query += " ORDER BY model_version DESC LIMIT 1"
            
            results = client.execute(query, params)
            
            if not results:
                raise ValueError(f"Model not found: {model_code} v{model_version or 'latest'}")
            
            # Parse result
            row = results[0]
            metadata = {
                'model_id': row[0],
                'model_name': row[1],
                'model_code': row[2],
                'model_version': row[3],
                'model_type': row[4],
                'domain': row[5],
                'algorithm': row[6],
                'model_framework': row[7],
                'model_format': row[8],
                'model_path': row[9],
                'model_storage_url': row[10],
                'model_storage_type': row[11],
                'description': row[12],
                'tags': row[13] if row[13] else [],
                'metadata': json.loads(row[14]) if row[14] else {},
                'performance_metrics': json.loads(row[15]) if row[15] else {},
                'feature_importance': json.loads(row[16]) if row[16] else {},
                'evaluation_metrics': json.loads(row[17]) if row[17] else {},
                'model_status': row[18],
                'is_active': bool(row[19]),
                'deployed_at': row[20],
                'deployment_environment': row[21],
                'company_id': row[22],
                'brand_id': row[23],
                'parent_model_id': row[24],
                'is_latest_version': bool(row[25]),
                'training_date': row[26],
                'training_duration_sec': row[27],
                'created_at': row[28],
                'updated_at': row[29]
            }
            
            # Load model from storage
            model_path = metadata['model_path']
            model, _ = load_model(model_path)
            
            self.logger.info(f"Loaded model: {model_code} v{metadata['model_version']} from {model_path}")
            return model, metadata
            
        finally:
            if not self.client:
                client.disconnect()
    
    def list_models(
        self,
        domain: Optional[str] = None,
        status: Optional[str] = None,
        model_type: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        List models from registry.
        
        Args:
            domain: Filter by domain ('customer', 'editorial', 'company')
            status: Filter by status ('PRODUCTION', 'STAGING', etc.)
            model_type: Filter by model type
            limit: Limit number of results
        
        Returns:
            DataFrame with model list
        """
        import pandas as pd
        
        client = self._get_client()
        
        try:
            query = """
            SELECT 
                model_id, model_name, model_code, model_version,
                model_type, domain, algorithm, model_framework,
                model_status, is_active, deployed_at,
                training_date, training_duration_sec,
                company_id, brand_id,
                created_at, updated_at
            FROM ml_model_registry
            WHERE is_active = 1
            """
            
            params = []
            
            if domain:
                query += " AND domain = ?"
                params.append(domain)
            
            if status:
                query += " AND model_status = ?"
                params.append(status)
            
            if model_type:
                query += " AND model_type = ?"
                params.append(model_type)
            
            query += " ORDER BY model_code, model_version DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            results = client.execute(query, params)
            
            if not results:
                return pd.DataFrame()
            
            columns = [
                'model_id', 'model_name', 'model_code', 'model_version',
                'model_type', 'domain', 'algorithm', 'model_framework',
                'model_status', 'is_active', 'deployed_at',
                'training_date', 'training_duration_sec',
                'company_id', 'brand_id',
                'created_at', 'updated_at'
            ]
            
            df = pd.DataFrame(results, columns=columns)
            self.logger.info(f"Listed {len(df)} models")
            return df
            
        finally:
            if not self.client:
                client.disconnect()
    
    def update_model_status(
        self,
        model_id: str,
        new_status: str,
        deployment_environment: Optional[str] = None
    ) -> bool:
        """
        Update model status.
        
        Args:
            model_id: Model ID
            new_status: New status ('TRAINING', 'VALIDATED', 'STAGING', 'PRODUCTION', 'DEPRECATED')
            deployment_environment: Deployment environment (if deploying)
        
        Returns:
            True if successful
        """
        client = self._get_client()
        
        try:
            query = """
            ALTER TABLE ml_model_registry
            UPDATE 
                model_status = ?,
                deployed_at = ?,
                deployment_environment = ?,
                updated_at = now()
            WHERE model_id = ?
            """
            
            deployed_at = datetime.now() if new_status == 'PRODUCTION' else None
            
            params = [
                new_status,
                deployed_at,
                deployment_environment or 'development',
                model_id
            ]
            
            client.execute(query, params)
            
            self.logger.info(f"Updated model {model_id} status to {new_status}")
            return True
            
        finally:
            if not self.client:
                client.disconnect()

