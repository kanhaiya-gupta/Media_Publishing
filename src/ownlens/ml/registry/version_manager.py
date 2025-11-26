"""
OwnLens - ML Module: Version Manager

Version management for ML models.
"""

from typing import Optional, Dict, Any
import logging
from clickhouse_driver import Client

from ownlens.ml.utils.config import get_ml_config


logger = logging.getLogger(__name__)


class VersionManager:
    """
    Version manager for ML models.
    
    Handles:
    - Creating new model versions
    - Getting latest version
    - Managing version relationships
    """
    
    def __init__(self, client: Optional[Client] = None):
        """
        Initialize version manager.
        
        Args:
            client: ClickHouse client (if None, creates new)
        """
        self.config = get_ml_config()
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized version manager")
    
    def _get_client(self) -> Client:
        """Get ClickHouse client."""
        if self.client:
            return self.client
        return Client(**self.config.get_clickhouse_connection_params())
    
    def get_latest_version(self, model_code: str) -> Optional[str]:
        """
        Get latest version of a model.
        
        Args:
            model_code: Model code (e.g., 'churn_prediction')
        
        Returns:
            Latest version string (e.g., '1.2.0') or None
        """
        client = self._get_client()
        
        try:
            query = """
            SELECT model_version
            FROM ml_model_registry
            WHERE model_code = ? AND is_active = 1 AND is_latest_version = 1
            ORDER BY model_version DESC
            LIMIT 1
            """
            
            results = client.execute(query, [model_code])
            
            if not results:
                return None
            
            return results[0][0]
            
        finally:
            if not self.client:
                client.disconnect()
    
    def create_version(
        self,
        model_code: str,
        parent_model_id: Optional[str] = None,
        version_type: str = 'patch'  # 'major', 'minor', 'patch'
    ) -> str:
        """
        Create new version number.
        
        Args:
            model_code: Model code
            parent_model_id: Parent model ID (previous version)
            version_type: Version increment type ('major', 'minor', 'patch')
        
        Returns:
            New version string (e.g., '1.0.1')
        """
        # Get latest version
        latest_version = self.get_latest_version(model_code)
        
        if latest_version is None:
            # First version
            new_version = '1.0.0'
        else:
            # Increment version
            parts = latest_version.split('.')
            major = int(parts[0])
            minor = int(parts[1])
            patch = int(parts[2]) if len(parts) > 2 else 0
            
            if version_type == 'major':
                major += 1
                minor = 0
                patch = 0
            elif version_type == 'minor':
                minor += 1
                patch = 0
            else:  # patch
                patch += 1
            
            new_version = f"{major}.{minor}.{patch}"
        
        # Update is_latest_version flags
        if parent_model_id:
            self._update_latest_version_flags(model_code, parent_model_id)
        
        self.logger.info(f"Created new version: {model_code} v{new_version}")
        return new_version
    
    def _update_latest_version_flags(self, model_code: str, new_latest_model_id: str):
        """Update is_latest_version flags."""
        client = self._get_client()
        
        try:
            # Set all versions to not latest
            query = """
            ALTER TABLE ml_model_registry
            UPDATE is_latest_version = 0, updated_at = now()
            WHERE model_code = ?
            """
            client.execute(query, [model_code])
            
            # Set new version as latest
            query = """
            ALTER TABLE ml_model_registry
            UPDATE is_latest_version = 1, updated_at = now()
            WHERE model_id = ?
            """
            client.execute(query, [new_latest_model_id])
            
        finally:
            if not self.client:
                client.disconnect()
    
    def get_version_history(self, model_code: str) -> list:
        """
        Get version history for a model.
        
        Args:
            model_code: Model code
        
        Returns:
            List of version dictionaries
        """
        client = self._get_client()
        
        try:
            query = """
            SELECT 
                model_id, model_version, model_status,
                training_date, deployed_at,
                parent_model_id, is_latest_version
            FROM ml_model_registry
            WHERE model_code = ? AND is_active = 1
            ORDER BY model_version DESC
            """
            
            results = client.execute(query, [model_code])
            
            if not results:
                return []
            
            history = []
            for row in results:
                history.append({
                    'model_id': row[0],
                    'model_version': row[1],
                    'model_status': row[2],
                    'training_date': row[3],
                    'deployed_at': row[4],
                    'parent_model_id': row[5],
                    'is_latest_version': bool(row[6])
                })
            
            return history
            
        finally:
            if not self.client:
                client.disconnect()

