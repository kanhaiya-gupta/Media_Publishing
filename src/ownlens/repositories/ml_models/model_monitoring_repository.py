"""
OwnLens - ML Models Domain: Model Monitoring Repository

Repository for ml_model_monitoring table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.ml_models.model_monitoring import ModelMonitoring, ModelMonitoringCreate, ModelMonitoringUpdate

logger = logging.getLogger(__name__)


class ModelMonitoringRepository(BaseRepository[ModelMonitoring]):
    """Repository for ml_model_monitoring table."""

    def __init__(self, connection_manager):
        """Initialize the model monitoring repository."""
        super().__init__(connection_manager, table_name="ml_model_monitoring")
        self.model_monitoring_table = "ml_model_monitoring"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "monitoring_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_monitoring"

    # ============================================================================
    # MODEL MONITORING OPERATIONS
    # ============================================================================

    async def create_model_monitoring(self, monitoring_data: Union[ModelMonitoringCreate, Dict[str, Any]]) -> Optional[ModelMonitoring]:
        """
        Create a new model monitoring record.
        
        Args:
            monitoring_data: Model monitoring creation data (Pydantic model or dict)
            
        Returns:
            Created model monitoring with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(monitoring_data, table_name=self.model_monitoring_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelMonitoring(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating model monitoring: {e}")
            return None

    async def get_model_monitoring_by_id(self, monitoring_id: UUID) -> Optional[ModelMonitoring]:
        """Get model monitoring by ID."""
        try:
            result = await self.get_by_id(monitoring_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelMonitoring(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model monitoring by ID {monitoring_id}: {e}")
            return None

    async def get_monitoring_by_model(self, model_id: UUID, limit: int = 100, offset: int = 0) -> List[ModelMonitoring]:
        """Get all monitoring records for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="monitoring_date",
                limit=limit,
                offset=offset
            )
            return [ModelMonitoring(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting monitoring by model {model_id}: {e}")
            return []

    async def get_latest_monitoring(self, model_id: UUID) -> Optional[ModelMonitoring]:
        """Get the latest monitoring record for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="monitoring_date",
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return ModelMonitoring(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest monitoring for model {model_id}: {e}")
            return None

    async def get_monitoring_by_date(self, monitoring_date: date, model_id: Optional[UUID] = None) -> List[ModelMonitoring]:
        """Get monitoring records for a specific date."""
        try:
            filters = {"monitoring_date": monitoring_date}
            if model_id:
                filters["model_id"] = model_id
            
            results = await self.get_all(filters=filters, order_by="model_id")
            return [ModelMonitoring(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting monitoring by date {monitoring_date}: {e}")
            return []

    async def update_model_monitoring(self, monitoring_id: UUID, monitoring_data: ModelMonitoringUpdate) -> Optional[ModelMonitoring]:
        """Update model monitoring data."""
        try:
            data = monitoring_data.model_dump(exclude_unset=True)
            
            result = await self.update(monitoring_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelMonitoring(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating model monitoring {monitoring_id}: {e}")
            return None

    async def delete_model_monitoring(self, monitoring_id: UUID) -> bool:
        """Delete model monitoring and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(monitoring_id)
            
        except Exception as e:
            logger.error(f"Error deleting model monitoring {monitoring_id}: {e}")
            return False

