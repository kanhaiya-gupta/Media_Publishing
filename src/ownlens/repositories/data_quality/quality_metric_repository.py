"""
OwnLens - Data Quality Domain: Quality Metric Repository

Repository for data_quality_metrics table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.data_quality.quality_metric import QualityMetric, QualityMetricCreate, QualityMetricUpdate

logger = logging.getLogger(__name__)


class QualityMetricRepository(BaseRepository[QualityMetric]):
    """Repository for data_quality_metrics table."""

    def __init__(self, connection_manager):
        """Initialize the quality metric repository."""
        super().__init__(connection_manager, table_name="data_quality_metrics")
        self.quality_metrics_table = "data_quality_metrics"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "metric_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "data_quality_metrics"

    # ============================================================================
    # QUALITY METRIC OPERATIONS
    # ============================================================================

    async def create_quality_metric(self, metric_data: QualityMetricCreate) -> Optional[QualityMetric]:
        """
        Create a new quality metric record.
        
        Args:
            metric_data: Quality metric creation data
            
        Returns:
            Created quality metric with generated ID, or None if creation failed
        """
        try:
            data = metric_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.quality_metrics_table)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityMetric(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating quality metric: {e}")
            return None

    async def get_quality_metric_by_id(self, metric_id: UUID) -> Optional[QualityMetric]:
        """Get quality metric by ID."""
        try:
            result = await self.get_by_id(metric_id)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityMetric(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting quality metric by ID {metric_id}: {e}")
            return None

    async def get_quality_metrics_by_table(self, table_name: str, limit: int = 100, offset: int = 0) -> List[QualityMetric]:
        """Get all quality metrics for a table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="metric_date",
                limit=limit,
                offset=offset
            )
            return [QualityMetric(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality metrics by table {table_name}: {e}")
            return []

    async def get_quality_metrics_by_date(self, metric_date: date, table_name: Optional[str] = None) -> List[QualityMetric]:
        """Get quality metrics for a specific date."""
        try:
            filters = {"metric_date": metric_date}
            if table_name:
                filters["table_name"] = table_name
            
            results = await self.get_all(filters=filters, order_by="table_name")
            return [QualityMetric(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality metrics by date {metric_date}: {e}")
            return []

    async def get_latest_quality_metrics(self, table_name: Optional[str] = None) -> List[QualityMetric]:
        """Get the latest quality metrics."""
        try:
            filters = {}
            if table_name:
                filters["table_name"] = table_name
            
            results = await self.get_all(
                filters=filters,
                order_by="metric_date",
                limit=100
            )
            return [QualityMetric(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting latest quality metrics: {e}")
            return []

    async def update_quality_metric(self, metric_id: UUID, metric_data: QualityMetricUpdate) -> Optional[QualityMetric]:
        """Update quality metric data."""
        try:
            data = metric_data.model_dump(exclude_unset=True)
            
            result = await self.update(metric_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityMetric(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating quality metric {metric_id}: {e}")
            return None

    async def delete_quality_metric(self, metric_id: UUID) -> bool:
        """Delete quality metric and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(metric_id)
            
        except Exception as e:
            logger.error(f"Error deleting quality metric {metric_id}: {e}")
            return False

