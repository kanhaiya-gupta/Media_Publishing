"""
OwnLens - Data Quality Domain: Quality Metric Service

Service for quality metric management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.data_quality import QualityMetricRepository
from ...models.data_quality.quality_metric import QualityMetric, QualityMetricCreate, QualityMetricUpdate

logger = logging.getLogger(__name__)


class QualityMetricService(BaseService[QualityMetric, QualityMetricCreate, QualityMetricUpdate, QualityMetric]):
    """Service for quality metric management."""
    
    def __init__(self, repository: QualityMetricRepository, service_name: str = None):
        """Initialize the quality metric service."""
        super().__init__(repository, service_name or "QualityMetricService")
        self.repository: QualityMetricRepository = repository
    
    def get_model_class(self):
        """Get the QualityMetric model class."""
        return QualityMetric
    
    def get_create_model_class(self):
        """Get the QualityMetricCreate model class."""
        return QualityMetricCreate
    
    def get_update_model_class(self):
        """Get the QualityMetricUpdate model class."""
        return QualityMetricUpdate
    
    def get_in_db_model_class(self):
        """Get the QualityMetric model class."""
        return QualityMetric
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for quality metric operations."""
        return True  # Quality metrics validation is typically handled at the model level
    
    async def create_quality_metric(self, metric_data: QualityMetricCreate) -> QualityMetric:
        """Create a new quality metric."""
        try:
            await self.validate_input(metric_data, "create")
            await self.validate_business_rules(metric_data, "create")
            
            result = await self.repository.create_quality_metric(metric_data)
            if not result:
                raise NotFoundError("Failed to create quality metric", "CREATE_FAILED")
            
            self.log_operation("create_quality_metric", {"metric_id": str(result.metric_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_quality_metric", "create")
            raise
    
    async def get_quality_metrics_by_table(self, table_name: str, limit: int = 100) -> List[QualityMetric]:
        """Get quality metrics for a table."""
        try:
            return await self.repository.get_quality_metrics_by_table(table_name, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_quality_metrics_by_table", "read")
            return []

