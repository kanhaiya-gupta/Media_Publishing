"""
OwnLens - ML Models Domain: Model Monitoring Service

Service for model monitoring management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import ModelMonitoringRepository
from ...models.ml_models.model_monitoring import ModelMonitoring, ModelMonitoringCreate, ModelMonitoringUpdate

logger = logging.getLogger(__name__)


class ModelMonitoringService(BaseService[ModelMonitoring, ModelMonitoringCreate, ModelMonitoringUpdate, ModelMonitoring]):
    """Service for model monitoring management."""
    
    def __init__(self, repository: ModelMonitoringRepository, service_name: str = None):
        """Initialize the model monitoring service."""
        super().__init__(repository, service_name or "ModelMonitoringService")
        self.repository: ModelMonitoringRepository = repository
    
    def get_model_class(self):
        """Get the ModelMonitoring model class."""
        return ModelMonitoring
    
    def get_create_model_class(self):
        """Get the ModelMonitoringCreate model class."""
        return ModelMonitoringCreate
    
    def get_update_model_class(self):
        """Get the ModelMonitoringUpdate model class."""
        return ModelMonitoringUpdate
    
    def get_in_db_model_class(self):
        """Get the ModelMonitoring model class."""
        return ModelMonitoring
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for model monitoring operations."""
        try:
            if operation == "create":
                # Validate monitoring date
                if hasattr(data, 'monitoring_date'):
                    monitor_date = data.monitoring_date
                else:
                    monitor_date = data.get('monitoring_date') if isinstance(data, dict) else None
                
                if not monitor_date:
                    raise ValidationError(
                        "Monitoring date is required",
                        "INVALID_DATE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_model_monitoring(self, monitoring_data: ModelMonitoringCreate) -> ModelMonitoring:
        """Create a new model monitoring record."""
        try:
            await self.validate_input(monitoring_data, "create")
            await self.validate_business_rules(monitoring_data, "create")
            
            result = await self.repository.create_model_monitoring(monitoring_data)
            if not result:
                raise NotFoundError("Failed to create model monitoring", "CREATE_FAILED")
            
            self.log_operation("create_model_monitoring", {"monitoring_id": str(result.monitoring_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_model_monitoring", "create")
            raise
    
    async def get_model_monitoring_by_id(self, monitoring_id: UUID) -> ModelMonitoring:
        """Get model monitoring by ID."""
        try:
            result = await self.repository.get_model_monitoring_by_id(monitoring_id)
            if not result:
                raise NotFoundError(f"Model monitoring with ID {monitoring_id} not found", "MODEL_MONITORING_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_model_monitoring_by_id", "read")
            raise
    
    async def get_model_monitoring_by_model(self, model_id: UUID, limit: int = 100) -> List[ModelMonitoring]:
        """Get model monitoring records for a model."""
        try:
            return await self.repository.get_model_monitoring_by_model(model_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_model_monitoring_by_model", "read")
            return []

