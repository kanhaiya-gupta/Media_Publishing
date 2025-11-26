"""
OwnLens - ML Models Domain: Model Registry Service

Service for model registry management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import ModelRegistryRepository
from ...models.ml_models.model_registry import ModelRegistry, ModelRegistryCreate, ModelRegistryUpdate

logger = logging.getLogger(__name__)


class ModelRegistryService(BaseService[ModelRegistry, ModelRegistryCreate, ModelRegistryUpdate, ModelRegistry]):
    """Service for model registry management."""
    
    def __init__(self, repository: ModelRegistryRepository, service_name: str = None):
        """Initialize the model registry service."""
        super().__init__(repository, service_name or "ModelRegistryService")
        self.repository: ModelRegistryRepository = repository
    
    def get_model_class(self):
        """Get the ModelRegistry model class."""
        return ModelRegistry
    
    def get_create_model_class(self):
        """Get the ModelRegistryCreate model class."""
        return ModelRegistryCreate
    
    def get_update_model_class(self):
        """Get the ModelRegistryUpdate model class."""
        return ModelRegistryUpdate
    
    def get_in_db_model_class(self):
        """Get the ModelRegistry model class."""
        return ModelRegistry
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for model registry operations."""
        try:
            # Validate model type
            if hasattr(data, 'model_type'):
                model_type = data.model_type
            else:
                model_type = data.get('model_type') if isinstance(data, dict) else None
            
            if model_type and model_type not in ['churn_prediction', 'user_segmentation', 'content_recommendation', 'conversion_prediction', 'click_prediction', 'engagement_prediction', 'ad_optimization']:
                raise ValidationError(
                    "Invalid model type",
                    "INVALID_MODEL_TYPE"
                )
            
            # Validate accuracy range
            if hasattr(data, 'accuracy'):
                accuracy = data.accuracy
            else:
                accuracy = data.get('accuracy') if isinstance(data, dict) else None
            
            if accuracy is not None and (accuracy < 0 or accuracy > 1):
                raise ValidationError(
                    "Model accuracy must be between 0 and 1",
                    "INVALID_ACCURACY"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_model_registry(self, model_data: ModelRegistryCreate) -> ModelRegistry:
        """Create a new model registry entry."""
        try:
            await self.validate_input(model_data, "create")
            await self.validate_business_rules(model_data, "create")
            
            result = await self.repository.create_model_registry(model_data)
            if not result:
                raise NotFoundError("Failed to create model registry", "CREATE_FAILED")
            
            self.log_operation("create_model_registry", {"model_id": str(result.model_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_model_registry", "create")
            raise
    
    async def get_model_registry_by_id(self, model_id: UUID) -> ModelRegistry:
        """Get model registry by ID."""
        try:
            result = await self.repository.get_model_registry_by_id(model_id)
            if not result:
                raise NotFoundError(f"Model registry with ID {model_id} not found", "MODEL_REGISTRY_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_model_registry_by_id", "read")
            raise
    
    async def get_production_models(self) -> List[ModelRegistry]:
        """Get all models in production."""
        try:
            return await self.repository.get_production_models()
        except Exception as e:
            await self.handle_error(e, "get_production_models", "read")
            return []

