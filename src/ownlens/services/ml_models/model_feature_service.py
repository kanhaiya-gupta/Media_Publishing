"""
OwnLens - ML Models Domain: Model Feature Service

Service for model feature management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import ModelFeatureRepository
from ...models.ml_models.model_feature import ModelFeature, ModelFeatureCreate, ModelFeatureUpdate

logger = logging.getLogger(__name__)


class ModelFeatureService(BaseService[ModelFeature, ModelFeatureCreate, ModelFeatureUpdate, ModelFeature]):
    """Service for model feature management."""
    
    def __init__(self, repository: ModelFeatureRepository, service_name: str = None):
        """Initialize the model feature service."""
        super().__init__(repository, service_name or "ModelFeatureService")
        self.repository: ModelFeatureRepository = repository
    
    def get_model_class(self):
        """Get the ModelFeature model class."""
        return ModelFeature
    
    def get_create_model_class(self):
        """Get the ModelFeatureCreate model class."""
        return ModelFeatureCreate
    
    def get_update_model_class(self):
        """Get the ModelFeatureUpdate model class."""
        return ModelFeatureUpdate
    
    def get_in_db_model_class(self):
        """Get the ModelFeature model class."""
        return ModelFeature
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for model feature operations."""
        try:
            if operation == "create":
                # Validate feature name
                if hasattr(data, 'feature_name'):
                    name = data.feature_name
                else:
                    name = data.get('feature_name') if isinstance(data, dict) else None
                
                if not name or len(name.strip()) == 0:
                    raise ValidationError(
                        "Feature name is required",
                        "INVALID_NAME"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_model_feature(self, feature_data: ModelFeatureCreate) -> ModelFeature:
        """Create a new model feature."""
        try:
            await self.validate_input(feature_data, "create")
            await self.validate_business_rules(feature_data, "create")
            
            result = await self.repository.create_model_feature(feature_data)
            if not result:
                raise NotFoundError("Failed to create model feature", "CREATE_FAILED")
            
            self.log_operation("create_model_feature", {"feature_id": str(result.feature_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_model_feature", "create")
            raise
    
    async def get_model_feature_by_id(self, feature_id: UUID) -> ModelFeature:
        """Get model feature by ID."""
        try:
            result = await self.repository.get_model_feature_by_id(feature_id)
            if not result:
                raise NotFoundError(f"Model feature with ID {feature_id} not found", "MODEL_FEATURE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_model_feature_by_id", "read")
            raise
    
    async def get_model_features_by_model(self, model_id: UUID, limit: int = 100) -> List[ModelFeature]:
        """Get model features for a model."""
        try:
            return await self.repository.get_model_features_by_model(model_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_model_features_by_model", "read")
            return []

