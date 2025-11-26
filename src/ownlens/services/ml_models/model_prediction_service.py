"""
OwnLens - ML Models Domain: Model Prediction Service

Service for model prediction management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import ModelPredictionRepository
from ...models.ml_models.model_prediction import ModelPrediction, ModelPredictionCreate, ModelPredictionUpdate

logger = logging.getLogger(__name__)


class ModelPredictionService(BaseService[ModelPrediction, ModelPredictionCreate, ModelPredictionUpdate, ModelPrediction]):
    """Service for model prediction management."""
    
    def __init__(self, repository: ModelPredictionRepository, service_name: str = None):
        """Initialize the model prediction service."""
        super().__init__(repository, service_name or "ModelPredictionService")
        self.repository: ModelPredictionRepository = repository
    
    def get_model_class(self):
        """Get the ModelPrediction model class."""
        return ModelPrediction
    
    def get_create_model_class(self):
        """Get the ModelPredictionCreate model class."""
        return ModelPredictionCreate
    
    def get_update_model_class(self):
        """Get the ModelPredictionUpdate model class."""
        return ModelPredictionUpdate
    
    def get_in_db_model_class(self):
        """Get the ModelPrediction model class."""
        return ModelPrediction
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for model prediction operations."""
        return True  # Predictions are typically append-only
    
    async def create_model_prediction(self, prediction_data: ModelPredictionCreate) -> ModelPrediction:
        """Create a new model prediction."""
        try:
            await self.validate_input(prediction_data, "create")
            await self.validate_business_rules(prediction_data, "create")
            
            result = await self.repository.create_model_prediction(prediction_data)
            if not result:
                raise NotFoundError("Failed to create model prediction", "CREATE_FAILED")
            
            self.log_operation("create_model_prediction", {"prediction_id": str(result.prediction_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_model_prediction", "create")
            raise
    
    async def get_predictions_by_model(self, model_id: UUID, limit: int = 100) -> List[ModelPrediction]:
        """Get predictions for a model."""
        try:
            return await self.repository.get_predictions_by_model(model_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_predictions_by_model", "read")
            return []

