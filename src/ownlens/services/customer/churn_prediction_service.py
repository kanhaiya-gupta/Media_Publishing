"""
OwnLens - Customer Domain: Churn Prediction Service

Service for churn prediction management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import ChurnPredictionRepository
from ...models.customer.churn_prediction import ChurnPrediction, ChurnPredictionCreate, ChurnPredictionUpdate

logger = logging.getLogger(__name__)


class ChurnPredictionService(BaseService[ChurnPrediction, ChurnPredictionCreate, ChurnPredictionUpdate, ChurnPrediction]):
    """Service for churn prediction management."""
    
    def __init__(self, repository: ChurnPredictionRepository, service_name: str = None):
        """Initialize the churn prediction service."""
        super().__init__(repository, service_name or "ChurnPredictionService")
        self.repository: ChurnPredictionRepository = repository
    
    def get_model_class(self):
        """Get the ChurnPrediction model class."""
        return ChurnPrediction
    
    def get_create_model_class(self):
        """Get the ChurnPredictionCreate model class."""
        return ChurnPredictionCreate
    
    def get_update_model_class(self):
        """Get the ChurnPredictionUpdate model class."""
        return ChurnPredictionUpdate
    
    def get_in_db_model_class(self):
        """Get the ChurnPrediction model class."""
        return ChurnPrediction
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for churn prediction operations."""
        try:
            if operation == "create":
                # Validate churn probability
                if hasattr(data, 'churn_probability'):
                    prob = data.churn_probability
                else:
                    prob = data.get('churn_probability') if isinstance(data, dict) else None
                
                if prob is not None and (prob < 0 or prob > 1):
                    raise ValidationError(
                        "Churn probability must be between 0 and 1",
                        "INVALID_PROBABILITY"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_churn_prediction(self, prediction_data: ChurnPredictionCreate) -> ChurnPrediction:
        """Create a new churn prediction."""
        try:
            await self.validate_input(prediction_data, "create")
            await self.validate_business_rules(prediction_data, "create")
            
            result = await self.repository.create_churn_prediction(prediction_data)
            if not result:
                raise NotFoundError("Failed to create churn prediction", "CREATE_FAILED")
            
            self.log_operation("create_churn_prediction", {"prediction_id": str(result.prediction_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_churn_prediction", "create")
            raise
    
    async def get_churn_prediction_by_id(self, prediction_id: UUID) -> ChurnPrediction:
        """Get churn prediction by ID."""
        try:
            result = await self.repository.get_churn_prediction_by_id(prediction_id)
            if not result:
                raise NotFoundError(f"Churn prediction with ID {prediction_id} not found", "CHURN_PREDICTION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_churn_prediction_by_id", "read")
            raise
    
    async def get_churn_predictions_by_user(self, user_id: UUID, limit: int = 100) -> List[ChurnPrediction]:
        """Get churn predictions for a user."""
        try:
            return await self.repository.get_churn_predictions_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_churn_predictions_by_user", "read")
            return []
    
    async def get_high_risk_predictions(self, brand_id: UUID, threshold: float = 0.7, limit: int = 100) -> List[ChurnPrediction]:
        """Get high-risk churn predictions."""
        try:
            return await self.repository.get_high_risk_predictions(brand_id, threshold, limit)
        except Exception as e:
            await self.handle_error(e, "get_high_risk_predictions", "read")
            return []

