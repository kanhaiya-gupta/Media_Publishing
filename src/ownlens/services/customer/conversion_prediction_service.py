"""
OwnLens - Customer Domain: Conversion Prediction Service

Service for conversion prediction management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import ConversionPredictionRepository
from ...models.customer.conversion_prediction import ConversionPrediction, ConversionPredictionCreate, ConversionPredictionUpdate

logger = logging.getLogger(__name__)


class ConversionPredictionService(BaseService[ConversionPrediction, ConversionPredictionCreate, ConversionPredictionUpdate, ConversionPrediction]):
    """Service for conversion prediction management."""
    
    def __init__(self, repository: ConversionPredictionRepository, service_name: str = None):
        """Initialize the conversion prediction service."""
        super().__init__(repository, service_name or "ConversionPredictionService")
        self.repository: ConversionPredictionRepository = repository
    
    def get_model_class(self):
        """Get the ConversionPrediction model class."""
        return ConversionPrediction
    
    def get_create_model_class(self):
        """Get the ConversionPredictionCreate model class."""
        return ConversionPredictionCreate
    
    def get_update_model_class(self):
        """Get the ConversionPredictionUpdate model class."""
        return ConversionPredictionUpdate
    
    def get_in_db_model_class(self):
        """Get the ConversionPrediction model class."""
        return ConversionPrediction
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for conversion prediction operations."""
        try:
            if operation == "create":
                # Validate conversion probability
                if hasattr(data, 'conversion_probability'):
                    prob = data.conversion_probability
                else:
                    prob = data.get('conversion_probability') if isinstance(data, dict) else None
                
                if prob is not None and (prob < 0 or prob > 1):
                    raise ValidationError(
                        "Conversion probability must be between 0 and 1",
                        "INVALID_PROBABILITY"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_conversion_prediction(self, prediction_data: ConversionPredictionCreate) -> ConversionPrediction:
        """Create a new conversion prediction."""
        try:
            await self.validate_input(prediction_data, "create")
            await self.validate_business_rules(prediction_data, "create")
            
            result = await self.repository.create_conversion_prediction(prediction_data)
            if not result:
                raise NotFoundError("Failed to create conversion prediction", "CREATE_FAILED")
            
            self.log_operation("create_conversion_prediction", {"prediction_id": str(result.prediction_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_conversion_prediction", "create")
            raise
    
    async def get_conversion_prediction_by_id(self, prediction_id: UUID) -> ConversionPrediction:
        """Get conversion prediction by ID."""
        try:
            result = await self.repository.get_conversion_prediction_by_id(prediction_id)
            if not result:
                raise NotFoundError(f"Conversion prediction with ID {prediction_id} not found", "CONVERSION_PREDICTION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_conversion_prediction_by_id", "read")
            raise
    
    async def get_conversion_predictions_by_user(self, user_id: UUID, limit: int = 100) -> List[ConversionPrediction]:
        """Get conversion predictions for a user."""
        try:
            return await self.repository.get_conversion_predictions_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_conversion_predictions_by_user", "read")
            return []
    
    async def get_high_probability_predictions(self, brand_id: UUID, threshold: float = 0.7, limit: int = 100) -> List[ConversionPrediction]:
        """Get high-probability conversion predictions."""
        try:
            return await self.repository.get_high_probability_predictions(brand_id, threshold, limit)
        except Exception as e:
            await self.handle_error(e, "get_high_probability_predictions", "read")
            return []








