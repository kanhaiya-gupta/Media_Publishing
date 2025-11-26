"""
OwnLens - Customer Domain: Churn Prediction Repository

Repository for customer_churn_predictions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.customer.churn_prediction import ChurnPrediction, ChurnPredictionCreate, ChurnPredictionUpdate

logger = logging.getLogger(__name__)


class ChurnPredictionRepository(BaseRepository[ChurnPrediction]):
    """Repository for customer_churn_predictions table."""

    def __init__(self, connection_manager):
        """Initialize the churn prediction repository."""
        super().__init__(connection_manager, table_name="customer_churn_predictions")
        self.churn_predictions_table = "customer_churn_predictions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "prediction_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_churn_predictions"

    # ============================================================================
    # CHURN PREDICTION OPERATIONS
    # ============================================================================

    async def create_churn_prediction(self, prediction_data: Union[ChurnPredictionCreate, Dict[str, Any]]) -> Optional[ChurnPrediction]:
        """
        Create a new churn prediction record.
        
        Args:
            prediction_data: Churn prediction creation data (Pydantic model or dict)
            
        Returns:
            Created churn prediction with generated ID, or None if creation failed
        """
        try:
            if isinstance(prediction_data, dict):
                prediction_data = ChurnPredictionCreate.model_validate(prediction_data)
            data = prediction_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.churn_predictions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ChurnPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating churn prediction: {e}")
            return None

    async def get_churn_prediction_by_id(self, prediction_id: UUID) -> Optional[ChurnPrediction]:
        """Get churn prediction by ID."""
        try:
            result = await self.get_by_id(prediction_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ChurnPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting churn prediction by ID {prediction_id}: {e}")
            return None

    async def get_churn_predictions_by_user(self, user_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[ChurnPrediction]:
        """Get all churn predictions for a user."""
        try:
            filters = {"user_id": user_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="prediction_date",
                limit=limit,
                offset=offset
            )
            return [ChurnPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting churn predictions by user {user_id}: {e}")
            return []

    async def get_latest_churn_prediction(self, user_id: UUID, brand_id: Optional[UUID] = None) -> Optional[ChurnPrediction]:
        """Get the latest churn prediction for a user."""
        try:
            filters = {"user_id": user_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="prediction_date",
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return ChurnPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest churn prediction for user {user_id}: {e}")
            return None

    async def get_high_risk_users(self, brand_id: UUID, risk_level: str = "high", limit: int = 100) -> List[ChurnPrediction]:
        """Get users with high churn risk."""
        try:
            filters = {"brand_id": brand_id, "churn_risk_level": risk_level}
            
            results = await self.get_all(
                filters=filters,
                order_by="churn_probability",
                limit=limit
            )
            return [ChurnPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting high risk users for brand {brand_id}: {e}")
            return []

    async def get_churn_predictions_by_date(self, prediction_date: date, brand_id: Optional[UUID] = None) -> List[ChurnPrediction]:
        """Get all churn predictions for a specific date."""
        try:
            filters = {"prediction_date": prediction_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="churn_probability")
            return [ChurnPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting churn predictions by date {prediction_date}: {e}")
            return []

    async def update_churn_prediction(self, prediction_id: UUID, prediction_data: ChurnPredictionUpdate) -> Optional[ChurnPrediction]:
        """Update churn prediction data."""
        try:
            data = prediction_data.model_dump(exclude_unset=True)
            
            result = await self.update(prediction_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ChurnPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating churn prediction {prediction_id}: {e}")
            return None

    async def delete_churn_prediction(self, prediction_id: UUID) -> bool:
        """Delete churn prediction and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(prediction_id)
            
        except Exception as e:
            logger.error(f"Error deleting churn prediction {prediction_id}: {e}")
            return False

