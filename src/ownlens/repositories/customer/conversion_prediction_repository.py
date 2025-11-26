"""
OwnLens - Customer Domain: Conversion Prediction Repository

Repository for customer_conversion_predictions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.customer.conversion_prediction import ConversionPrediction, ConversionPredictionCreate, ConversionPredictionUpdate

logger = logging.getLogger(__name__)


class ConversionPredictionRepository(BaseRepository[ConversionPrediction]):
    """Repository for customer_conversion_predictions table."""

    def __init__(self, connection_manager):
        """Initialize the conversion prediction repository."""
        super().__init__(connection_manager, table_name="customer_conversion_predictions")
        self.conversion_predictions_table = "customer_conversion_predictions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "prediction_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_conversion_predictions"

    # ============================================================================
    # CONVERSION PREDICTION OPERATIONS
    # ============================================================================

    async def create_conversion_prediction(self, prediction_data: Union[ConversionPredictionCreate, Dict[str, Any]]) -> Optional[ConversionPrediction]:
        """
        Create a new conversion prediction record.
        
        Args:
            prediction_data: Conversion prediction creation data (Pydantic model or dict)
            
        Returns:
            Created conversion prediction with generated ID, or None if creation failed
        """
        try:
            if isinstance(prediction_data, dict):
                prediction_data = ConversionPredictionCreate.model_validate(prediction_data)
            data = prediction_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.conversion_predictions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ConversionPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating conversion prediction: {e}")
            return None

    async def get_conversion_prediction_by_id(self, prediction_id: UUID) -> Optional[ConversionPrediction]:
        """Get conversion prediction by ID."""
        try:
            result = await self.get_by_id(prediction_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ConversionPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting conversion prediction by ID {prediction_id}: {e}")
            return None

    async def get_conversion_predictions_by_user(self, user_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[ConversionPrediction]:
        """Get all conversion predictions for a user."""
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
            return [ConversionPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting conversion predictions by user {user_id}: {e}")
            return []

    async def get_latest_conversion_prediction(self, user_id: UUID, brand_id: Optional[UUID] = None) -> Optional[ConversionPrediction]:
        """Get the latest conversion prediction for a user."""
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
                return ConversionPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest conversion prediction for user {user_id}: {e}")
            return None

    async def get_high_value_prospects(self, brand_id: UUID, min_probability: float = 0.7, limit: int = 100) -> List[ConversionPrediction]:
        """Get users with high conversion probability."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.conversion_predictions_table)
                .where_equals("brand_id", brand_id)
                .where_gte("conversion_probability", min_probability)
                .order_by("conversion_probability")
                .limit(limit)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [ConversionPrediction(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting high value prospects for brand {brand_id}: {e}")
            return []

    async def get_conversion_predictions_by_date(self, prediction_date: date, brand_id: Optional[UUID] = None) -> List[ConversionPrediction]:
        """Get all conversion predictions for a specific date."""
        try:
            filters = {"prediction_date": prediction_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="conversion_probability")
            return [ConversionPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting conversion predictions by date {prediction_date}: {e}")
            return []

    async def get_converted_users(self, brand_id: UUID, limit: int = 100) -> List[ConversionPrediction]:
        """Get users who actually converted."""
        try:
            filters = {"brand_id": brand_id, "did_convert": True}
            
            results = await self.get_all(
                filters=filters,
                order_by="converted_at",
                limit=limit
            )
            return [ConversionPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting converted users for brand {brand_id}: {e}")
            return []

    async def update_conversion_prediction(self, prediction_id: UUID, prediction_data: ConversionPredictionUpdate) -> Optional[ConversionPrediction]:
        """Update conversion prediction data."""
        try:
            data = prediction_data.model_dump(exclude_unset=True)
            
            result = await self.update(prediction_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ConversionPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating conversion prediction {prediction_id}: {e}")
            return None

    async def delete_conversion_prediction(self, prediction_id: UUID) -> bool:
        """Delete conversion prediction and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(prediction_id)
            
        except Exception as e:
            logger.error(f"Error deleting conversion prediction {prediction_id}: {e}")
            return False

