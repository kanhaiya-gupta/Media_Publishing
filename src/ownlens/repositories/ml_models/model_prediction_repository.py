"""
OwnLens - ML Models Domain: Model Prediction Repository

Repository for ml_model_predictions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.ml_models.model_prediction import ModelPrediction, ModelPredictionCreate, ModelPredictionUpdate

logger = logging.getLogger(__name__)


class ModelPredictionRepository(BaseRepository[ModelPrediction]):
    """Repository for ml_model_predictions table."""

    def __init__(self, connection_manager):
        """Initialize the model prediction repository."""
        super().__init__(connection_manager, table_name="ml_model_predictions")
        self.model_predictions_table = "ml_model_predictions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "prediction_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_predictions"

    # ============================================================================
    # MODEL PREDICTION OPERATIONS
    # ============================================================================

    async def create_model_prediction(self, prediction_data: Union[ModelPredictionCreate, Dict[str, Any]]) -> Optional[ModelPrediction]:
        """
        Create a new model prediction record.
        
        Args:
            prediction_data: Model prediction creation data (Pydantic model or dict)
            
        Returns:
            Created model prediction with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(prediction_data, table_name=self.model_predictions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating model prediction: {e}")
            return None

    async def get_model_prediction_by_id(self, prediction_id: UUID) -> Optional[ModelPrediction]:
        """Get model prediction by ID."""
        try:
            result = await self.get_by_id(prediction_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model prediction by ID {prediction_id}: {e}")
            return None

    async def get_predictions_by_model(self, model_id: UUID, limit: int = 100, offset: int = 0) -> List[ModelPrediction]:
        """Get all predictions for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="prediction_timestamp",
                limit=limit,
                offset=offset
            )
            return [ModelPrediction(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting predictions by model {model_id}: {e}")
            return []

    async def get_predictions_by_date_range(self, start_date: date, end_date: date, model_id: Optional[UUID] = None) -> List[ModelPrediction]:
        """Get predictions within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.model_predictions_table)
                .where_gte("prediction_timestamp", start_date)
                .where_lte("prediction_timestamp", end_date)
            )
            
            if model_id:
                query_builder = query_builder.where_equals("model_id", model_id)
            
            query, params = query_builder.order_by("prediction_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [ModelPrediction(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting predictions by date range: {e}")
            return []

    async def update_model_prediction(self, prediction_id: UUID, prediction_data: ModelPredictionUpdate) -> Optional[ModelPrediction]:
        """Update model prediction data."""
        try:
            data = prediction_data.model_dump(exclude_unset=True)
            
            result = await self.update(prediction_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelPrediction(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating model prediction {prediction_id}: {e}")
            return None

    async def delete_model_prediction(self, prediction_id: UUID) -> bool:
        """Delete model prediction and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(prediction_id)
            
        except Exception as e:
            logger.error(f"Error deleting model prediction {prediction_id}: {e}")
            return False

