"""
OwnLens - ML Models Domain: Model Feature Repository

Repository for ml_model_features table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.ml_models.model_feature import ModelFeature, ModelFeatureCreate, ModelFeatureUpdate

logger = logging.getLogger(__name__)


class ModelFeatureRepository(BaseRepository[ModelFeature]):
    """Repository for ml_model_features table."""

    def __init__(self, connection_manager):
        """Initialize the model feature repository."""
        super().__init__(connection_manager, table_name="ml_model_features")
        self.model_features_table = "ml_model_features"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "feature_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_features"

    # ============================================================================
    # MODEL FEATURE OPERATIONS
    # ============================================================================

    async def create_model_feature(self, feature_data: Union[ModelFeatureCreate, Dict[str, Any]]) -> Optional[ModelFeature]:
        """
        Create a new model feature record.
        
        Args:
            feature_data: Model feature creation data (Pydantic model or dict)
            
        Returns:
            Created model feature with generated ID, or None if creation failed
        """
        try:
            # Convert to dict if needed
            if isinstance(feature_data, dict):
                data = feature_data
            else:
                data = feature_data.model_dump(exclude_unset=True)
            
            # Ensure default_value is a string (not integer) - database might convert "0" to 0
            if 'default_value' in data and data['default_value'] is not None:
                if isinstance(data['default_value'], int):
                    data['default_value'] = str(data['default_value'])
                elif not isinstance(data['default_value'], str):
                    data['default_value'] = str(data['default_value']) if data['default_value'] is not None else None
            
            result = await self.create(data, table_name=self.model_features_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure default_value is a string when converting back from database
                if 'default_value' in result and result['default_value'] is not None:
                    if isinstance(result['default_value'], int):
                        result['default_value'] = str(result['default_value'])
                return ModelFeature(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating model feature: {e}")
            return None

    async def get_model_feature_by_id(self, feature_id: UUID) -> Optional[ModelFeature]:
        """Get model feature by ID."""
        try:
            result = await self.get_by_id(feature_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelFeature(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model feature by ID {feature_id}: {e}")
            return None

    async def get_features_by_model(self, model_id: UUID) -> List[ModelFeature]:
        """Get all features for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="feature_name"
            )
            return [ModelFeature(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting features by model {model_id}: {e}")
            return []

    async def get_features_by_type(self, feature_type: str, model_id: Optional[UUID] = None) -> List[ModelFeature]:
        """Get features by type."""
        try:
            filters = {"feature_type": feature_type}
            if model_id:
                filters["model_id"] = model_id
            
            results = await self.get_all(filters=filters, order_by="feature_name")
            return [ModelFeature(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting features by type {feature_type}: {e}")
            return []

    async def update_model_feature(self, feature_id: UUID, feature_data: ModelFeatureUpdate) -> Optional[ModelFeature]:
        """Update model feature data."""
        try:
            data = feature_data.model_dump(exclude_unset=True)
            
            result = await self.update(feature_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelFeature(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating model feature {feature_id}: {e}")
            return None

    async def delete_model_feature(self, feature_id: UUID) -> bool:
        """Delete model feature and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(feature_id)
            
        except Exception as e:
            logger.error(f"Error deleting model feature {feature_id}: {e}")
            return False

