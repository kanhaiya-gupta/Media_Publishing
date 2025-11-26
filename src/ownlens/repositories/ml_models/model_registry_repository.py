"""
OwnLens - ML Models Domain: Model Registry Repository

Repository for ml_model_registry table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.ml_models.model_registry import ModelRegistry, ModelRegistryCreate, ModelRegistryUpdate

logger = logging.getLogger(__name__)


class ModelRegistryRepository(BaseRepository[ModelRegistry]):
    """Repository for ml_model_registry table."""

    def __init__(self, connection_manager):
        """Initialize the model registry repository."""
        super().__init__(connection_manager, table_name="ml_model_registry")
        self.model_registry_table = "ml_model_registry"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "model_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_registry"

    # ============================================================================
    # MODEL REGISTRY OPERATIONS
    # ============================================================================

    async def create_model_registry(self, model_data: Union[ModelRegistryCreate, Dict[str, Any]]) -> Optional[ModelRegistry]:
        """
        Create a new model registry record.
        
        Args:
            model_data: Model registry creation data (Pydantic model or dict)
            
        Returns:
            Created model registry with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(model_data, table_name=self.model_registry_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelRegistry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating model registry: {e}")
            return None

    async def get_model_registry_by_id(self, model_id: UUID) -> Optional[ModelRegistry]:
        """Get model registry by ID."""
        try:
            result = await self.get_by_id(model_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelRegistry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model registry by ID {model_id}: {e}")
            return None

    async def get_model_by_name(self, model_name: str, model_type: Optional[str] = None) -> Optional[ModelRegistry]:
        """Get model by name."""
        try:
            filters = {"model_name": model_name}
            if model_type:
                filters["model_type"] = model_type
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return ModelRegistry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model by name {model_name}: {e}")
            return None

    async def get_models_by_type(self, model_type: str) -> List[ModelRegistry]:
        """Get all models by type."""
        try:
            results = await self.get_all(
                filters={"model_type": model_type},
                order_by="model_name"
            )
            return [ModelRegistry(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting models by type {model_type}: {e}")
            return []

    async def get_active_models(self) -> List[ModelRegistry]:
        """Get all active models."""
        try:
            results = await self.get_all(
                filters={"is_active": True},
                order_by="model_name"
            )
            return [ModelRegistry(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active models: {e}")
            return []

    async def get_production_models(self) -> List[ModelRegistry]:
        """Get all models in production."""
        try:
            results = await self.get_all(
                filters={"status": "production"},
                order_by="model_name"
            )
            return [ModelRegistry(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting production models: {e}")
            return []

    async def update_model_registry(self, model_id: UUID, model_data: ModelRegistryUpdate) -> Optional[ModelRegistry]:
        """Update model registry data."""
        try:
            data = model_data.model_dump(exclude_unset=True)
            
            result = await self.update(model_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelRegistry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating model registry {model_id}: {e}")
            return None

    async def delete_model_registry(self, model_id: UUID) -> bool:
        """Delete model registry and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(model_id)
            
        except Exception as e:
            logger.error(f"Error deleting model registry {model_id}: {e}")
            return False

