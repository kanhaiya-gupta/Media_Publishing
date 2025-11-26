"""
OwnLens - ML Models Domain: Model AB Test Repository

Repository for ml_model_ab_tests table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.ml_models.model_ab_test import ModelABTest, ModelABTestCreate, ModelABTestUpdate

logger = logging.getLogger(__name__)


class ModelABTestRepository(BaseRepository[ModelABTest]):
    """Repository for ml_model_ab_tests table."""

    def __init__(self, connection_manager):
        """Initialize the model AB test repository."""
        super().__init__(connection_manager, table_name="ml_model_ab_tests")
        self.model_ab_tests_table = "ml_model_ab_tests"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "test_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_ab_tests"

    # ============================================================================
    # MODEL AB TEST OPERATIONS
    # ============================================================================

    async def create_model_ab_test(self, test_data: Union[ModelABTestCreate, Dict[str, Any]]) -> Optional[ModelABTest]:
        """
        Create a new model AB test record.
        
        Args:
            test_data: Model AB test creation data (Pydantic model or dict)
            
        Returns:
            Created model AB test with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(test_data, table_name=self.model_ab_tests_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelABTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating model AB test: {e}")
            return None

    async def get_model_ab_test_by_id(self, test_id: UUID) -> Optional[ModelABTest]:
        """Get model AB test by ID."""
        try:
            result = await self.get_by_id(test_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelABTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting model AB test by ID {test_id}: {e}")
            return None

    async def get_ab_tests_by_model(self, model_id: UUID) -> List[ModelABTest]:
        """Get all AB tests for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="test_start_date"
            )
            return [ModelABTest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting AB tests by model {model_id}: {e}")
            return []

    async def get_active_ab_tests(self) -> List[ModelABTest]:
        """Get all active AB tests."""
        try:
            results = await self.get_all(
                filters={"status": "active"},
                order_by="test_start_date"
            )
            return [ModelABTest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active AB tests: {e}")
            return []

    async def get_completed_ab_tests(self, limit: int = 100) -> List[ModelABTest]:
        """Get completed AB tests."""
        try:
            results = await self.get_all(
                filters={"status": "completed"},
                order_by="test_end_date",
                limit=limit
            )
            return [ModelABTest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting completed AB tests: {e}")
            return []

    async def update_model_ab_test(self, test_id: UUID, test_data: ModelABTestUpdate) -> Optional[ModelABTest]:
        """Update model AB test data."""
        try:
            data = test_data.model_dump(exclude_unset=True)
            
            result = await self.update(test_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ModelABTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating model AB test {test_id}: {e}")
            return None

    async def delete_model_ab_test(self, test_id: UUID) -> bool:
        """Delete model AB test and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(test_id)
            
        except Exception as e:
            logger.error(f"Error deleting model AB test {test_id}: {e}")
            return False

