"""
OwnLens - Editorial Domain: Headline Test Repository

Repository for editorial_headline_tests table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.headline_test import HeadlineTest, HeadlineTestCreate, HeadlineTestUpdate

logger = logging.getLogger(__name__)


class HeadlineTestRepository(BaseRepository[HeadlineTest]):
    """Repository for editorial_headline_tests table."""

    def __init__(self, connection_manager):
        """Initialize the headline test repository."""
        super().__init__(connection_manager, table_name="editorial_headline_tests")
        self.headline_tests_table = "editorial_headline_tests"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "test_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_headline_tests"

    # ============================================================================
    # HEADLINE TEST OPERATIONS
    # ============================================================================

    async def create_headline_test(self, test_data: HeadlineTestCreate) -> Optional[HeadlineTest]:
        """
        Create a new headline test record.
        
        Args:
            test_data: Headline test creation data (dict or Pydantic model)
            
        Returns:
            Created headline test with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(test_data, table_name=self.headline_tests_table)
            if result:
                result = self._convert_json_to_lists(result)
                return HeadlineTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating headline test: {e}")
            return None

    async def get_headline_test_by_id(self, test_id: UUID) -> Optional[HeadlineTest]:
        """Get headline test by ID."""
        try:
            result = await self.get_by_id(test_id)
            if result:
                result = self._convert_json_to_lists(result)
                return HeadlineTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting headline test by ID {test_id}: {e}")
            return None

    async def get_headline_tests_by_article(self, article_id: UUID) -> List[HeadlineTest]:
        """Get all headline tests for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="test_start_date"
            )
            return [HeadlineTest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting headline tests by article {article_id}: {e}")
            return []

    async def get_active_headline_tests(self, brand_id: Optional[UUID] = None) -> List[HeadlineTest]:
        """Get all active headline tests."""
        try:
            filters = {"status": "active"}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="test_start_date")
            return [HeadlineTest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active headline tests: {e}")
            return []

    async def update_headline_test(self, test_id: UUID, test_data: HeadlineTestUpdate) -> Optional[HeadlineTest]:
        """Update headline test data."""
        try:
            data = test_data.model_dump(exclude_unset=True)
            
            result = await self.update(test_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return HeadlineTest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating headline test {test_id}: {e}")
            return None

    async def delete_headline_test(self, test_id: UUID) -> bool:
        """Delete headline test and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(test_id)
            
        except Exception as e:
            logger.error(f"Error deleting headline test {test_id}: {e}")
            return False

