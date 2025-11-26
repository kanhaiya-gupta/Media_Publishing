"""
OwnLens - Editorial Domain: Category Performance Repository

Repository for editorial_category_performance table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.editorial.category_performance import CategoryPerformance, CategoryPerformanceCreate, CategoryPerformanceUpdate

logger = logging.getLogger(__name__)


class CategoryPerformanceRepository(BaseRepository[CategoryPerformance]):
    """Repository for editorial_category_performance table."""

    def __init__(self, connection_manager):
        """Initialize the category performance repository."""
        super().__init__(connection_manager, table_name="editorial_category_performance")
        self.category_performance_table = "editorial_category_performance"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "performance_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_category_performance"

    # ============================================================================
    # CATEGORY PERFORMANCE OPERATIONS
    # ============================================================================

    async def create_category_performance(self, performance_data: CategoryPerformanceCreate) -> Optional[CategoryPerformance]:
        """
        Create a new category performance record.
        
        Args:
            performance_data: Category performance creation data (dict or Pydantic model)
            
        Returns:
            Created category performance with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(performance_data, table_name=self.category_performance_table)
            if result:
                result = self._convert_json_to_lists(result)
                return CategoryPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating category performance: {e}")
            return None

    async def get_category_performance_by_id(self, performance_id: UUID) -> Optional[CategoryPerformance]:
        """Get category performance by ID."""
        try:
            result = await self.get_by_id(performance_id)
            if result:
                result = self._convert_json_to_lists(result)
                return CategoryPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting category performance by ID {performance_id}: {e}")
            return None

    async def get_category_performance_by_category(self, category_id: UUID, brand_id: Optional[UUID] = None) -> Optional[CategoryPerformance]:
        """Get performance for a category."""
        try:
            filters = {"category_id": category_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="performance_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return CategoryPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting category performance by category {category_id}: {e}")
            return None

    async def get_top_performing_categories(self, brand_id: UUID, limit: int = 100, order_by: str = "total_views") -> List[CategoryPerformance]:
        """Get top performing categories."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by=order_by,
                limit=limit
            )
            return [CategoryPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top performing categories for brand {brand_id}: {e}")
            return []

    async def get_category_performance_by_date(self, performance_date: date, brand_id: Optional[UUID] = None) -> List[CategoryPerformance]:
        """Get category performance for a specific date."""
        try:
            filters = {"performance_date": performance_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="total_views")
            return [CategoryPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting category performance by date {performance_date}: {e}")
            return []

    async def update_category_performance(self, performance_id: UUID, performance_data: CategoryPerformanceUpdate) -> Optional[CategoryPerformance]:
        """Update category performance data."""
        try:
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.update(performance_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return CategoryPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating category performance {performance_id}: {e}")
            return None

    async def delete_category_performance(self, performance_id: UUID) -> bool:
        """Delete category performance and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(performance_id)
            
        except Exception as e:
            logger.error(f"Error deleting category performance {performance_id}: {e}")
            return False

