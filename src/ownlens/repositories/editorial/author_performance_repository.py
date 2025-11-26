"""
OwnLens - Editorial Domain: Author Performance Repository

Repository for editorial_author_performance table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.editorial.author_performance import AuthorPerformance, AuthorPerformanceCreate, AuthorPerformanceUpdate

logger = logging.getLogger(__name__)


class AuthorPerformanceRepository(BaseRepository[AuthorPerformance]):
    """Repository for editorial_author_performance table."""

    def __init__(self, connection_manager):
        """Initialize the author performance repository."""
        super().__init__(connection_manager, table_name="editorial_author_performance")
        self.author_performance_table = "editorial_author_performance"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "performance_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_author_performance"

    # ============================================================================
    # AUTHOR PERFORMANCE OPERATIONS
    # ============================================================================

    async def create_author_performance(self, performance_data: AuthorPerformanceCreate) -> Optional[AuthorPerformance]:
        """
        Create a new author performance record.
        
        Args:
            performance_data: Author performance creation data (dict or Pydantic model)
            
        Returns:
            Created author performance with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(performance_data, table_name=self.author_performance_table)
            if result:
                result = self._convert_json_to_lists(result)
                return AuthorPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating author performance: {e}")
            return None

    async def get_author_performance_by_id(self, performance_id: UUID) -> Optional[AuthorPerformance]:
        """Get author performance by ID."""
        try:
            result = await self.get_by_id(performance_id)
            if result:
                result = self._convert_json_to_lists(result)
                return AuthorPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting author performance by ID {performance_id}: {e}")
            return None

    async def get_author_performance_by_author(self, author_id: UUID, brand_id: Optional[UUID] = None) -> Optional[AuthorPerformance]:
        """Get performance for an author."""
        try:
            filters = {"author_id": author_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="performance_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return AuthorPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting author performance by author {author_id}: {e}")
            return None

    async def get_top_performing_authors(self, brand_id: UUID, limit: int = 100, order_by: str = "total_views") -> List[AuthorPerformance]:
        """Get top performing authors."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by=order_by,
                limit=limit
            )
            return [AuthorPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top performing authors for brand {brand_id}: {e}")
            return []

    async def get_author_performance_by_date(self, performance_date: date, brand_id: Optional[UUID] = None) -> List[AuthorPerformance]:
        """Get author performance for a specific date."""
        try:
            filters = {"performance_date": performance_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="total_views")
            return [AuthorPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting author performance by date {performance_date}: {e}")
            return []

    async def update_author_performance(self, performance_id: UUID, performance_data: AuthorPerformanceUpdate) -> Optional[AuthorPerformance]:
        """Update author performance data."""
        try:
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.update(performance_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return AuthorPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating author performance {performance_id}: {e}")
            return None

    async def delete_author_performance(self, performance_id: UUID) -> bool:
        """Delete author performance and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(performance_id)
            
        except Exception as e:
            logger.error(f"Error deleting author performance {performance_id}: {e}")
            return False

