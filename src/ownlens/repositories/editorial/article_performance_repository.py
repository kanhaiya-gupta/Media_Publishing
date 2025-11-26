"""
OwnLens - Editorial Domain: Article Performance Repository

Repository for editorial_article_performance table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.editorial.article_performance import ArticlePerformance, ArticlePerformanceCreate, ArticlePerformanceUpdate

logger = logging.getLogger(__name__)


class ArticlePerformanceRepository(BaseRepository[ArticlePerformance]):
    """Repository for editorial_article_performance table."""

    def __init__(self, connection_manager):
        """Initialize the article performance repository."""
        super().__init__(connection_manager, table_name="editorial_article_performance")
        self.article_performance_table = "editorial_article_performance"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "performance_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_article_performance"

    # ============================================================================
    # ARTICLE PERFORMANCE OPERATIONS
    # ============================================================================

    async def create_article_performance(self, performance_data: ArticlePerformanceCreate) -> Optional[ArticlePerformance]:
        """
        Create a new article performance record.
        
        Args:
            performance_data: Article performance creation data (dict or Pydantic model)
            
        Returns:
            Created article performance with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(performance_data, table_name=self.article_performance_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticlePerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating article performance: {e}")
            return None

    async def get_article_performance_by_id(self, performance_id: UUID) -> Optional[ArticlePerformance]:
        """Get article performance by ID."""
        try:
            result = await self.get_by_id(performance_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticlePerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article performance by ID {performance_id}: {e}")
            return None

    async def get_article_performance_by_article(self, article_id: UUID, brand_id: Optional[UUID] = None) -> Optional[ArticlePerformance]:
        """Get performance for an article."""
        try:
            filters = {"article_id": article_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="performance_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return ArticlePerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article performance by article {article_id}: {e}")
            return None

    async def get_top_performing_articles(self, brand_id: UUID, limit: int = 100, order_by: str = "total_views") -> List[ArticlePerformance]:
        """Get top performing articles."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by=order_by,
                limit=limit
            )
            return [ArticlePerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top performing articles for brand {brand_id}: {e}")
            return []

    async def get_article_performance_by_date(self, performance_date: date, brand_id: Optional[UUID] = None) -> List[ArticlePerformance]:
        """Get article performance for a specific date."""
        try:
            filters = {"performance_date": performance_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="total_views")
            return [ArticlePerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting article performance by date {performance_date}: {e}")
            return []

    async def update_article_performance(self, performance_id: UUID, performance_data: ArticlePerformanceUpdate) -> Optional[ArticlePerformance]:
        """Update article performance data."""
        try:
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.update(performance_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticlePerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating article performance {performance_id}: {e}")
            return None

    async def delete_article_performance(self, performance_id: UUID) -> bool:
        """Delete article performance and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(performance_id)
            
        except Exception as e:
            logger.error(f"Error deleting article performance {performance_id}: {e}")
            return False

