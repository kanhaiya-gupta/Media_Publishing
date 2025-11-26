"""
OwnLens - Company Domain: Content Performance Repository

Repository for company_content_performance table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.company.content_performance import ContentPerformance, ContentPerformanceCreate, ContentPerformanceUpdate

logger = logging.getLogger(__name__)


class ContentPerformanceRepository(BaseRepository[ContentPerformance]):
    """Repository for company_content_performance table."""

    def __init__(self, connection_manager):
        """Initialize the content performance repository."""
        super().__init__(connection_manager, table_name="company_content_performance")
        self.content_performance_table = "company_content_performance"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "performance_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_content_performance"

    # ============================================================================
    # CONTENT PERFORMANCE OPERATIONS
    # ============================================================================

    async def create_content_performance(self, performance_data: Union[ContentPerformanceCreate, Dict[str, Any]]) -> Optional[ContentPerformance]:
        """
        Create a new content performance record.
        
        Args:
            performance_data: Content performance creation data (dict or ContentPerformanceCreate model)
            
        Returns:
            Created content performance with generated ID, or None if creation failed
        """
        try:
            if isinstance(performance_data, dict):
                performance_data = ContentPerformanceCreate.model_validate(performance_data)
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.content_performance_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating content performance: {e}")
            return None

    async def get_content_performance_by_id(self, performance_id: UUID) -> Optional[ContentPerformance]:
        """Get content performance by ID."""
        try:
            result = await self.get_by_id(performance_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content performance by ID {performance_id}: {e}")
            return None

    async def get_content_performance_by_content(self, content_id: UUID, company_id: Optional[UUID] = None) -> Optional[ContentPerformance]:
        """Get performance for a content."""
        try:
            filters = {"content_id": content_id}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="performance_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return ContentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content performance by content {content_id}: {e}")
            return None

    async def get_top_performing_content(self, company_id: UUID, limit: int = 100, order_by: str = "total_views") -> List[ContentPerformance]:
        """Get top performing content."""
        try:
            results = await self.get_all(
                filters={"company_id": company_id},
                order_by=order_by,
                limit=limit
            )
            return [ContentPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top performing content for company {company_id}: {e}")
            return []

    async def get_content_performance_by_date(self, performance_date: date, company_id: Optional[UUID] = None) -> List[ContentPerformance]:
        """Get content performance for a specific date."""
        try:
            filters = {"performance_date": performance_date}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="total_views")
            return [ContentPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content performance by date {performance_date}: {e}")
            return []

    async def update_content_performance(self, performance_id: UUID, performance_data: ContentPerformanceUpdate) -> Optional[ContentPerformance]:
        """Update content performance data."""
        try:
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.update(performance_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating content performance {performance_id}: {e}")
            return None

    async def delete_content_performance(self, performance_id: UUID) -> bool:
        """Delete content performance and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(performance_id)
            
        except Exception as e:
            logger.error(f"Error deleting content performance {performance_id}: {e}")
            return False

