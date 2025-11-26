"""
OwnLens - Editorial Domain: Trending Topic Repository

Repository for editorial_trending_topics table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.editorial.trending_topic import TrendingTopic, TrendingTopicCreate, TrendingTopicUpdate

logger = logging.getLogger(__name__)


class TrendingTopicRepository(BaseRepository[TrendingTopic]):
    """Repository for editorial_trending_topics table."""

    def __init__(self, connection_manager):
        """Initialize the trending topic repository."""
        super().__init__(connection_manager, table_name="editorial_trending_topics")
        self.trending_topics_table = "editorial_trending_topics"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "topic_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_trending_topics"

    # ============================================================================
    # TRENDING TOPIC OPERATIONS
    # ============================================================================

    async def create_trending_topic(self, topic_data: TrendingTopicCreate) -> Optional[TrendingTopic]:
        """
        Create a new trending topic record.
        
        Args:
            topic_data: Trending topic creation data (dict or Pydantic model)
            
        Returns:
            Created trending topic with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(topic_data, table_name=self.trending_topics_table)
            if result:
                result = self._convert_json_to_lists(result)
                return TrendingTopic(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating trending topic: {e}")
            return None

    async def get_trending_topic_by_id(self, topic_id: UUID) -> Optional[TrendingTopic]:
        """Get trending topic by ID."""
        try:
            result = await self.get_by_id(topic_id)
            if result:
                result = self._convert_json_to_lists(result)
                return TrendingTopic(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting trending topic by ID {topic_id}: {e}")
            return None

    async def get_trending_topics_by_brand(self, brand_id: UUID, limit: int = 100, order_by: str = "trending_score") -> List[TrendingTopic]:
        """Get trending topics for a brand."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by=order_by,
                limit=limit
            )
            return [TrendingTopic(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting trending topics by brand {brand_id}: {e}")
            return []

    async def get_trending_topics_by_date(self, topic_date: date, brand_id: Optional[UUID] = None) -> List[TrendingTopic]:
        """Get trending topics for a specific date."""
        try:
            filters = {"topic_date": topic_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="trending_score")
            return [TrendingTopic(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting trending topics by date {topic_date}: {e}")
            return []

    async def get_top_trending_topics(self, brand_id: UUID, limit: int = 50) -> List[TrendingTopic]:
        """Get top trending topics."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by="trending_score",
                limit=limit
            )
            return [TrendingTopic(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top trending topics for brand {brand_id}: {e}")
            return []

    async def update_trending_topic(self, topic_id: UUID, topic_data: TrendingTopicUpdate) -> Optional[TrendingTopic]:
        """Update trending topic data."""
        try:
            data = topic_data.model_dump(exclude_unset=True)
            
            result = await self.update(topic_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return TrendingTopic(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating trending topic {topic_id}: {e}")
            return None

    async def delete_trending_topic(self, topic_id: UUID) -> bool:
        """Delete trending topic and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(topic_id)
            
        except Exception as e:
            logger.error(f"Error deleting trending topic {topic_id}: {e}")
            return False

