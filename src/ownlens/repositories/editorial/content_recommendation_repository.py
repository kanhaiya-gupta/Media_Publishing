"""
OwnLens - Editorial Domain: Content Recommendation Repository

Repository for editorial_content_recommendations table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.editorial.content_recommendation import ContentRecommendation, ContentRecommendationCreate, ContentRecommendationUpdate

logger = logging.getLogger(__name__)


class ContentRecommendationRepository(BaseRepository[ContentRecommendation]):
    """Repository for editorial_content_recommendations table."""

    def __init__(self, connection_manager):
        """Initialize the content recommendation repository."""
        super().__init__(connection_manager, table_name="editorial_content_recommendations")
        self.content_recommendations_table = "editorial_content_recommendations"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "recommendation_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_content_recommendations"

    # ============================================================================
    # CONTENT RECOMMENDATION OPERATIONS
    # ============================================================================

    async def create_content_recommendation(self, recommendation_data: Union[ContentRecommendationCreate, Dict[str, Any]]) -> Optional[ContentRecommendation]:
        """
        Create a new content recommendation record.
        
        Args:
            recommendation_data: Content recommendation creation data (dict or ContentRecommendationCreate model)
            
        Returns:
            Created content recommendation with generated ID, or None if creation failed
        """
        try:
            if isinstance(recommendation_data, dict):
                recommendation_data = ContentRecommendationCreate.model_validate(recommendation_data)
            data = recommendation_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.content_recommendations_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentRecommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating content recommendation: {e}")
            return None

    async def get_content_recommendation_by_id(self, recommendation_id: UUID) -> Optional[ContentRecommendation]:
        """Get content recommendation by ID."""
        try:
            result = await self.get_by_id(recommendation_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentRecommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content recommendation by ID {recommendation_id}: {e}")
            return None

    async def get_content_recommendations_by_article(self, article_id: UUID, brand_id: Optional[UUID] = None) -> List[ContentRecommendation]:
        """Get all recommendations for an article."""
        try:
            filters = {"article_id": article_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="recommendation_score")
            return [ContentRecommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content recommendations by article {article_id}: {e}")
            return []

    async def get_content_recommendations_by_status(self, status: str, brand_id: Optional[UUID] = None, limit: int = 100) -> List[ContentRecommendation]:
        """Get recommendations by status."""
        try:
            filters = {"status": status}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="created_at",
                limit=limit
            )
            return [ContentRecommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content recommendations by status {status}: {e}")
            return []

    async def get_implemented_recommendations(self, brand_id: Optional[UUID] = None, limit: int = 100) -> List[ContentRecommendation]:
        """Get implemented recommendations."""
        try:
            filters = {"status": "implemented"}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="implemented_at",
                limit=limit
            )
            return [ContentRecommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting implemented recommendations: {e}")
            return []

    async def update_content_recommendation(self, recommendation_id: UUID, recommendation_data: ContentRecommendationUpdate) -> Optional[ContentRecommendation]:
        """Update content recommendation data."""
        try:
            data = recommendation_data.model_dump(exclude_unset=True)
            
            result = await self.update(recommendation_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentRecommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating content recommendation {recommendation_id}: {e}")
            return None

    async def delete_content_recommendation(self, recommendation_id: UUID) -> bool:
        """Delete content recommendation and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(recommendation_id)
            
        except Exception as e:
            logger.error(f"Error deleting content recommendation {recommendation_id}: {e}")
            return False

