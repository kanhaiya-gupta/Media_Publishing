"""
OwnLens - Customer Domain: Recommendation Repository

Repository for customer_recommendations table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.customer.recommendation import Recommendation, RecommendationCreate, RecommendationUpdate

logger = logging.getLogger(__name__)


class RecommendationRepository(BaseRepository[Recommendation]):
    """Repository for customer_recommendations table."""

    def __init__(self, connection_manager):
        """Initialize the recommendation repository."""
        super().__init__(connection_manager, table_name="customer_recommendations")
        self.recommendations_table = "customer_recommendations"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "recommendation_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_recommendations"

    # ============================================================================
    # RECOMMENDATION OPERATIONS
    # ============================================================================

    async def create_recommendation(self, recommendation_data: Union[RecommendationCreate, Dict[str, Any]]) -> Optional[Recommendation]:
        """
        Create a new recommendation record.
        
        Args:
            recommendation_data: Recommendation creation data (Pydantic model or dict)
            
        Returns:
            Created recommendation with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(recommendation_data, table_name=self.recommendations_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Recommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating recommendation: {e}")
            return None

    async def get_recommendation_by_id(self, recommendation_id: UUID) -> Optional[Recommendation]:
        """Get recommendation by ID."""
        try:
            result = await self.get_by_id(recommendation_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Recommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting recommendation by ID {recommendation_id}: {e}")
            return None

    async def get_recommendations_by_user(self, user_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[Recommendation]:
        """Get all recommendations for a user."""
        try:
            filters = {"user_id": user_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="rank_position",
                limit=limit,
                offset=offset
            )
            return [Recommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting recommendations by user {user_id}: {e}")
            return []

    async def get_active_recommendations(self, user_id: UUID, brand_id: Optional[UUID] = None) -> List[Recommendation]:
        """Get active (non-expired) recommendations for a user."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.recommendations_table)
                .where_equals("user_id", user_id)
                .where_or([
                    ("expires_at", "IS", None),
                    ("expires_at", ">", datetime.utcnow())
                ])
                .order_by("rank_position")
                .build()
            )
            
            if brand_id:
                query = query.replace("WHERE user_id =", f"WHERE brand_id = $1 AND user_id =")
                params = [brand_id] + list(params) if params else [brand_id]
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [Recommendation(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting active recommendations for user {user_id}: {e}")
            return []

    async def get_recommendations_by_article(self, article_id: str, brand_id: Optional[UUID] = None) -> List[Recommendation]:
        """Get all recommendations for an article."""
        try:
            filters = {"article_id": article_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="recommendation_score")
            return [Recommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting recommendations by article {article_id}: {e}")
            return []

    async def get_clicked_recommendations(self, user_id: Optional[UUID] = None, brand_id: Optional[UUID] = None, limit: int = 100) -> List[Recommendation]:
        """Get recommendations that were clicked."""
        try:
            filters = {"was_clicked": True}
            if user_id:
                filters["user_id"] = user_id
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="clicked_at",
                limit=limit
            )
            return [Recommendation(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting clicked recommendations: {e}")
            return []

    async def update_recommendation(self, recommendation_id: UUID, recommendation_data: Union[RecommendationUpdate, Dict[str, Any]]) -> Optional[Recommendation]:
        """Update recommendation data."""
        try:
            # Base update() method already handles dict/Pydantic model conversion
            result = await self.update(recommendation_id, recommendation_data)
            if result:
                result = self._convert_json_to_lists(result)
                return Recommendation(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating recommendation {recommendation_id}: {e}")
            return None

    async def delete_recommendation(self, recommendation_id: UUID) -> bool:
        """Delete recommendation and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(recommendation_id)
            
        except Exception as e:
            logger.error(f"Error deleting recommendation {recommendation_id}: {e}")
            return False

