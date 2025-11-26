"""
OwnLens - Customer Domain: User Features Repository

Repository for customer_user_features table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.customer.user_features import UserFeatures, UserFeaturesCreate, UserFeaturesUpdate

logger = logging.getLogger(__name__)


class UserFeaturesRepository(BaseRepository[UserFeatures]):
    """Repository for customer_user_features table."""

    def __init__(self, connection_manager):
        """Initialize the user features repository."""
        super().__init__(connection_manager, table_name="customer_user_features")
        self.user_features_table = "customer_user_features"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "user_feature_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_user_features"

    # ============================================================================
    # USER FEATURES OPERATIONS
    # ============================================================================

    async def create_user_features(self, features_data: UserFeaturesCreate) -> Optional[UserFeatures]:
        """
        Create a new user features record.
        
        Args:
            features_data: User features creation data (dict or Pydantic model)
            
        Returns:
            Created user features with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(features_data, table_name=self.user_features_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserFeatures(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user features: {e}")
            return None

    async def get_user_features_by_id(self, user_feature_id: UUID) -> Optional[UserFeatures]:
        """Get user features by ID."""
        try:
            result = await self.get_by_id(user_feature_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserFeatures(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user features by ID {user_feature_id}: {e}")
            return None

    async def get_user_features_by_user(self, user_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[UserFeatures]:
        """Get all features for a user."""
        try:
            filters = {"user_id": user_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="feature_date",
                limit=limit,
                offset=offset
            )
            return [UserFeatures(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting user features by user {user_id}: {e}")
            return []

    async def get_latest_user_features(self, user_id: UUID, brand_id: Optional[UUID] = None) -> Optional[UserFeatures]:
        """Get the latest features for a user."""
        try:
            filters = {"user_id": user_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="feature_date",
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return UserFeatures(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest user features for user {user_id}: {e}")
            return None

    async def get_user_features_by_date(self, feature_date: date, brand_id: Optional[UUID] = None) -> List[UserFeatures]:
        """Get all user features for a specific date."""
        try:
            filters = {"feature_date": feature_date}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="user_id")
            return [UserFeatures(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting user features by date {feature_date}: {e}")
            return []

    async def update_user_features(self, user_feature_id: UUID, features_data: UserFeaturesUpdate) -> Optional[UserFeatures]:
        """Update user features data."""
        try:
            data = features_data.model_dump(exclude_unset=True)
            
            result = await self.update(user_feature_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserFeatures(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user features {user_feature_id}: {e}")
            return None

    async def delete_user_features(self, user_feature_id: UUID) -> bool:
        """Delete user features and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(user_feature_id)
            
        except Exception as e:
            logger.error(f"Error deleting user features {user_feature_id}: {e}")
            return False

