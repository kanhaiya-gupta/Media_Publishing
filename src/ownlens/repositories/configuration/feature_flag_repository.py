"""
OwnLens - Configuration Domain: Feature Flag Repository

Repository for configuration_feature_flags table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.configuration.feature_flag import FeatureFlag, FeatureFlagCreate, FeatureFlagUpdate

logger = logging.getLogger(__name__)


class FeatureFlagRepository(BaseRepository[FeatureFlag]):
    """Repository for configuration_feature_flags table."""

    def __init__(self, connection_manager):
        """Initialize the feature flag repository."""
        super().__init__(connection_manager, table_name="configuration_feature_flags")
        self.feature_flags_table = "configuration_feature_flags"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "flag_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "configuration_feature_flags"

    # ============================================================================
    # FEATURE FLAG OPERATIONS
    # ============================================================================

    async def create_feature_flag(self, flag_data: Union[FeatureFlagCreate, Dict[str, Any]]) -> Optional[FeatureFlag]:
        """
        Create a new feature flag record.
        
        Args:
            flag_data: Feature flag creation data (Pydantic model or dict)
            
        Returns:
            Created feature flag with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(flag_data, table_name=self.feature_flags_table)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlag(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating feature flag: {e}")
            return None

    async def get_feature_flag_by_id(self, flag_id: UUID) -> Optional[FeatureFlag]:
        """Get feature flag by ID."""
        try:
            result = await self.get_by_id(flag_id)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlag(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting feature flag by ID {flag_id}: {e}")
            return None

    async def get_feature_flag_by_key(self, flag_key: str) -> Optional[FeatureFlag]:
        """Get feature flag by key."""
        try:
            result = await self.find_by_field("flag_key", flag_key)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlag(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting feature flag by key {flag_key}: {e}")
            return None

    async def get_active_feature_flags(self) -> List[FeatureFlag]:
        """Get all active feature flags."""
        try:
            results = await self.get_all(
                filters={"is_active": True},
                order_by="flag_key"
            )
            return [FeatureFlag(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active feature flags: {e}")
            return []

    async def get_all_feature_flags(self) -> List[FeatureFlag]:
        """Get all feature flags."""
        try:
            results = await self.get_all(order_by="flag_key")
            return [FeatureFlag(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting all feature flags: {e}")
            return []

    async def update_feature_flag(self, flag_id: UUID, flag_data: FeatureFlagUpdate) -> Optional[FeatureFlag]:
        """Update feature flag data."""
        try:
            data = flag_data.model_dump(exclude_unset=True)
            
            result = await self.update(flag_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlag(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating feature flag {flag_id}: {e}")
            return None

    async def delete_feature_flag(self, flag_id: UUID) -> bool:
        """Delete feature flag and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(flag_id)
            
        except Exception as e:
            logger.error(f"Error deleting feature flag {flag_id}: {e}")
            return False

