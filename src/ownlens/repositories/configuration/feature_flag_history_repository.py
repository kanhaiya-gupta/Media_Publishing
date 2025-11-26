"""
OwnLens - Configuration Domain: Feature Flag History Repository

Repository for configuration_feature_flag_history table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.configuration.feature_flag_history import FeatureFlagHistory, FeatureFlagHistoryCreate, FeatureFlagHistoryUpdate

logger = logging.getLogger(__name__)


class FeatureFlagHistoryRepository(BaseRepository[FeatureFlagHistory]):
    """Repository for configuration_feature_flag_history table."""

    def __init__(self, connection_manager):
        """Initialize the feature flag history repository."""
        super().__init__(connection_manager, table_name="configuration_feature_flag_history")
        self.feature_flag_history_table = "configuration_feature_flag_history"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "history_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "configuration_feature_flag_history"

    # ============================================================================
    # FEATURE FLAG HISTORY OPERATIONS
    # ============================================================================

    async def create_feature_flag_history(self, history_data: FeatureFlagHistoryCreate) -> Optional[FeatureFlagHistory]:
        """
        Create a new feature flag history record.
        
        Args:
            history_data: Feature flag history creation data
            
        Returns:
            Created feature flag history with generated ID, or None if creation failed
        """
        try:
            data = history_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.feature_flag_history_table)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlagHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating feature flag history: {e}")
            return None

    async def get_feature_flag_history_by_id(self, history_id: UUID) -> Optional[FeatureFlagHistory]:
        """Get feature flag history by ID."""
        try:
            result = await self.get_by_id(history_id)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlagHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting feature flag history by ID {history_id}: {e}")
            return None

    async def get_history_by_flag(self, flag_id: UUID, limit: int = 100, offset: int = 0) -> List[FeatureFlagHistory]:
        """Get all history records for a feature flag."""
        try:
            results = await self.get_all(
                filters={"flag_id": flag_id},
                order_by="changed_at",
                limit=limit,
                offset=offset
            )
            return [FeatureFlagHistory(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting history by flag {flag_id}: {e}")
            return []

    async def get_history_by_date_range(self, start_date: date, end_date: date, flag_id: Optional[UUID] = None) -> List[FeatureFlagHistory]:
        """Get history records within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.feature_flag_history_table)
                .where_gte("changed_at", start_date)
                .where_lte("changed_at", end_date)
            )
            
            if flag_id:
                query_builder = query_builder.where_equals("flag_id", flag_id)
            
            query, params = query_builder.order_by("changed_at").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [FeatureFlagHistory(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting history by date range: {e}")
            return []

    async def update_feature_flag_history(self, history_id: UUID, history_data: FeatureFlagHistoryUpdate) -> Optional[FeatureFlagHistory]:
        """Update feature flag history data."""
        try:
            data = history_data.model_dump(exclude_unset=True)
            
            result = await self.update(history_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return FeatureFlagHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating feature flag history {history_id}: {e}")
            return None

    async def delete_feature_flag_history(self, history_id: UUID) -> bool:
        """Delete feature flag history and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(history_id)
            
        except Exception as e:
            logger.error(f"Error deleting feature flag history {history_id}: {e}")
            return False

