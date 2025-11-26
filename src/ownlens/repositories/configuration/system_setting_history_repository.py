"""
OwnLens - Configuration Domain: System Setting History Repository

Repository for configuration_system_settings_history table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging
import json

from ..shared import BaseRepository
from ...models.configuration.system_setting_history import SystemSettingHistory, SystemSettingHistoryCreate, SystemSettingHistoryUpdate

logger = logging.getLogger(__name__)


class SystemSettingHistoryRepository(BaseRepository[SystemSettingHistory]):
    """Repository for configuration_system_settings_history table."""

    def __init__(self, connection_manager):
        """Initialize the system setting history repository."""
        super().__init__(connection_manager, table_name="configuration_system_settings_history")
        self.system_setting_history_table = "configuration_system_settings_history"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "history_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "configuration_system_settings_history"

    # ============================================================================
    # SYSTEM SETTING HISTORY OPERATIONS
    # ============================================================================

    async def create_system_setting_history(self, history_data: Union[SystemSettingHistoryCreate, Dict[str, Any]]) -> Optional[SystemSettingHistory]:
        """
        Create a new system setting history record.
        
        Args:
            history_data: System setting history creation data (Pydantic model or dict)
            
        Returns:
            Created system setting history with generated ID, or None if creation failed
        """
        try:
            # BaseRepository.create() handles dict/Pydantic model conversion automatically
            # But we need to ensure old_value and new_value are strings before passing to create()
            # Convert to dict first to check/convert values (BaseRepository would do this anyway)
            data = self._convert_model_to_dict(history_data)
            
            # Ensure old_value and new_value are always strings (they might be converted to native types)
            def ensure_string(value):
                """Convert value to string if not already a string"""
                if value is None:
                    return None
                if isinstance(value, str):
                    return value
                if isinstance(value, bool):
                    return str(value).lower()
                if isinstance(value, (int, float)):
                    return str(value)
                if isinstance(value, dict):
                    return json.dumps(value)
                return str(value)
            
            if 'old_value' in data:
                data['old_value'] = ensure_string(data['old_value'])
            if 'new_value' in data:
                data['new_value'] = ensure_string(data['new_value'])
            
            result = await self.create(data, table_name=self.system_setting_history_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure old_value and new_value are strings when converting back from database
                def ensure_string(value):
                    """Convert value to string if not already a string"""
                    if value is None:
                        return None
                    if isinstance(value, str):
                        return value
                    if isinstance(value, bool):
                        return str(value).lower()
                    if isinstance(value, (int, float)):
                        return str(value)
                    if isinstance(value, dict):
                        return json.dumps(value)
                    return str(value)
                
                if 'old_value' in result:
                    result['old_value'] = ensure_string(result['old_value'])
                if 'new_value' in result:
                    result['new_value'] = ensure_string(result['new_value'])
                
                return SystemSettingHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating system setting history: {e}")
            return None

    async def get_system_setting_history_by_id(self, history_id: UUID) -> Optional[SystemSettingHistory]:
        """Get system setting history by ID."""
        try:
            result = await self.get_by_id(history_id)
            if result:
                result = self._convert_json_to_lists(result)
                return SystemSettingHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting system setting history by ID {history_id}: {e}")
            return None

    async def get_history_by_setting(self, setting_id: UUID, limit: int = 100, offset: int = 0) -> List[SystemSettingHistory]:
        """Get all history records for a system setting."""
        try:
            results = await self.get_all(
                filters={"setting_id": setting_id},
                order_by="changed_at",
                limit=limit,
                offset=offset
            )
            return [SystemSettingHistory(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting history by setting {setting_id}: {e}")
            return []

    async def get_history_by_date_range(self, start_date: date, end_date: date, setting_id: Optional[UUID] = None) -> List[SystemSettingHistory]:
        """Get history records within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.system_setting_history_table)
                .where_gte("changed_at", start_date)
                .where_lte("changed_at", end_date)
            )
            
            if setting_id:
                query_builder = query_builder.where_equals("setting_id", setting_id)
            
            query, params = query_builder.order_by("changed_at").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [SystemSettingHistory(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting history by date range: {e}")
            return []

    async def update_system_setting_history(self, history_id: UUID, history_data: SystemSettingHistoryUpdate) -> Optional[SystemSettingHistory]:
        """Update system setting history data."""
        try:
            data = history_data.model_dump(exclude_unset=True)
            
            result = await self.update(history_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return SystemSettingHistory(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating system setting history {history_id}: {e}")
            return None

    async def delete_system_setting_history(self, history_id: UUID) -> bool:
        """Delete system setting history and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(history_id)
            
        except Exception as e:
            logger.error(f"Error deleting system setting history {history_id}: {e}")
            return False

