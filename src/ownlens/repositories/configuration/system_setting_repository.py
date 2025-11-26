"""
OwnLens - Configuration Domain: System Setting Repository

Repository for configuration_system_settings table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.configuration.system_setting import SystemSetting, SystemSettingCreate, SystemSettingUpdate

logger = logging.getLogger(__name__)


class SystemSettingRepository(BaseRepository[SystemSetting]):
    """Repository for configuration_system_settings table."""

    def __init__(self, connection_manager):
        """Initialize the system setting repository."""
        super().__init__(connection_manager, table_name="configuration_system_settings")
        self.system_settings_table = "configuration_system_settings"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "setting_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "configuration_system_settings"

    # ============================================================================
    # SYSTEM SETTING OPERATIONS
    # ============================================================================

    async def create_system_setting(self, setting_data: Union[SystemSettingCreate, Dict[str, Any]]) -> Optional[SystemSetting]:
        """
        Create a new system setting record.
        
        Args:
            setting_data: System setting creation data (Pydantic model or dict)
            
        Returns:
            Created system setting with generated ID, or None if creation failed
        """
        try:
            # Convert to dict if needed
            if isinstance(setting_data, dict):
                data = setting_data
            else:
                data = setting_data.model_dump(exclude_unset=True)
            
            # Ensure default_value is always a string (database might convert it)
            if 'default_value' in data and data['default_value'] is not None:
                if not isinstance(data['default_value'], str):
                    # Convert to string based on type
                    if isinstance(data['default_value'], bool):
                        data['default_value'] = str(data['default_value']).lower()
                    elif isinstance(data['default_value'], (int, float)):
                        data['default_value'] = str(data['default_value'])
                    elif isinstance(data['default_value'], dict):
                        import json
                        data['default_value'] = json.dumps(data['default_value'])
                    else:
                        data['default_value'] = str(data['default_value'])
            
            result = await self.create(data, table_name=self.system_settings_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure default_value is a string when converting back from database
                if 'default_value' in result and result['default_value'] is not None:
                    if not isinstance(result['default_value'], str):
                        if isinstance(result['default_value'], bool):
                            result['default_value'] = str(result['default_value']).lower()
                        elif isinstance(result['default_value'], (int, float)):
                            result['default_value'] = str(result['default_value'])
                        elif isinstance(result['default_value'], dict):
                            import json
                            result['default_value'] = json.dumps(result['default_value'])
                        else:
                            result['default_value'] = str(result['default_value'])
                return SystemSetting(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating system setting: {e}")
            return None

    async def get_system_setting_by_id(self, setting_id: UUID) -> Optional[SystemSetting]:
        """Get system setting by ID."""
        try:
            result = await self.get_by_id(setting_id)
            if result:
                result = self._convert_json_to_lists(result)
                return SystemSetting(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting system setting by ID {setting_id}: {e}")
            return None

    async def get_system_setting_by_key(self, setting_key: str) -> Optional[SystemSetting]:
        """Get system setting by key."""
        try:
            result = await self.find_by_field("setting_key", setting_key)
            if result:
                result = self._convert_json_to_lists(result)
                return SystemSetting(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting system setting by key {setting_key}: {e}")
            return None

    async def get_system_settings_by_category(self, category: str) -> List[SystemSetting]:
        """Get all system settings for a category."""
        try:
            results = await self.get_all(
                filters={"category": category},
                order_by="setting_key"
            )
            return [SystemSetting(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting system settings by category {category}: {e}")
            return []

    async def get_all_system_settings(self) -> List[SystemSetting]:
        """Get all system settings."""
        try:
            results = await self.get_all(order_by="setting_key")
            return [SystemSetting(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting all system settings: {e}")
            return []

    async def update_system_setting(self, setting_id: UUID, setting_data: SystemSettingUpdate) -> Optional[SystemSetting]:
        """Update system setting data."""
        try:
            data = setting_data.model_dump(exclude_unset=True)
            
            result = await self.update(setting_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return SystemSetting(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating system setting {setting_id}: {e}")
            return None

    async def delete_system_setting(self, setting_id: UUID) -> bool:
        """Delete system setting and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(setting_id)
            
        except Exception as e:
            logger.error(f"Error deleting system setting {setting_id}: {e}")
            return False

