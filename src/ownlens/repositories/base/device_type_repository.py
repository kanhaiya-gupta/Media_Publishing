"""
OwnLens - Base Domain: Device Type Repository

Repository for device_types table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.device_type import DeviceType, DeviceTypeCreate, DeviceTypeUpdate

logger = logging.getLogger(__name__)


class DeviceTypeRepository(BaseRepository[DeviceType]):
    """Repository for device_types table."""

    def __init__(self, connection_manager):
        """Initialize the device type repository."""
        super().__init__(connection_manager, table_name="device_types")
        self.device_types_table = "device_types"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "device_type_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "device_types"

    # ============================================================================
    # DEVICE TYPE OPERATIONS
    # ============================================================================

    async def create_device_type(self, device_type_data: DeviceTypeCreate) -> Optional[DeviceType]:
        """
        Create a new device type record.
        
        Args:
            device_type_data: Device type creation data (dict or DeviceTypeCreate model)
            
        Returns:
            Created device type with generated ID, or None if creation failed
        """
        try:
            # Convert dict to DeviceTypeCreate model if needed (this runs validators)
            if isinstance(device_type_data, dict):
                device_type_data = DeviceTypeCreate.model_validate(device_type_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(device_type_data, table_name=self.device_types_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DeviceType(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating device type: {e}")
            return None

    async def get_device_type_by_id(self, device_type_id: UUID) -> Optional[DeviceType]:
        """Get device type by ID."""
        try:
            result = await self.get_by_id(device_type_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DeviceType(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting device type by ID {device_type_id}: {e}")
            return None

    async def get_device_type_by_code(self, device_type_code: str) -> Optional[DeviceType]:
        """Get device type by code."""
        try:
            result = await self.find_by_field("device_type_code", device_type_code)
            if result:
                result = self._convert_json_to_lists(result)
                return DeviceType(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting device type by code {device_type_code}: {e}")
            return None

    async def get_active_device_types(self) -> List[DeviceType]:
        """Get all active device types."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="device_type_name")
            return [DeviceType(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active device types: {e}")
            return []

    async def update_device_type(self, device_type_id: UUID, device_type_data: DeviceTypeUpdate) -> Optional[DeviceType]:
        """Update device type data."""
        try:
            data = device_type_data.model_dump(exclude_unset=True)
            
            result = await self.update(device_type_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DeviceType(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating device type {device_type_id}: {e}")
            return None

    async def delete_device_type(self, device_type_id: UUID) -> bool:
        """Delete device type and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(device_type_id)
            
        except Exception as e:
            logger.error(f"Error deleting device type {device_type_id}: {e}")
            return False

