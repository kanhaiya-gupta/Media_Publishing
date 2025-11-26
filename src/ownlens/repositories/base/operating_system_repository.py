"""
OwnLens - Base Domain: Operating System Repository

Repository for operating_systems table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.operating_system import OperatingSystem, OperatingSystemCreate, OperatingSystemUpdate

logger = logging.getLogger(__name__)


class OperatingSystemRepository(BaseRepository[OperatingSystem]):
    """Repository for operating_systems table."""

    def __init__(self, connection_manager):
        """Initialize the operating system repository."""
        super().__init__(connection_manager, table_name="operating_systems")
        self.operating_systems_table = "operating_systems"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "os_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "operating_systems"

    # ============================================================================
    # OPERATING SYSTEM OPERATIONS
    # ============================================================================

    async def create_operating_system(self, os_data: OperatingSystemCreate) -> Optional[OperatingSystem]:
        """
        Create a new operating system record.
        
        Args:
            os_data: Operating system creation data (dict or OperatingSystemCreate model)
            
        Returns:
            Created operating system with generated ID, or None if creation failed
        """
        try:
            # Convert dict to OperatingSystemCreate model if needed (this runs validators)
            if isinstance(os_data, dict):
                os_data = OperatingSystemCreate.model_validate(os_data)
            
            # Convert to dict first to handle enum conversion if needed
            data = self._convert_model_to_dict(os_data)
            
            # Ensure enum is string
            if 'os_family' in data and data['os_family'] is not None:
                if hasattr(data['os_family'], 'value'):
                    data['os_family'] = data['os_family'].value
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(data, table_name=self.operating_systems_table)
            if result:
                result = self._convert_json_to_lists(result)
                return OperatingSystem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating operating system: {e}")
            return None

    async def get_operating_system_by_id(self, os_id: UUID) -> Optional[OperatingSystem]:
        """Get operating system by ID."""
        try:
            result = await self.get_by_id(os_id)
            if result:
                result = self._convert_json_to_lists(result)
                return OperatingSystem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting operating system by ID {os_id}: {e}")
            return None

    async def get_operating_system_by_code(self, os_code: str) -> Optional[OperatingSystem]:
        """Get operating system by code."""
        try:
            result = await self.find_by_field("os_code", os_code)
            if result:
                result = self._convert_json_to_lists(result)
                return OperatingSystem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting operating system by code {os_code}: {e}")
            return None

    async def get_active_operating_systems(self) -> List[OperatingSystem]:
        """Get all active operating systems."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="os_name")
            return [OperatingSystem(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active operating systems: {e}")
            return []

    async def get_operating_systems_by_family(self, os_family: str) -> List[OperatingSystem]:
        """Get operating systems by family."""
        try:
            results = await self.find_all_by_field("os_family", os_family)
            return [OperatingSystem(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting operating systems by family {os_family}: {e}")
            return []

    async def update_operating_system(self, os_id: UUID, os_data: OperatingSystemUpdate) -> Optional[OperatingSystem]:
        """Update operating system data."""
        try:
            data = os_data.model_dump(exclude_unset=True)
            
            # Ensure enum is string
            if 'os_family' in data and data['os_family'] is not None:
                if hasattr(data['os_family'], 'value'):
                    data['os_family'] = data['os_family'].value
            
            result = await self.update(os_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return OperatingSystem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating operating system {os_id}: {e}")
            return None

    async def delete_operating_system(self, os_id: UUID) -> bool:
        """Delete operating system and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(os_id)
            
        except Exception as e:
            logger.error(f"Error deleting operating system {os_id}: {e}")
            return False

