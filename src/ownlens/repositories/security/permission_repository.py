"""
OwnLens - Security Domain: Permission Repository

Repository for security_permissions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.security.permission import Permission, PermissionCreate, PermissionUpdate

logger = logging.getLogger(__name__)


class PermissionRepository(BaseRepository[Permission]):
    """Repository for security_permissions table."""

    def __init__(self, connection_manager):
        """Initialize the permission repository."""
        super().__init__(connection_manager, table_name="security_permissions")
        self.permissions_table = "security_permissions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "permission_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_permissions"

    # ============================================================================
    # PERMISSION OPERATIONS
    # ============================================================================

    async def create_permission(self, permission_data: Union[PermissionCreate, Dict[str, Any]]) -> Optional[Permission]:
        """
        Create a new permission record.
        
        Args:
            permission_data: Permission creation data (dict or PermissionCreate model)
            
        Returns:
            Created permission with generated ID, or None if creation failed
        """
        try:
            # Convert dict to PermissionCreate model if needed (this runs validators)
            if isinstance(permission_data, dict):
                permission_data = PermissionCreate.model_validate(permission_data)
            else:
                # If it's already a model but ID is None, re-validate to trigger validator
                if hasattr(permission_data, 'permission_id') and permission_data.permission_id is None:
                    # Re-validate to trigger the ID generator validator
                    permission_data = PermissionCreate.model_validate(permission_data.model_dump())
            
            # Store the generated permission_id from the validator (in case result doesn't have it)
            generated_permission_id = permission_data.permission_id
            # If still None, generate it manually
            if generated_permission_id is None:
                from uuid import uuid4
                generated_permission_id = uuid4()
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(permission_data, table_name=self.permissions_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure permission_id is present in result (use generated one if missing)
                if "permission_id" not in result or result.get("permission_id") is None:
                    result["permission_id"] = generated_permission_id
                return Permission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating permission: {e}")
            return None

    async def get_permission_by_id(self, permission_id: UUID) -> Optional[Permission]:
        """Get permission by ID."""
        try:
            result = await self.get_by_id(permission_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Permission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting permission by ID {permission_id}: {e}")
            return None

    async def get_permission_by_name(self, permission_name: str) -> Optional[Permission]:
        """Get permission by name."""
        try:
            result = await self.find_by_field("permission_name", permission_name)
            if result:
                result = self._convert_json_to_lists(result)
                return Permission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting permission by name {permission_name}: {e}")
            return None

    async def get_all_permissions(self) -> List[Permission]:
        """Get all permissions."""
        try:
            results = await self.get_all(order_by="permission_name")
            return [Permission(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting all permissions: {e}")
            return []

    async def update_permission(self, permission_id: UUID, permission_data: PermissionUpdate) -> Optional[Permission]:
        """Update permission data."""
        try:
            data = permission_data.model_dump(exclude_unset=True)
            
            result = await self.update(permission_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Permission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating permission {permission_id}: {e}")
            return None

    async def delete_permission(self, permission_id: UUID) -> bool:
        """Delete permission and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(permission_id)
            
        except Exception as e:
            logger.error(f"Error deleting permission {permission_id}: {e}")
            return False

