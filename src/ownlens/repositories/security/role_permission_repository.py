"""
OwnLens - Security Domain: Role Permission Repository

Repository for security_role_permissions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.security.role_permission import RolePermission, RolePermissionCreate, RolePermissionUpdate

logger = logging.getLogger(__name__)


class RolePermissionRepository(BaseRepository[RolePermission]):
    """Repository for security_role_permissions table."""

    def __init__(self, connection_manager):
        """Initialize the role permission repository."""
        super().__init__(connection_manager, table_name="security_role_permissions")
        self.role_permissions_table = "security_role_permissions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "role_permission_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_role_permissions"

    # ============================================================================
    # ROLE PERMISSION OPERATIONS
    # ============================================================================

    async def create_role_permission(self, role_permission_data: Union[RolePermissionCreate, Dict[str, Any]]) -> Optional[RolePermission]:
        """
        Create a new role permission record.
        
        Args:
            role_permission_data: Role permission creation data (dict or RolePermissionCreate model)
            
        Returns:
            Created role permission with generated ID, or None if creation failed
        """
        try:
            # Convert dict to RolePermissionCreate model if needed (this runs validators)
            if isinstance(role_permission_data, dict):
                role_permission_data = RolePermissionCreate.model_validate(role_permission_data)
            else:
                # If it's already a model but ID is None, re-validate to trigger validator
                if hasattr(role_permission_data, 'role_permission_id') and role_permission_data.role_permission_id is None:
                    # Re-validate to trigger the ID generator validator
                    role_permission_data = RolePermissionCreate.model_validate(role_permission_data.model_dump())
            
            # Store the generated role_permission_id from the validator (in case result doesn't have it)
            generated_role_permission_id = role_permission_data.role_permission_id
            # If still None, generate it manually
            if generated_role_permission_id is None:
                from uuid import uuid4
                generated_role_permission_id = uuid4()
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(role_permission_data, table_name=self.role_permissions_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure role_permission_id is present in result (use generated one if missing)
                if "role_permission_id" not in result or result.get("role_permission_id") is None:
                    result["role_permission_id"] = generated_role_permission_id
                return RolePermission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating role permission: {e}")
            return None

    async def get_role_permission_by_id(self, role_permission_id: UUID) -> Optional[RolePermission]:
        """Get role permission by ID."""
        try:
            result = await self.get_by_id(role_permission_id)
            if result:
                result = self._convert_json_to_lists(result)
                return RolePermission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting role permission by ID {role_permission_id}: {e}")
            return None

    async def get_permissions_by_role(self, role_id: UUID) -> List[RolePermission]:
        """Get all permissions for a role."""
        try:
            results = await self.get_all(
                filters={"role_id": role_id},
                order_by="permission_id"
            )
            return [RolePermission(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting permissions by role {role_id}: {e}")
            return []

    async def get_roles_by_permission(self, permission_id: UUID) -> List[RolePermission]:
        """Get all roles with a specific permission."""
        try:
            results = await self.get_all(
                filters={"permission_id": permission_id},
                order_by="role_id"
            )
            return [RolePermission(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting roles by permission {permission_id}: {e}")
            return []

    async def update_role_permission(self, role_permission_id: UUID, role_permission_data: RolePermissionUpdate) -> Optional[RolePermission]:
        """Update role permission data."""
        try:
            data = role_permission_data.model_dump(exclude_unset=True)
            
            result = await self.update(role_permission_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return RolePermission(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating role permission {role_permission_id}: {e}")
            return None

    async def delete_role_permission(self, role_permission_id: UUID) -> bool:
        """Delete role permission and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(role_permission_id)
            
        except Exception as e:
            logger.error(f"Error deleting role permission {role_permission_id}: {e}")
            return False

