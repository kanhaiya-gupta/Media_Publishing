"""
OwnLens - Security Domain: Role Repository

Repository for security_roles table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.security.role import Role, RoleCreate, RoleUpdate

logger = logging.getLogger(__name__)


class RoleRepository(BaseRepository[Role]):
    """Repository for security_roles table."""

    def __init__(self, connection_manager):
        """Initialize the role repository."""
        super().__init__(connection_manager, table_name="security_roles")
        self.roles_table = "security_roles"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "role_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_roles"

    # ============================================================================
    # ROLE OPERATIONS
    # ============================================================================

    async def create_role(self, role_data: RoleCreate) -> Optional[Role]:
        """
        Create a new role record.
        
        Args:
            role_data: Role creation data
            
        Returns:
            Created role with generated ID, or None if creation failed
        """
        try:
            data = role_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.roles_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Role(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating role: {e}")
            return None

    async def get_role_by_id(self, role_id: UUID) -> Optional[Role]:
        """Get role by ID."""
        try:
            result = await self.get_by_id(role_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Role(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting role by ID {role_id}: {e}")
            return None

    async def get_role_by_name(self, role_name: str) -> Optional[Role]:
        """Get role by name."""
        try:
            result = await self.find_by_field("role_name", role_name)
            if result:
                result = self._convert_json_to_lists(result)
                return Role(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting role by name {role_name}: {e}")
            return None

    async def get_all_roles(self) -> List[Role]:
        """Get all roles."""
        try:
            results = await self.get_all(order_by="role_name")
            return [Role(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting all roles: {e}")
            return []

    async def update_role(self, role_id: UUID, role_data: RoleUpdate) -> Optional[Role]:
        """Update role data."""
        try:
            data = role_data.model_dump(exclude_unset=True)
            
            result = await self.update(role_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Role(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating role {role_id}: {e}")
            return None

    async def delete_role(self, role_id: UUID) -> bool:
        """Delete role and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(role_id)
            
        except Exception as e:
            logger.error(f"Error deleting role {role_id}: {e}")
            return False

