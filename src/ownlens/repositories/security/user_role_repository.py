"""
OwnLens - Security Domain: User Role Repository

Repository for security_user_roles table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.security.user_role import UserRole, UserRoleCreate, UserRoleUpdate

logger = logging.getLogger(__name__)


class UserRoleRepository(BaseRepository[UserRole]):
    """Repository for security_user_roles table."""

    def __init__(self, connection_manager):
        """Initialize the user role repository."""
        super().__init__(connection_manager, table_name="security_user_roles")
        self.user_roles_table = "security_user_roles"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "user_role_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_user_roles"

    # ============================================================================
    # USER ROLE OPERATIONS
    # ============================================================================

    async def create_user_role(self, user_role_data: Union[UserRoleCreate, Dict[str, Any]]) -> Optional[UserRole]:
        """
        Create a new user role record.
        
        Args:
            user_role_data: User role creation data (Pydantic model or dict)
            
        Returns:
            Created user role with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(user_role_data, dict):
                user_role_data = UserRoleCreate.model_validate(user_role_data)
            
            data = user_role_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.user_roles_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserRole(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user role: {e}")
            return None

    async def get_user_role_by_id(self, user_role_id: UUID) -> Optional[UserRole]:
        """Get user role by ID."""
        try:
            result = await self.get_by_id(user_role_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserRole(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user role by ID {user_role_id}: {e}")
            return None

    async def get_roles_by_user(self, user_id: UUID) -> List[UserRole]:
        """Get all roles for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="assigned_at"
            )
            return [UserRole(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting roles by user {user_id}: {e}")
            return []

    async def get_users_by_role(self, role_id: UUID) -> List[UserRole]:
        """Get all users with a specific role."""
        try:
            results = await self.get_all(
                filters={"role_id": role_id},
                order_by="assigned_at"
            )
            return [UserRole(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting users by role {role_id}: {e}")
            return []

    async def update_user_role(self, user_role_id: UUID, user_role_data: UserRoleUpdate) -> Optional[UserRole]:
        """Update user role data."""
        try:
            data = user_role_data.model_dump(exclude_unset=True)
            
            result = await self.update(user_role_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserRole(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user role {user_role_id}: {e}")
            return None

    async def delete_user_role(self, user_role_id: UUID) -> bool:
        """Delete user role and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(user_role_id)
            
        except Exception as e:
            logger.error(f"Error deleting user role {user_role_id}: {e}")
            return False

