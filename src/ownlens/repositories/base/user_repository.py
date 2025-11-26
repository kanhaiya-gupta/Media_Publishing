"""
OwnLens - Base Domain: User Repository

Repository for users table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.customer.user import User, UserCreate, UserUpdate

logger = logging.getLogger(__name__)


class UserRepository(BaseRepository[User]):
    """Repository for users table."""

    def __init__(self, connection_manager):
        """Initialize the user repository."""
        super().__init__(connection_manager, table_name="users")
        self.users_table = "users"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "user_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "users"

    # ============================================================================
    # USER OPERATIONS
    # ============================================================================

    async def create_user(self, user_data: UserCreate) -> Optional[User]:
        """
        Create a new user record.
        
        Args:
            user_data: User creation data (dict or Pydantic model)
            
        Returns:
            Created user with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(user_data, table_name=self.users_table)
            if result:
                result = self._convert_json_to_lists(result)
                return User(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return None

    async def get_user_by_id(self, user_id: UUID) -> Optional[User]:
        """Get user by ID."""
        try:
            result = await self.get_by_id(user_id)
            if result:
                result = self._convert_json_to_lists(result)
                return User(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by ID {user_id}: {e}")
            return None

    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email address."""
        try:
            result = await self.find_by_field("email", email)
            if result:
                result = self._convert_json_to_lists(result)
                return User(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by email {email}: {e}")
            return None

    async def get_users_by_country(self, country_code: str) -> List[User]:
        """Get users by country code."""
        try:
            results = await self.find_all_by_field("primary_country_code", country_code)
            return [User(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting users by country {country_code}: {e}")
            return []

    async def get_active_users(self) -> List[User]:
        """Get all active users."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="email")
            return [User(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active users: {e}")
            return []

    async def update_user(self, user_id: UUID, user_data: UserUpdate) -> Optional[User]:
        """Update user data."""
        try:
            # Convert Pydantic model to dict for update
            data = self._convert_model_to_dict(user_data)
            result = await self.update(user_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return User(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user {user_id}: {e}")
            return None

    async def delete_user(self, user_id: UUID) -> bool:
        """Delete user and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(user_id)
            
        except Exception as e:
            logger.error(f"Error deleting user {user_id}: {e}")
            return False
