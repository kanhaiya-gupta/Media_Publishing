"""
OwnLens - Base Domain: User Account Repository

Repository for user_accounts table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.user_account import UserAccount, UserAccountCreate, UserAccountUpdate

logger = logging.getLogger(__name__)


class UserAccountRepository(BaseRepository[UserAccount]):
    """Repository for user_accounts table."""

    def __init__(self, connection_manager):
        """Initialize the user account repository."""
        super().__init__(connection_manager, table_name="user_accounts")
        self.user_accounts_table = "user_accounts"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "account_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "user_accounts"

    # ============================================================================
    # USER ACCOUNT OPERATIONS
    # ============================================================================

    async def create_user_account(self, account_data: UserAccountCreate) -> Optional[UserAccount]:
        """
        Create a new user account record.
        
        Args:
            account_data: User account creation data (dict or Pydantic model)
            
        Returns:
            Created user account with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(account_data, table_name=self.user_accounts_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserAccount(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user account: {e}")
            return None

    async def get_user_account_by_id(self, account_id: UUID) -> Optional[UserAccount]:
        """Get user account by ID."""
        try:
            result = await self.get_by_id(account_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserAccount(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user account by ID {account_id}: {e}")
            return None

    async def get_user_accounts_by_user(self, user_id: UUID) -> List[UserAccount]:
        """Get all accounts for a user."""
        try:
            results = await self.find_all_by_field("user_id", user_id)
            return [UserAccount(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting user accounts by user {user_id}: {e}")
            return []

    async def get_user_accounts_by_type(self, account_type: str) -> List[UserAccount]:
        """Get accounts by account type."""
        try:
            results = await self.find_all_by_field("account_type", account_type)
            return [UserAccount(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting user accounts by type {account_type}: {e}")
            return []

    async def get_primary_account(self, user_id: UUID) -> Optional[UserAccount]:
        """Get the primary account for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id, "is_primary": True}
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return UserAccount(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting primary account for user {user_id}: {e}")
            return None

    async def update_user_account(self, account_id: UUID, account_data: UserAccountUpdate) -> Optional[UserAccount]:
        """Update user account data."""
        try:
            data = account_data.model_dump(exclude_unset=True)
            
            result = await self.update(account_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserAccount(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user account {account_id}: {e}")
            return None

    async def delete_user_account(self, account_id: UUID) -> bool:
        """Delete user account and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(account_id)
            
        except Exception as e:
            logger.error(f"Error deleting user account {account_id}: {e}")
            return False
