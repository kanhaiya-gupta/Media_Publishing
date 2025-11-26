"""
OwnLens - Security Domain: API Key Repository

Repository for security_api_keys table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.security.api_key import ApiKey, ApiKeyCreate, ApiKeyUpdate

logger = logging.getLogger(__name__)


class ApiKeyRepository(BaseRepository[ApiKey]):
    """Repository for security_api_keys table."""

    def __init__(self, connection_manager):
        """Initialize the API key repository."""
        super().__init__(connection_manager, table_name="security_api_keys")
        self.api_keys_table = "security_api_keys"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "api_key_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_api_keys"

    # ============================================================================
    # API KEY OPERATIONS
    # ============================================================================

    async def create_api_key(self, api_key_data: ApiKeyCreate) -> Optional[ApiKey]:
        """
        Create a new API key record.
        
        Args:
            api_key_data: API key creation data
            
        Returns:
            Created API key with generated ID, or None if creation failed
        """
        try:
            data = api_key_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.api_keys_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKey(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating API key: {e}")
            return None

    async def get_api_key_by_id(self, api_key_id: UUID) -> Optional[ApiKey]:
        """Get API key by ID."""
        try:
            result = await self.get_by_id(api_key_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKey(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting API key by ID {api_key_id}: {e}")
            return None

    async def get_api_key_by_key_hash(self, key_hash: str) -> Optional[ApiKey]:
        """Get API key by key hash."""
        try:
            result = await self.find_by_field("key_hash", key_hash)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKey(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting API key by key hash: {e}")
            return None

    async def get_api_keys_by_user(self, user_id: UUID, active_only: bool = True) -> List[ApiKey]:
        """Get all API keys for a user."""
        try:
            filters = {"user_id": user_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(filters=filters, order_by="created_at")
            return [ApiKey(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting API keys by user {user_id}: {e}")
            return []

    async def get_active_api_keys(self) -> List[ApiKey]:
        """Get all active API keys."""
        try:
            results = await self.get_all(
                filters={"is_active": True},
                order_by="created_at"
            )
            return [ApiKey(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active API keys: {e}")
            return []

    async def update_api_key(self, api_key_id: UUID, api_key_data: ApiKeyUpdate) -> Optional[ApiKey]:
        """Update API key data."""
        try:
            data = api_key_data.model_dump(exclude_unset=True)
            
            result = await self.update(api_key_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKey(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating API key {api_key_id}: {e}")
            return None

    async def delete_api_key(self, api_key_id: UUID) -> bool:
        """Delete API key and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(api_key_id)
            
        except Exception as e:
            logger.error(f"Error deleting API key {api_key_id}: {e}")
            return False

