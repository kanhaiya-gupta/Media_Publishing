"""
OwnLens - Security Domain: API Key Usage Repository

Repository for security_api_key_usage table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.security.api_key_usage import ApiKeyUsage, ApiKeyUsageCreate, ApiKeyUsageUpdate

logger = logging.getLogger(__name__)


class ApiKeyUsageRepository(BaseRepository[ApiKeyUsage]):
    """Repository for security_api_key_usage table."""

    def __init__(self, connection_manager):
        """Initialize the API key usage repository."""
        super().__init__(connection_manager, table_name="security_api_key_usage")
        self.api_key_usage_table = "security_api_key_usage"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "usage_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_api_key_usage"

    # ============================================================================
    # API KEY USAGE OPERATIONS
    # ============================================================================

    async def create_api_key_usage(self, usage_data: Union[ApiKeyUsageCreate, Dict[str, Any]]) -> Optional[ApiKeyUsage]:
        """
        Create a new API key usage record.
        
        Args:
            usage_data: API key usage creation data (Pydantic model or dict)
            
        Returns:
            Created API key usage with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(usage_data, table_name=self.api_key_usage_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKeyUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating API key usage: {e}")
            return None

    async def get_api_key_usage_by_id(self, usage_id: UUID) -> Optional[ApiKeyUsage]:
        """Get API key usage by ID."""
        try:
            result = await self.get_by_id(usage_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKeyUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting API key usage by ID {usage_id}: {e}")
            return None

    async def get_api_key_usage_by_api_key(self, api_key_id: UUID, limit: int = 100, offset: int = 0) -> List[ApiKeyUsage]:
        """Get all usage records for an API key."""
        try:
            results = await self.get_all(
                filters={"api_key_id": api_key_id},
                order_by="usage_timestamp",
                limit=limit,
                offset=offset
            )
            return [ApiKeyUsage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting API key usage by API key {api_key_id}: {e}")
            return []

    async def get_api_key_usage_by_date_range(self, start_date: date, end_date: date, api_key_id: Optional[UUID] = None) -> List[ApiKeyUsage]:
        """Get usage records within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.api_key_usage_table)
                .where_gte("usage_timestamp", start_date)
                .where_lte("usage_timestamp", end_date)
            )
            
            if api_key_id:
                query_builder = query_builder.where_equals("api_key_id", api_key_id)
            
            query, params = query_builder.order_by("usage_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [ApiKeyUsage(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting API key usage by date range: {e}")
            return []

    async def update_api_key_usage(self, usage_id: UUID, usage_data: Union[ApiKeyUsageUpdate, Dict[str, Any]]) -> Optional[ApiKeyUsage]:
        """Update API key usage data."""
        try:
            # Base update() method already handles dict/Pydantic model conversion
            result = await self.update(usage_id, usage_data)
            if result:
                result = self._convert_json_to_lists(result)
                return ApiKeyUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating API key usage {usage_id}: {e}")
            return None

    async def delete_api_key_usage(self, usage_id: UUID) -> bool:
        """Delete API key usage and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(usage_id)
            
        except Exception as e:
            logger.error(f"Error deleting API key usage {usage_id}: {e}")
            return False

