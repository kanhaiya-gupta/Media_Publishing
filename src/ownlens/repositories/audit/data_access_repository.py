"""
OwnLens - Audit Domain: Data Access Repository

Repository for audit_data_access table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.data_access import DataAccess, DataAccessCreate, DataAccessUpdate

logger = logging.getLogger(__name__)


class DataAccessRepository(BaseRepository[DataAccess]):
    """Repository for audit_data_access table."""

    def __init__(self, connection_manager):
        """Initialize the data access repository."""
        super().__init__(connection_manager, table_name="audit_data_access")
        self.data_access_table = "audit_data_access"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "access_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_data_access"

    # ============================================================================
    # DATA ACCESS OPERATIONS
    # ============================================================================

    async def create_data_access(self, access_data: Union[DataAccessCreate, Dict[str, Any]]) -> Optional[DataAccess]:
        """
        Create a new data access record.
        
        Args:
            access_data: Data access creation data (Pydantic model or dict)
            
        Returns:
            Created data access with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(access_data, table_name=self.data_access_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DataAccess(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating data access: {e}")
            return None

    async def get_data_access_by_id(self, access_id: UUID) -> Optional[DataAccess]:
        """Get data access by ID."""
        try:
            result = await self.get_by_id(access_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DataAccess(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting data access by ID {access_id}: {e}")
            return None

    async def get_data_access_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[DataAccess]:
        """Get all data access records for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="access_timestamp",
                limit=limit,
                offset=offset
            )
            return [DataAccess(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting data access by user {user_id}: {e}")
            return []

    async def get_data_access_by_table(self, table_name: str, limit: int = 100, offset: int = 0) -> List[DataAccess]:
        """Get all data access records for a table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="access_timestamp",
                limit=limit,
                offset=offset
            )
            return [DataAccess(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting data access by table {table_name}: {e}")
            return []

    async def get_data_access_by_date_range(self, start_date: date, end_date: date, user_id: Optional[UUID] = None) -> List[DataAccess]:
        """Get data access records within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.data_access_table)
                .where_gte("access_timestamp", start_date)
                .where_lte("access_timestamp", end_date)
            )
            
            if user_id:
                query_builder = query_builder.where_equals("user_id", user_id)
            
            query, params = query_builder.order_by("access_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [DataAccess(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting data access by date range: {e}")
            return []

    async def update_data_access(self, access_id: UUID, access_data: DataAccessUpdate) -> Optional[DataAccess]:
        """Update data access data."""
        try:
            data = access_data.model_dump(exclude_unset=True)
            
            result = await self.update(access_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DataAccess(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating data access {access_id}: {e}")
            return None

    async def delete_data_access(self, access_id: UUID) -> bool:
        """Delete data access and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(access_id)
            
        except Exception as e:
            logger.error(f"Error deleting data access {access_id}: {e}")
            return False

