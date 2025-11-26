"""
OwnLens - Audit Domain: Data Change Repository

Repository for audit_data_changes table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.data_change import DataChange, DataChangeCreate, DataChangeUpdate

logger = logging.getLogger(__name__)


class DataChangeRepository(BaseRepository[DataChange]):
    """Repository for audit_data_changes table."""

    def __init__(self, connection_manager):
        """Initialize the data change repository."""
        super().__init__(connection_manager, table_name="audit_data_changes")
        self.data_changes_table = "audit_data_changes"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "change_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_data_changes"

    # ============================================================================
    # DATA CHANGE OPERATIONS
    # ============================================================================

    async def create_data_change(self, change_data: Union[DataChangeCreate, Dict[str, Any]]) -> Optional[DataChange]:
        """
        Create a new data change record.
        
        Args:
            change_data: Data change creation data (Pydantic model or dict)
            
        Returns:
            Created data change with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(change_data, table_name=self.data_changes_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DataChange(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating data change: {e}")
            return None

    async def get_data_change_by_id(self, change_id: UUID) -> Optional[DataChange]:
        """Get data change by ID."""
        try:
            result = await self.get_by_id(change_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DataChange(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting data change by ID {change_id}: {e}")
            return None

    async def get_data_changes_by_table(self, table_name: str, limit: int = 100, offset: int = 0) -> List[DataChange]:
        """Get all data changes for a table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="change_timestamp",
                limit=limit,
                offset=offset
            )
            return [DataChange(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting data changes by table {table_name}: {e}")
            return []

    async def get_data_changes_by_record(self, table_name: str, record_id: UUID, limit: int = 100) -> List[DataChange]:
        """Get all changes for a specific record."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name, "record_id": record_id},
                order_by="change_timestamp",
                limit=limit
            )
            return [DataChange(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting data changes by record {record_id} in table {table_name}: {e}")
            return []

    async def get_data_changes_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[DataChange]:
        """Get all data changes by a user."""
        try:
            results = await self.get_all(
                filters={"changed_by": user_id},
                order_by="change_timestamp",
                limit=limit,
                offset=offset
            )
            return [DataChange(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting data changes by user {user_id}: {e}")
            return []

    async def get_data_changes_by_date_range(self, start_date: date, end_date: date, table_name: Optional[str] = None) -> List[DataChange]:
        """Get data changes within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.data_changes_table)
                .where_gte("change_timestamp", start_date)
                .where_lte("change_timestamp", end_date)
            )
            
            if table_name:
                query_builder = query_builder.where_equals("table_name", table_name)
            
            query, params = query_builder.order_by("change_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [DataChange(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting data changes by date range: {e}")
            return []

    async def update_data_change(self, change_id: UUID, change_data: DataChangeUpdate) -> Optional[DataChange]:
        """Update data change data."""
        try:
            data = change_data.model_dump(exclude_unset=True)
            
            result = await self.update(change_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DataChange(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating data change {change_id}: {e}")
            return None

    async def delete_data_change(self, change_id: UUID) -> bool:
        """Delete data change and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(change_id)
            
        except Exception as e:
            logger.error(f"Error deleting data change {change_id}: {e}")
            return False

