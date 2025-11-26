"""
OwnLens - Audit Domain: Audit Log Repository

Repository for audit_logs table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.audit_log import AuditLog, AuditLogCreate, AuditLogUpdate

logger = logging.getLogger(__name__)


class AuditLogRepository(BaseRepository[AuditLog]):
    """Repository for audit_logs table."""

    def __init__(self, connection_manager):
        """Initialize the audit log repository."""
        super().__init__(connection_manager, table_name="audit_logs")
        self.audit_logs_table = "audit_logs"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "log_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_logs"

    # ============================================================================
    # AUDIT LOG OPERATIONS
    # ============================================================================

    async def create_audit_log(self, log_data: Union[AuditLogCreate, Dict[str, Any]]) -> Optional[AuditLog]:
        """
        Create a new audit log record.
        
        Args:
            log_data: Audit log creation data (Pydantic model or dict)
            
        Returns:
            Created audit log with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(log_data, table_name=self.audit_logs_table)
            if result:
                result = self._convert_json_to_lists(result)
                return AuditLog(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating audit log: {e}")
            return None

    async def get_audit_log_by_id(self, log_id: UUID) -> Optional[AuditLog]:
        """Get audit log by ID."""
        try:
            result = await self.get_by_id(log_id)
            if result:
                result = self._convert_json_to_lists(result)
                return AuditLog(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting audit log by ID {log_id}: {e}")
            return None

    async def get_audit_logs_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[AuditLog]:
        """Get all audit logs for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="log_timestamp",
                limit=limit,
                offset=offset
            )
            return [AuditLog(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting audit logs by user {user_id}: {e}")
            return []

    async def get_audit_logs_by_action(self, action: str, limit: int = 100, offset: int = 0) -> List[AuditLog]:
        """Get audit logs by action."""
        try:
            results = await self.get_all(
                filters={"action": action},
                order_by="log_timestamp",
                limit=limit,
                offset=offset
            )
            return [AuditLog(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting audit logs by action {action}: {e}")
            return []

    async def get_audit_logs_by_date_range(self, start_date: date, end_date: date, user_id: Optional[UUID] = None) -> List[AuditLog]:
        """Get audit logs within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.audit_logs_table)
                .where_gte("log_timestamp", start_date)
                .where_lte("log_timestamp", end_date)
            )
            
            if user_id:
                query_builder = query_builder.where_equals("user_id", user_id)
            
            query, params = query_builder.order_by("log_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [AuditLog(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting audit logs by date range: {e}")
            return []

    async def update_audit_log(self, log_id: UUID, log_data: AuditLogUpdate) -> Optional[AuditLog]:
        """Update audit log data."""
        try:
            data = log_data.model_dump(exclude_unset=True)
            
            result = await self.update(log_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return AuditLog(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating audit log {log_id}: {e}")
            return None

    async def delete_audit_log(self, log_id: UUID) -> bool:
        """Delete audit log and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(log_id)
            
        except Exception as e:
            logger.error(f"Error deleting audit log {log_id}: {e}")
            return False

