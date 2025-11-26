"""
OwnLens - Audit Domain: Security Event Repository

Repository for audit_security_events table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.security_event import SecurityEvent, SecurityEventCreate, SecurityEventUpdate

logger = logging.getLogger(__name__)


class SecurityEventRepository(BaseRepository[SecurityEvent]):
    """Repository for audit_security_events table."""

    def __init__(self, connection_manager):
        """Initialize the security event repository."""
        super().__init__(connection_manager, table_name="audit_security_events")
        self.security_events_table = "audit_security_events"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "event_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_security_events"

    # ============================================================================
    # SECURITY EVENT OPERATIONS
    # ============================================================================

    async def create_security_event(self, event_data: Union[SecurityEventCreate, Dict[str, Any]]) -> Optional[SecurityEvent]:
        """
        Create a new security event record.
        
        Args:
            event_data: Security event creation data (Pydantic model or dict)
            
        Returns:
            Created security event with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(event_data, table_name=self.security_events_table)
            if result:
                result = self._convert_json_to_lists(result)
                return SecurityEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating security event: {e}")
            return None

    async def get_security_event_by_id(self, event_id: UUID) -> Optional[SecurityEvent]:
        """Get security event by ID."""
        try:
            result = await self.get_by_id(event_id)
            if result:
                result = self._convert_json_to_lists(result)
                return SecurityEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting security event by ID {event_id}: {e}")
            return None

    async def get_security_events_by_type(self, event_type: str, limit: int = 100, offset: int = 0) -> List[SecurityEvent]:
        """Get security events by type."""
        try:
            results = await self.get_all(
                filters={"event_type": event_type},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [SecurityEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting security events by type {event_type}: {e}")
            return []

    async def get_security_events_by_severity(self, severity: str, limit: int = 100) -> List[SecurityEvent]:
        """Get security events by severity."""
        try:
            results = await self.get_all(
                filters={"severity": severity},
                order_by="event_timestamp",
                limit=limit
            )
            return [SecurityEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting security events by severity {severity}: {e}")
            return []

    async def get_security_events_by_date_range(self, start_date: date, end_date: date, event_type: Optional[str] = None) -> List[SecurityEvent]:
        """Get security events within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.security_events_table)
                .where_gte("event_timestamp", start_date)
                .where_lte("event_timestamp", end_date)
            )
            
            if event_type:
                query_builder = query_builder.where_equals("event_type", event_type)
            
            query, params = query_builder.order_by("event_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [SecurityEvent(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting security events by date range: {e}")
            return []

    async def update_security_event(self, event_id: UUID, event_data: SecurityEventUpdate) -> Optional[SecurityEvent]:
        """Update security event data."""
        try:
            data = event_data.model_dump(exclude_unset=True)
            
            result = await self.update(event_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return SecurityEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating security event {event_id}: {e}")
            return None

    async def delete_security_event(self, event_id: UUID) -> bool:
        """Delete security event and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(event_id)
            
        except Exception as e:
            logger.error(f"Error deleting security event {event_id}: {e}")
            return False

