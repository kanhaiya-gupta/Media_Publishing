"""
OwnLens - Audit Domain: Compliance Event Repository

Repository for audit_compliance_events table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.compliance_event import ComplianceEvent, ComplianceEventCreate, ComplianceEventUpdate

logger = logging.getLogger(__name__)


class ComplianceEventRepository(BaseRepository[ComplianceEvent]):
    """Repository for audit_compliance_events table."""

    def __init__(self, connection_manager):
        """Initialize the compliance event repository."""
        super().__init__(connection_manager, table_name="audit_compliance_events")
        self.compliance_events_table = "audit_compliance_events"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "event_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_compliance_events"

    # ============================================================================
    # COMPLIANCE EVENT OPERATIONS
    # ============================================================================

    async def create_compliance_event(self, event_data: Union[ComplianceEventCreate, Dict[str, Any]]) -> Optional[ComplianceEvent]:
        """
        Create a new compliance event record.
        
        Args:
            event_data: Compliance event creation data (Pydantic model or dict)
            
        Returns:
            Created compliance event with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(event_data, table_name=self.compliance_events_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ComplianceEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating compliance event: {e}")
            return None

    async def get_compliance_event_by_id(self, event_id: UUID) -> Optional[ComplianceEvent]:
        """Get compliance event by ID."""
        try:
            result = await self.get_by_id(event_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ComplianceEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting compliance event by ID {event_id}: {e}")
            return None

    async def get_compliance_events_by_type(self, event_type: str, limit: int = 100, offset: int = 0) -> List[ComplianceEvent]:
        """Get compliance events by type."""
        try:
            results = await self.get_all(
                filters={"event_type": event_type},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [ComplianceEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting compliance events by type {event_type}: {e}")
            return []

    async def get_compliance_events_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[ComplianceEvent]:
        """Get all compliance events for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [ComplianceEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting compliance events by user {user_id}: {e}")
            return []

    async def get_compliance_events_by_date_range(self, start_date: date, end_date: date, event_type: Optional[str] = None) -> List[ComplianceEvent]:
        """Get compliance events within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.compliance_events_table)
                .where_gte("event_timestamp", start_date)
                .where_lte("event_timestamp", end_date)
            )
            
            if event_type:
                query_builder = query_builder.where_equals("event_type", event_type)
            
            query, params = query_builder.order_by("event_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [ComplianceEvent(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting compliance events by date range: {e}")
            return []

    async def update_compliance_event(self, event_id: UUID, event_data: ComplianceEventUpdate) -> Optional[ComplianceEvent]:
        """Update compliance event data."""
        try:
            data = event_data.model_dump(exclude_unset=True)
            
            result = await self.update(event_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ComplianceEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating compliance event {event_id}: {e}")
            return None

    async def delete_compliance_event(self, event_id: UUID) -> bool:
        """Delete compliance event and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(event_id)
            
        except Exception as e:
            logger.error(f"Error deleting compliance event {event_id}: {e}")
            return False

