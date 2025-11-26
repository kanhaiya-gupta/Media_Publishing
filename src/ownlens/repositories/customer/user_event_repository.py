"""
OwnLens - Customer Domain: User Event Repository

Repository for customer_events table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.customer.user_event import UserEvent, UserEventCreate, UserEventUpdate

logger = logging.getLogger(__name__)


class UserEventRepository(BaseRepository[UserEvent]):
    """Repository for customer_events table."""

    def __init__(self, connection_manager):
        """Initialize the user event repository."""
        super().__init__(connection_manager, table_name="customer_events")
        self.events_table = "customer_events"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "event_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_events"

    # ============================================================================
    # USER EVENT OPERATIONS
    # ============================================================================

    async def create_user_event(self, event_data: Union[UserEventCreate, Dict[str, Any]]) -> Optional[UserEvent]:
        """
        Create a new user event record.
        
        Args:
            event_data: User event creation data (dict or UserEventCreate model)
            
        Returns:
            Created user event with generated ID, or None if creation failed
        """
        try:
            # Convert dict to UserEventCreate model if needed (this runs validators)
            if isinstance(event_data, dict):
                event_data = UserEventCreate.model_validate(event_data)
            
            # Convert to dict - include event_date for partition routing
            # The trigger will only set event_date if it's NULL, but we need it for partition routing
            if isinstance(event_data, UserEventCreate):
                data_dict = event_data.model_dump(exclude_unset=True)
            else:
                data_dict = dict(event_data)
            
            # Ensure event_date is set - derive from event_timestamp if not provided
            if 'event_date' not in data_dict or data_dict.get('event_date') is None:
                if 'event_timestamp' in data_dict and data_dict['event_timestamp']:
                    event_timestamp = data_dict['event_timestamp']
                    if isinstance(event_timestamp, datetime):
                        data_dict['event_date'] = event_timestamp.date()
                    elif isinstance(event_timestamp, str):
                        # Parse string to datetime, then get date
                        from dateutil.parser import parse
                        dt = parse(event_timestamp)
                        data_dict['event_date'] = dt.date()
                    else:
                        # Fallback to today if we can't parse
                        data_dict['event_date'] = date.today()
                else:
                    # No event_timestamp either, use today
                    data_dict['event_date'] = date.today()
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(data_dict, table_name=self.events_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user event: {e}")
            return None

    async def get_user_event_by_id(self, event_id: UUID) -> Optional[UserEvent]:
        """Get user event by ID."""
        try:
            result = await self.get_by_id(event_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user event by ID {event_id}: {e}")
            return None

    async def get_events_by_session(self, session_id: UUID, limit: int = 100, offset: int = 0) -> List[UserEvent]:
        """Get all events for a session."""
        try:
            results = await self.get_all(
                filters={"session_id": session_id},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [UserEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting events by session {session_id}: {e}")
            return []

    async def get_events_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[UserEvent]:
        """Get all events for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [UserEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting events by user {user_id}: {e}")
            return []

    async def get_events_by_type(self, event_type: str, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[UserEvent]:
        """Get events by event type."""
        try:
            filters = {"event_type": event_type}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [UserEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting events by type {event_type}: {e}")
            return []

    async def get_events_by_date_range(self, start_date: date, end_date: date, brand_id: Optional[UUID] = None) -> List[UserEvent]:
        """Get events within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.events_table)
                .where_gte("event_timestamp", start_date)
                .where_lte("event_timestamp", end_date)
                .order_by("event_timestamp")
                .build()
            )
            
            if brand_id:
                query = query.replace("WHERE", f"WHERE brand_id = $1 AND")
                params = [brand_id] + list(params) if params else [brand_id]
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [UserEvent(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting events by date range: {e}")
            return []

    async def update_user_event(self, event_id: UUID, event_data: UserEventUpdate) -> Optional[UserEvent]:
        """Update user event data."""
        try:
            data = event_data.model_dump(exclude_unset=True)
            
            result = await self.update(event_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user event {event_id}: {e}")
            return None

    async def delete_user_event(self, event_id: UUID) -> bool:
        """Delete user event and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(event_id)
            
        except Exception as e:
            logger.error(f"Error deleting user event {event_id}: {e}")
            return False

