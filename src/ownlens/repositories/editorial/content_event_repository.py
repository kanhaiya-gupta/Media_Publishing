"""
OwnLens - Editorial Domain: Content Event Repository

Repository for editorial_content_events table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.editorial.content_event import ContentEvent, ContentEventCreate, ContentEventUpdate

logger = logging.getLogger(__name__)


class ContentEventRepository(BaseRepository[ContentEvent]):
    """Repository for editorial_content_events table."""

    def __init__(self, connection_manager):
        """Initialize the content event repository."""
        super().__init__(connection_manager, table_name="editorial_content_events")
        self.content_events_table = "editorial_content_events"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "event_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_content_events"

    # ============================================================================
    # CONTENT EVENT OPERATIONS
    # ============================================================================

    async def create_content_event(self, event_data: Union[ContentEventCreate, Dict[str, Any]]) -> Optional[ContentEvent]:
        """
        Create a new content event record.
        
        Args:
            event_data: Content event creation data (dict or ContentEventCreate model)
            
        Returns:
            Created content event with generated ID, or None if creation failed
        """
        try:
            # Convert dict to ContentEventCreate model if needed (this runs validators)
            if isinstance(event_data, dict):
                event_data = ContentEventCreate.model_validate(event_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(event_data, table_name=self.content_events_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating content event: {e}")
            return None

    async def get_content_event_by_id(self, event_id: UUID) -> Optional[ContentEvent]:
        """Get content event by ID."""
        try:
            result = await self.get_by_id(event_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content event by ID {event_id}: {e}")
            return None

    async def get_content_events_by_article(self, article_id: UUID, limit: int = 100, offset: int = 0) -> List[ContentEvent]:
        """Get all events for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="event_timestamp",
                limit=limit,
                offset=offset
            )
            return [ContentEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content events by article {article_id}: {e}")
            return []

    async def get_content_events_by_type(self, event_type: str, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[ContentEvent]:
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
            return [ContentEvent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content events by type {event_type}: {e}")
            return []

    async def get_content_events_by_date_range(self, start_date: date, end_date: date, brand_id: Optional[UUID] = None) -> List[ContentEvent]:
        """Get events within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.content_events_table)
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
            return [ContentEvent(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting content events by date range: {e}")
            return []

    async def update_content_event(self, event_id: UUID, event_data: ContentEventUpdate) -> Optional[ContentEvent]:
        """Update content event data."""
        try:
            data = event_data.model_dump(exclude_unset=True)
            
            result = await self.update(event_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentEvent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating content event {event_id}: {e}")
            return None

    async def delete_content_event(self, event_id: UUID) -> bool:
        """Delete content event and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(event_id)
            
        except Exception as e:
            logger.error(f"Error deleting content event {event_id}: {e}")
            return False

