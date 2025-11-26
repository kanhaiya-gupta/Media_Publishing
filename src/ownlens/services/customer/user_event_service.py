"""
OwnLens - Customer Domain: User Event Service

Service for user event management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import UserEventRepository
from ...models.customer.user_event import UserEvent, UserEventCreate, UserEventUpdate

logger = logging.getLogger(__name__)


class UserEventService(BaseService[UserEvent, UserEventCreate, UserEventUpdate, UserEvent]):
    """Service for user event management."""
    
    def __init__(self, repository: UserEventRepository, service_name: str = None):
        """Initialize the user event service."""
        super().__init__(repository, service_name or "UserEventService")
        self.repository: UserEventRepository = repository
    
    def get_model_class(self):
        """Get the UserEvent model class."""
        return UserEvent
    
    def get_create_model_class(self):
        """Get the UserEventCreate model class."""
        return UserEventCreate
    
    def get_update_model_class(self):
        """Get the UserEventUpdate model class."""
        return UserEventUpdate
    
    def get_in_db_model_class(self):
        """Get the UserEvent model class."""
        return UserEvent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user event operations."""
        return True  # User events are typically append-only, minimal validation needed
    
    async def create_user_event(self, event_data: UserEventCreate) -> UserEvent:
        """Create a new user event."""
        try:
            await self.validate_input(event_data, "create")
            await self.validate_business_rules(event_data, "create")
            
            result = await self.repository.create_user_event(event_data)
            if not result:
                raise NotFoundError("Failed to create user event", "CREATE_FAILED")
            
            self.log_operation("create_user_event", {"event_id": str(result.event_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user_event", "create")
            raise
    
    async def get_user_event_by_id(self, event_id: UUID) -> UserEvent:
        """Get user event by ID."""
        try:
            result = await self.repository.get_user_event_by_id(event_id)
            if not result:
                raise NotFoundError(f"User event with ID {event_id} not found", "USER_EVENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_event_by_id", "read")
            raise
    
    async def get_events_by_user(self, user_id: UUID, limit: int = 100) -> List[UserEvent]:
        """Get events for a user."""
        try:
            return await self.repository.get_events_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_events_by_user", "read")
            return []
    
    async def get_events_by_date_range(self, start_date: date, end_date: date, user_id: Optional[UUID] = None) -> List[UserEvent]:
        """Get events within a date range."""
        try:
            return await self.repository.get_events_by_date_range(start_date, end_date, user_id)
        except Exception as e:
            await self.handle_error(e, "get_events_by_date_range", "read")
            return []

