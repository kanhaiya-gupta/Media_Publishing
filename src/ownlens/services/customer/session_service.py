"""
OwnLens - Customer Domain: Session Service

Service for customer session management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import SessionRepository
from ...models.customer.session import Session, SessionCreate, SessionUpdate

logger = logging.getLogger(__name__)


class SessionService(BaseService[Session, SessionCreate, SessionUpdate, Session]):
    """Service for customer session management."""
    
    def __init__(self, repository: SessionRepository, service_name: str = None):
        """Initialize the session service."""
        super().__init__(repository, service_name or "SessionService")
        self.repository: SessionRepository = repository
    
    def get_model_class(self):
        """Get the Session model class."""
        return Session
    
    def get_create_model_class(self):
        """Get the SessionCreate model class."""
        return SessionCreate
    
    def get_update_model_class(self):
        """Get the SessionUpdate model class."""
        return SessionUpdate
    
    def get_in_db_model_class(self):
        """Get the Session model class."""
        return Session
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for session operations."""
        try:
            if operation == "create":
                # Validate session duration
                if hasattr(data, 'duration_seconds'):
                    duration = data.duration_seconds
                else:
                    duration = data.get('duration_seconds') if isinstance(data, dict) else None
                
                if duration is not None and duration < 0:
                    raise ValidationError(
                        "Session duration must be non-negative",
                        "INVALID_DURATION"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_session(self, session_data: SessionCreate) -> Session:
        """Create a new session."""
        try:
            await self.validate_input(session_data, "create")
            await self.validate_business_rules(session_data, "create")
            
            result = await self.repository.create_session(session_data)
            if not result:
                raise NotFoundError("Failed to create session", "CREATE_FAILED")
            
            self.log_operation("create_session", {"session_id": str(result.session_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_session", "create")
            raise
    
    async def get_session_by_id(self, session_id: UUID) -> Session:
        """Get session by ID."""
        try:
            result = await self.repository.get_session_by_id(session_id)
            if not result:
                raise NotFoundError(f"Session with ID {session_id} not found", "SESSION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_session_by_id", "read")
            raise
    
    async def get_sessions_by_user(self, user_id: UUID, limit: int = 100) -> List[Session]:
        """Get sessions for a user."""
        try:
            return await self.repository.get_sessions_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_sessions_by_user", "read")
            return []
    
    async def get_sessions_by_date(self, session_date: date, brand_id: Optional[UUID] = None) -> List[Session]:
        """Get sessions for a specific date."""
        try:
            return await self.repository.get_sessions_by_date(session_date, brand_id)
        except Exception as e:
            await self.handle_error(e, "get_sessions_by_date", "read")
            return []

