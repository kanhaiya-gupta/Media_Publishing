"""
OwnLens - Security Domain: User Session Service

Service for user session management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.security import UserSessionRepository
from ...models.security.user_session import UserSession, UserSessionCreate, UserSessionUpdate

logger = logging.getLogger(__name__)


class UserSessionService(BaseService[UserSession, UserSessionCreate, UserSessionUpdate, UserSession]):
    """Service for user session management."""
    
    def __init__(self, repository: UserSessionRepository, service_name: str = None):
        """Initialize the user session service."""
        super().__init__(repository, service_name or "UserSessionService")
        self.repository: UserSessionRepository = repository
    
    def get_model_class(self):
        """Get the UserSession model class."""
        return UserSession
    
    def get_create_model_class(self):
        """Get the UserSessionCreate model class."""
        return UserSessionCreate
    
    def get_update_model_class(self):
        """Get the UserSessionUpdate model class."""
        return UserSessionUpdate
    
    def get_in_db_model_class(self):
        """Get the UserSession model class."""
        return UserSession
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user session operations."""
        try:
            if operation == "create":
                # Validate session token
                if hasattr(data, 'session_token'):
                    token = data.session_token
                else:
                    token = data.get('session_token') if isinstance(data, dict) else None
                
                if not token or len(token.strip()) == 0:
                    raise ValidationError(
                        "Session token is required",
                        "INVALID_TOKEN"
                    )
                
                # Validate expiration
                if hasattr(data, 'expires_at'):
                    expires = data.expires_at
                else:
                    expires = data.get('expires_at') if isinstance(data, dict) else None
                
                if not expires:
                    raise ValidationError(
                        "Expiration timestamp is required",
                        "INVALID_EXPIRATION"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_user_session(self, session_data: UserSessionCreate) -> UserSession:
        """Create a new user session."""
        try:
            await self.validate_input(session_data, "create")
            await self.validate_business_rules(session_data, "create")
            
            result = await self.repository.create_user_session(session_data)
            if not result:
                raise NotFoundError("Failed to create user session", "CREATE_FAILED")
            
            self.log_operation("create_user_session", {"session_id": str(result.session_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user_session", "create")
            raise
    
    async def get_user_session_by_id(self, session_id: UUID) -> UserSession:
        """Get user session by ID."""
        try:
            result = await self.repository.get_user_session_by_id(session_id)
            if not result:
                raise NotFoundError(f"User session with ID {session_id} not found", "USER_SESSION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_session_by_id", "read")
            raise
    
    async def get_user_sessions_by_user(self, user_id: UUID, limit: int = 100) -> List[UserSession]:
        """Get user sessions for a user."""
        try:
            return await self.repository.get_user_sessions_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_user_sessions_by_user", "read")
            return []

