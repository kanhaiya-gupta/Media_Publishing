"""
OwnLens - Security Domain: User Session Repository

Repository for security_user_sessions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.security.user_session import UserSession, UserSessionCreate, UserSessionUpdate

logger = logging.getLogger(__name__)


class UserSessionRepository(BaseRepository[UserSession]):
    """Repository for security_user_sessions table."""

    def __init__(self, connection_manager):
        """Initialize the user session repository."""
        super().__init__(connection_manager, table_name="security_user_sessions")
        self.user_sessions_table = "security_user_sessions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "session_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "security_user_sessions"

    # ============================================================================
    # USER SESSION OPERATIONS
    # ============================================================================

    async def create_user_session(self, session_data: Union[UserSessionCreate, Dict[str, Any]]) -> Optional[UserSession]:
        """
        Create a new user session record.
        
        Args:
            session_data: User session creation data (dict or UserSessionCreate model)
            
        Returns:
            Created user session with generated ID, or None if creation failed
        """
        try:
            if isinstance(session_data, dict):
                session_data = UserSessionCreate.model_validate(session_data)
            data = session_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.user_sessions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSession(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user session: {e}")
            return None

    async def get_user_session_by_id(self, session_id: UUID) -> Optional[UserSession]:
        """Get user session by ID."""
        try:
            result = await self.get_by_id(session_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSession(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user session by ID {session_id}: {e}")
            return None

    async def get_user_session_by_token(self, session_token: str) -> Optional[UserSession]:
        """Get user session by session token."""
        try:
            result = await self.find_by_field("session_token", session_token)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSession(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user session by token: {e}")
            return None

    async def get_sessions_by_user(self, user_id: UUID, active_only: bool = True) -> List[UserSession]:
        """Get all sessions for a user."""
        try:
            filters = {"user_id": user_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(
                filters=filters,
                order_by="created_at"
            )
            return [UserSession(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting sessions by user {user_id}: {e}")
            return []

    async def get_active_sessions(self, user_id: Optional[UUID] = None) -> List[UserSession]:
        """Get all active sessions."""
        try:
            filters = {"is_active": True}
            if user_id:
                filters["user_id"] = user_id
            
            results = await self.get_all(filters=filters, order_by="created_at")
            return [UserSession(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active sessions: {e}")
            return []

    async def update_user_session(self, session_id: UUID, session_data: UserSessionUpdate) -> Optional[UserSession]:
        """Update user session data."""
        try:
            data = session_data.model_dump(exclude_unset=True)
            
            result = await self.update(session_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSession(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user session {session_id}: {e}")
            return None

    async def delete_user_session(self, session_id: UUID) -> bool:
        """Delete user session and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(session_id)
            
        except Exception as e:
            logger.error(f"Error deleting user session {session_id}: {e}")
            return False

