"""
OwnLens - Customer Domain: Session Repository

Repository for customer_sessions table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseRepository
from ...models.customer.session import Session, SessionCreate, SessionUpdate

logger = logging.getLogger(__name__)


class SessionRepository(BaseRepository[Session]):
    """Repository for customer_sessions table."""

    def __init__(self, connection_manager):
        """Initialize the session repository."""
        super().__init__(connection_manager, table_name="customer_sessions")
        self.sessions_table = "customer_sessions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "session_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_sessions"

    # ============================================================================
    # SESSION OPERATIONS
    # ============================================================================

    async def create_session(self, session_data: SessionCreate) -> Optional[Session]:
        """
        Create a new session record.
        
        Args:
            session_data: Session creation data
            
        Returns:
            Created session with generated ID, or None if creation failed
        """
        try:
            data = session_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.sessions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Session(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            return None

    async def get_session_by_id(self, session_id: UUID) -> Optional[Session]:
        """Get session by ID."""
        try:
            result = await self.get_by_id(session_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Session(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting session by ID {session_id}: {e}")
            return None

    async def get_sessions_by_user(self, user_id: UUID, limit: int = 100, offset: int = 0) -> List[Session]:
        """Get all sessions for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="session_start",
                limit=limit,
                offset=offset
            )
            return [Session(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting sessions by user {user_id}: {e}")
            return []

    async def get_sessions_by_brand(self, brand_id: UUID, limit: int = 100, offset: int = 0) -> List[Session]:
        """Get all sessions for a brand."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by="session_start",
                limit=limit,
                offset=offset
            )
            return [Session(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting sessions by brand {brand_id}: {e}")
            return []

    async def get_active_sessions(self, brand_id: Optional[UUID] = None) -> List[Session]:
        """Get all active sessions (sessions without an end time)."""
        try:
            from ..shared.query_builder import select_all, OrderDirection
            
            query, params = (
                select_all()
                .table(self.sessions_table)
                .where_is_null("session_end")
                .order_by("session_start", OrderDirection.DESC)
                .build()
            )
            
            if brand_id:
                query = query.replace("WHERE session_end IS NULL", f"WHERE session_end IS NULL AND brand_id = $1")
                params = [brand_id] if params is None else [brand_id] + params
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [Session(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting active sessions: {e}")
            return []

    async def update_session(self, session_id: UUID, session_data: SessionUpdate) -> Optional[Session]:
        """Update session data."""
        try:
            data = session_data.model_dump(exclude_unset=True)
            
            result = await self.update(session_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Session(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating session {session_id}: {e}")
            return None

    async def delete_session(self, session_id: UUID) -> bool:
        """Delete session and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(session_id)
            
        except Exception as e:
            logger.error(f"Error deleting session {session_id}: {e}")
            return False

