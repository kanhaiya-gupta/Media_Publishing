"""
OwnLens - Customer Domain: User Segment Repository

Repository for customer_user_segments table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.customer.user_segment import (
    UserSegment, UserSegmentCreate, UserSegmentUpdate,
    UserSegmentAssignment, UserSegmentAssignmentCreate
)

logger = logging.getLogger(__name__)


class UserSegmentRepository(BaseRepository[UserSegment]):
    """Repository for customer_user_segments table."""

    def __init__(self, connection_manager):
        """Initialize the user segment repository."""
        super().__init__(connection_manager, table_name="customer_user_segments")
        self.user_segments_table = "customer_user_segments"
        self.user_segment_assignments_table = "customer_user_segment_assignments"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "segment_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "customer_user_segments"

    # ============================================================================
    # USER SEGMENT OPERATIONS
    # ============================================================================

    async def create_user_segment(self, segment_data: UserSegmentCreate) -> Optional[UserSegment]:
        """
        Create a new user segment record.
        
        Args:
            segment_data: User segment creation data (dict or Pydantic model)
            
        Returns:
            Created user segment with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(segment_data, table_name=self.user_segments_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSegment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user segment: {e}")
            return None

    async def get_user_segment_by_id(self, segment_id: UUID) -> Optional[UserSegment]:
        """Get user segment by ID."""
        try:
            result = await self.get_by_id(segment_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSegment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user segment by ID {segment_id}: {e}")
            return None

    async def get_user_segment_by_code(self, segment_code: str, brand_id: Optional[UUID] = None) -> Optional[UserSegment]:
        """Get user segment by code."""
        try:
            filters = {"segment_code": segment_code}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return UserSegment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user segment by code {segment_code}: {e}")
            return None

    async def get_user_segments_by_brand(self, brand_id: UUID, active_only: bool = True) -> List[UserSegment]:
        """Get all user segments for a brand."""
        try:
            filters = {"brand_id": brand_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(filters=filters, order_by="segment_name")
            return [UserSegment(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting user segments by brand {brand_id}: {e}")
            return []

    async def get_active_user_segments(self, brand_id: Optional[UUID] = None) -> List[UserSegment]:
        """Get all active user segments."""
        try:
            filters = {"is_active": True}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="segment_name")
            return [UserSegment(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active user segments: {e}")
            return []

    async def update_user_segment(self, segment_id: UUID, segment_data: UserSegmentUpdate) -> Optional[UserSegment]:
        """Update user segment data."""
        try:
            data = segment_data.model_dump(exclude_unset=True)
            
            result = await self.update(segment_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSegment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user segment {segment_id}: {e}")
            return None

    async def delete_user_segment(self, segment_id: UUID) -> bool:
        """Delete user segment and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(segment_id)
            
        except Exception as e:
            logger.error(f"Error deleting user segment {segment_id}: {e}")
            return False

    # ============================================================================
    # USER SEGMENT ASSIGNMENT OPERATIONS
    # ============================================================================

    async def create_user_segment_assignment(self, assignment_data: UserSegmentAssignmentCreate) -> Optional[UserSegmentAssignment]:
        """
        Create a new user segment assignment record.
        
        Args:
            assignment_data: User segment assignment creation data (dict or Pydantic model)
            
        Returns:
            Created user segment assignment with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(assignment_data, table_name=self.user_segment_assignments_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserSegmentAssignment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user segment assignment: {e}")
            return None

    async def get_user_segment_assignment_by_id(self, assignment_id: UUID) -> Optional[UserSegmentAssignment]:
        """Get user segment assignment by ID."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.user_segment_assignments_table)
                .where_equals("assignment_id", assignment_id)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            if rows:
                result = self._convert_json_to_lists(rows[0])
                return UserSegmentAssignment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user segment assignment by ID {assignment_id}: {e}")
            return None

    async def get_user_segment_assignments_by_user(self, user_id: UUID, brand_id: Optional[UUID] = None, current_only: bool = True) -> List[UserSegmentAssignment]:
        """Get all segment assignments for a user."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.user_segment_assignments_table)
                .where_equals("user_id", user_id)
            )
            
            if brand_id:
                query_builder = query_builder.where_equals("brand_id", brand_id)
            if current_only:
                query_builder = query_builder.where_equals("is_current", True)
            
            query, params = query_builder.order_by("assignment_date").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [UserSegmentAssignment(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting user segment assignments by user {user_id}: {e}")
            return []

    async def get_user_segment_assignments_by_segment(self, segment_id: UUID, current_only: bool = True) -> List[UserSegmentAssignment]:
        """Get all assignments for a segment."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.user_segment_assignments_table)
                .where_equals("segment_id", segment_id)
            )
            
            if current_only:
                query_builder = query_builder.where_equals("is_current", True)
            
            query, params = query_builder.order_by("assignment_date").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [UserSegmentAssignment(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting user segment assignments by segment {segment_id}: {e}")
            return []

