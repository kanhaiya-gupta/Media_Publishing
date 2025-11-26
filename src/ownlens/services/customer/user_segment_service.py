"""
OwnLens - Customer Domain: User Segment Service

Service for user segment management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import UserSegmentRepository
from ...models.customer.user_segment import (
    UserSegment, UserSegmentCreate, UserSegmentUpdate,
    UserSegmentAssignment, UserSegmentAssignmentCreate
)

logger = logging.getLogger(__name__)


class UserSegmentService(BaseService[UserSegment, UserSegmentCreate, UserSegmentUpdate, UserSegment]):
    """Service for user segment management."""
    
    def __init__(self, repository: UserSegmentRepository, service_name: str = None):
        """Initialize the user segment service."""
        super().__init__(repository, service_name or "UserSegmentService")
        self.repository: UserSegmentRepository = repository
    
    def get_model_class(self):
        """Get the UserSegment model class."""
        return UserSegment
    
    def get_create_model_class(self):
        """Get the UserSegmentCreate model class."""
        return UserSegmentCreate
    
    def get_update_model_class(self):
        """Get the UserSegmentUpdate model class."""
        return UserSegmentUpdate
    
    def get_in_db_model_class(self):
        """Get the UserSegment model class."""
        return UserSegment
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user segment operations."""
        try:
            if operation == "create":
                # Check for duplicate segment code within brand
                if hasattr(data, 'segment_code') and hasattr(data, 'brand_id'):
                    segment_code = data.segment_code
                    brand_id = data.brand_id
                else:
                    segment_code = data.get('segment_code') if isinstance(data, dict) else None
                    brand_id = data.get('brand_id') if isinstance(data, dict) else None
                
                if segment_code and brand_id:
                    existing = await self.repository.get_user_segment_by_code(segment_code, brand_id)
                    if existing:
                        raise ValidationError(
                            f"Segment with code '{segment_code}' already exists for this brand",
                            "DUPLICATE_SEGMENT_CODE"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_user_segment(self, segment_data: UserSegmentCreate) -> UserSegment:
        """Create a new user segment."""
        try:
            await self.validate_input(segment_data, "create")
            await self.validate_business_rules(segment_data, "create")
            
            result = await self.repository.create_user_segment(segment_data)
            if not result:
                raise NotFoundError("Failed to create user segment", "CREATE_FAILED")
            
            self.log_operation("create_user_segment", {"segment_id": str(result.segment_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user_segment", "create")
            raise
    
    async def get_user_segment_by_id(self, segment_id: UUID) -> UserSegment:
        """Get user segment by ID."""
        try:
            result = await self.repository.get_user_segment_by_id(segment_id)
            if not result:
                raise NotFoundError(f"User segment with ID {segment_id} not found", "USER_SEGMENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_segment_by_id", "read")
            raise
    
    async def assign_user_to_segment(self, assignment_data: UserSegmentAssignmentCreate) -> UserSegmentAssignment:
        """Assign a user to a segment."""
        try:
            await self.validate_input(assignment_data, "create")
            
            result = await self.repository.create_user_segment_assignment(assignment_data)
            if not result:
                raise NotFoundError("Failed to assign user to segment", "ASSIGNMENT_FAILED")
            
            self.log_operation("assign_user_to_segment", {"assignment_id": str(result.assignment_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "assign_user_to_segment", "create")
            raise
    
    async def get_user_segments(self, user_id: UUID, brand_id: Optional[UUID] = None) -> List[UserSegmentAssignment]:
        """Get all segments for a user."""
        try:
            return await self.repository.get_user_segment_assignments_by_user(user_id, brand_id)
        except Exception as e:
            await self.handle_error(e, "get_user_segments", "read")
            return []

