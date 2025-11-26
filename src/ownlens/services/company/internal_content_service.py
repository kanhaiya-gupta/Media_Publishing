"""
OwnLens - Company Domain: Internal Content Service

Service for internal content management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.company import InternalContentRepository
from ...models.company.internal_content import InternalContent, InternalContentCreate, InternalContentUpdate

logger = logging.getLogger(__name__)


class InternalContentService(BaseService[InternalContent, InternalContentCreate, InternalContentUpdate, InternalContent]):
    """Service for internal content management."""
    
    def __init__(self, repository: InternalContentRepository, service_name: str = None):
        """Initialize the internal content service."""
        super().__init__(repository, service_name or "InternalContentService")
        self.repository: InternalContentRepository = repository
    
    def get_model_class(self):
        """Get the InternalContent model class."""
        return InternalContent
    
    def get_create_model_class(self):
        """Get the InternalContentCreate model class."""
        return InternalContentCreate
    
    def get_update_model_class(self):
        """Get the InternalContentUpdate model class."""
        return InternalContentUpdate
    
    def get_in_db_model_class(self):
        """Get the InternalContent model class."""
        return InternalContent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for internal content operations."""
        try:
            # Validate content type
            if hasattr(data, 'content_type'):
                content_type = data.content_type
            else:
                content_type = data.get('content_type') if isinstance(data, dict) else None
            
            if content_type and content_type not in ['article', 'announcement', 'policy', 'training', 'newsletter', 'other']:
                raise ValidationError(
                    "Invalid content type",
                    "INVALID_CONTENT_TYPE"
                )
            
            # Validate status
            if hasattr(data, 'status'):
                status = data.status
            else:
                status = data.get('status') if isinstance(data, dict) else None
            
            if status and status not in ['draft', 'scheduled', 'published', 'archived']:
                raise ValidationError(
                    "Invalid content status",
                    "INVALID_STATUS"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_internal_content(self, content_data: InternalContentCreate) -> InternalContent:
        """Create new internal content."""
        try:
            await self.validate_input(content_data, "create")
            await self.validate_business_rules(content_data, "create")
            
            result = await self.repository.create_internal_content(content_data)
            if not result:
                raise NotFoundError("Failed to create internal content", "CREATE_FAILED")
            
            self.log_operation("create_internal_content", {"content_id": str(result.content_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_internal_content", "create")
            raise
    
    async def get_internal_content_by_id(self, content_id: UUID) -> InternalContent:
        """Get internal content by ID."""
        try:
            result = await self.repository.get_internal_content_by_id(content_id)
            if not result:
                raise NotFoundError(f"Internal content with ID {content_id} not found", "INTERNAL_CONTENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_internal_content_by_id", "read")
            raise
    
    async def get_content_by_department(self, department_id: UUID, limit: int = 100) -> List[InternalContent]:
        """Get content for a department."""
        try:
            return await self.repository.get_content_by_department(department_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_content_by_department", "read")
            return []

