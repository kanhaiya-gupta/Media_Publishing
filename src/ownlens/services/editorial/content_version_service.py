"""
OwnLens - Editorial Domain: Content Version Service

Service for content version management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import ContentVersionRepository
from ...models.editorial.content_version import ContentVersion, ContentVersionCreate, ContentVersionUpdate

logger = logging.getLogger(__name__)


class ContentVersionService(BaseService[ContentVersion, ContentVersionCreate, ContentVersionUpdate, ContentVersion]):
    """Service for content version management."""
    
    def __init__(self, repository: ContentVersionRepository, service_name: str = None):
        """Initialize the content version service."""
        super().__init__(repository, service_name or "ContentVersionService")
        self.repository: ContentVersionRepository = repository
    
    def get_model_class(self):
        """Get the ContentVersion model class."""
        return ContentVersion
    
    def get_create_model_class(self):
        """Get the ContentVersionCreate model class."""
        return ContentVersionCreate
    
    def get_update_model_class(self):
        """Get the ContentVersionUpdate model class."""
        return ContentVersionUpdate
    
    def get_in_db_model_class(self):
        """Get the ContentVersion model class."""
        return ContentVersion
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for content version operations."""
        try:
            if operation == "create":
                # Validate version number
                if hasattr(data, 'version_number'):
                    version = data.version_number
                else:
                    version = data.get('version_number') if isinstance(data, dict) else None
                
                if version is not None and version < 1:
                    raise ValidationError(
                        "Version number must be at least 1",
                        "INVALID_VERSION"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_content_version(self, version_data: ContentVersionCreate) -> ContentVersion:
        """Create a new content version."""
        try:
            await self.validate_input(version_data, "create")
            await self.validate_business_rules(version_data, "create")
            
            result = await self.repository.create_content_version(version_data)
            if not result:
                raise NotFoundError("Failed to create content version", "CREATE_FAILED")
            
            self.log_operation("create_content_version", {"version_id": str(result.version_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_content_version", "create")
            raise
    
    async def get_content_version_by_id(self, version_id: UUID) -> ContentVersion:
        """Get content version by ID."""
        try:
            result = await self.repository.get_content_version_by_id(version_id)
            if not result:
                raise NotFoundError(f"Content version with ID {version_id} not found", "CONTENT_VERSION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_content_version_by_id", "read")
            raise
    
    async def get_content_versions_by_article(self, article_id: UUID, limit: int = 100) -> List[ContentVersion]:
        """Get content versions for an article."""
        try:
            return await self.repository.get_content_versions_by_article(article_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_content_versions_by_article", "read")
            return []

