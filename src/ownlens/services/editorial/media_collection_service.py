"""
OwnLens - Editorial Domain: Media Collection Service

Service for media collection management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import MediaCollectionRepository
from ...models.editorial.media_collection import MediaCollection, MediaCollectionCreate, MediaCollectionUpdate

logger = logging.getLogger(__name__)


class MediaCollectionService(BaseService[MediaCollection, MediaCollectionCreate, MediaCollectionUpdate, MediaCollection]):
    """Service for media collection management."""
    
    def __init__(self, repository: MediaCollectionRepository, service_name: str = None):
        """Initialize the media collection service."""
        super().__init__(repository, service_name or "MediaCollectionService")
        self.repository: MediaCollectionRepository = repository
    
    def get_model_class(self):
        """Get the MediaCollection model class."""
        return MediaCollection
    
    def get_create_model_class(self):
        """Get the MediaCollectionCreate model class."""
        return MediaCollectionCreate
    
    def get_update_model_class(self):
        """Get the MediaCollectionUpdate model class."""
        return MediaCollectionUpdate
    
    def get_in_db_model_class(self):
        """Get the MediaCollection model class."""
        return MediaCollection
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for media collection operations."""
        try:
            if operation == "create":
                # Validate collection name
                if hasattr(data, 'collection_name'):
                    name = data.collection_name
                else:
                    name = data.get('collection_name') if isinstance(data, dict) else None
                
                if not name or len(name.strip()) == 0:
                    raise ValidationError(
                        "Collection name is required",
                        "INVALID_NAME"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_media_collection(self, collection_data: MediaCollectionCreate) -> MediaCollection:
        """Create a new media collection."""
        try:
            await self.validate_input(collection_data, "create")
            await self.validate_business_rules(collection_data, "create")
            
            result = await self.repository.create_media_collection(collection_data)
            if not result:
                raise NotFoundError("Failed to create media collection", "CREATE_FAILED")
            
            self.log_operation("create_media_collection", {"collection_id": str(result.collection_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_media_collection", "create")
            raise
    
    async def get_media_collection_by_id(self, collection_id: UUID) -> MediaCollection:
        """Get media collection by ID."""
        try:
            result = await self.repository.get_media_collection_by_id(collection_id)
            if not result:
                raise NotFoundError(f"Media collection with ID {collection_id} not found", "MEDIA_COLLECTION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_media_collection_by_id", "read")
            raise
    
    async def get_media_collections_by_brand(self, brand_id: UUID, limit: int = 100) -> List[MediaCollection]:
        """Get media collections for a brand."""
        try:
            return await self.repository.get_media_collections_by_brand(brand_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_media_collections_by_brand", "read")
            return []








