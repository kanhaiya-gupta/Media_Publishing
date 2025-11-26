"""
OwnLens - Editorial Domain: Media Variant Service

Service for media variant management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import MediaVariantRepository
from ...models.editorial.media_variant import MediaVariant, MediaVariantCreate, MediaVariantUpdate

logger = logging.getLogger(__name__)


class MediaVariantService(BaseService[MediaVariant, MediaVariantCreate, MediaVariantUpdate, MediaVariant]):
    """Service for media variant management."""
    
    def __init__(self, repository: MediaVariantRepository, service_name: str = None):
        """Initialize the media variant service."""
        super().__init__(repository, service_name or "MediaVariantService")
        self.repository: MediaVariantRepository = repository
    
    def get_model_class(self):
        """Get the MediaVariant model class."""
        return MediaVariant
    
    def get_create_model_class(self):
        """Get the MediaVariantCreate model class."""
        return MediaVariantCreate
    
    def get_update_model_class(self):
        """Get the MediaVariantUpdate model class."""
        return MediaVariantUpdate
    
    def get_in_db_model_class(self):
        """Get the MediaVariant model class."""
        return MediaVariant
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for media variant operations."""
        try:
            if operation == "create":
                # Validate storage path
                if hasattr(data, 'storage_path'):
                    path = data.storage_path
                else:
                    path = data.get('storage_path') if isinstance(data, dict) else None
                
                if not path or len(path.strip()) == 0:
                    raise ValidationError(
                        "Storage path is required",
                        "INVALID_PATH"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_media_variant(self, variant_data: MediaVariantCreate) -> MediaVariant:
        """Create a new media variant."""
        try:
            await self.validate_input(variant_data, "create")
            await self.validate_business_rules(variant_data, "create")
            
            result = await self.repository.create_media_variant(variant_data)
            if not result:
                raise NotFoundError("Failed to create media variant", "CREATE_FAILED")
            
            self.log_operation("create_media_variant", {"variant_id": str(result.variant_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_media_variant", "create")
            raise
    
    async def get_media_variant_by_id(self, variant_id: UUID) -> MediaVariant:
        """Get media variant by ID."""
        try:
            result = await self.repository.get_media_variant_by_id(variant_id)
            if not result:
                raise NotFoundError(f"Media variant with ID {variant_id} not found", "MEDIA_VARIANT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_media_variant_by_id", "read")
            raise
    
    async def get_media_variants_by_media(self, media_id: UUID, limit: int = 100) -> List[MediaVariant]:
        """Get media variants for a media asset."""
        try:
            return await self.repository.get_media_variants_by_media(media_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_media_variants_by_media", "read")
            return []

