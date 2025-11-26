"""
OwnLens - Editorial Domain: Media Asset Service

Service for media asset management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import MediaAssetRepository
from ...models.editorial.media_asset import MediaAsset, MediaAssetCreate, MediaAssetUpdate

logger = logging.getLogger(__name__)


class MediaAssetService(BaseService[MediaAsset, MediaAssetCreate, MediaAssetUpdate, MediaAsset]):
    """Service for media asset management."""
    
    def __init__(self, repository: MediaAssetRepository, service_name: str = None):
        """Initialize the media asset service."""
        super().__init__(repository, service_name or "MediaAssetService")
        self.repository: MediaAssetRepository = repository
    
    def get_model_class(self):
        """Get the MediaAsset model class."""
        return MediaAsset
    
    def get_create_model_class(self):
        """Get the MediaAssetCreate model class."""
        return MediaAssetCreate
    
    def get_update_model_class(self):
        """Get the MediaAssetUpdate model class."""
        return MediaAssetUpdate
    
    def get_in_db_model_class(self):
        """Get the MediaAsset model class."""
        return MediaAsset
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for media asset operations."""
        try:
            if operation == "create":
                # Validate media code format
                if hasattr(data, 'media_code'):
                    media_code = data.media_code
                else:
                    media_code = data.get('media_code') if isinstance(data, dict) else None
                
                if media_code and not media_code.replace('_', '').replace('-', '').isalnum():
                    raise ValidationError(
                        "Media code must be alphanumeric with hyphens and underscores",
                        "INVALID_MEDIA_CODE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_media_asset(self, media_data: MediaAssetCreate) -> MediaAsset:
        """Create a new media asset."""
        try:
            await self.validate_input(media_data, "create")
            await self.validate_business_rules(media_data, "create")
            
            result = await self.repository.create_media_asset(media_data)
            if not result:
                raise NotFoundError("Failed to create media asset", "CREATE_FAILED")
            
            self.log_operation("create_media_asset", {"media_id": str(result.media_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_media_asset", "create")
            raise
    
    async def get_media_asset_by_id(self, media_id: UUID) -> MediaAsset:
        """Get media asset by ID."""
        try:
            result = await self.repository.get_media_asset_by_id(media_id)
            if not result:
                raise NotFoundError(f"Media asset with ID {media_id} not found", "MEDIA_ASSET_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_media_asset_by_id", "read")
            raise

