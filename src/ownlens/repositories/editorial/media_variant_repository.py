"""
OwnLens - Editorial Domain: Media Variant Repository

Repository for editorial_media_variants table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.media_variant import MediaVariant, MediaVariantCreate, MediaVariantUpdate

logger = logging.getLogger(__name__)


class MediaVariantRepository(BaseRepository[MediaVariant]):
    """Repository for editorial_media_variants table."""

    def __init__(self, connection_manager):
        """Initialize the media variant repository."""
        super().__init__(connection_manager, table_name="editorial_media_variants")
        self.media_variants_table = "editorial_media_variants"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "variant_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_media_variants"

    # ============================================================================
    # MEDIA VARIANT OPERATIONS
    # ============================================================================

    async def create_media_variant(self, variant_data: MediaVariantCreate) -> Optional[MediaVariant]:
        """
        Create a new media variant record.
        
        Args:
            variant_data: Media variant creation data (dict or Pydantic model)
            
        Returns:
            Created media variant with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(variant_data, table_name=self.media_variants_table)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaVariant(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating media variant: {e}")
            return None

    async def get_media_variant_by_id(self, variant_id: UUID) -> Optional[MediaVariant]:
        """Get media variant by ID."""
        try:
            result = await self.get_by_id(variant_id)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaVariant(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media variant by ID {variant_id}: {e}")
            return None

    async def get_media_variants_by_media(self, media_id: UUID) -> List[MediaVariant]:
        """Get all variants for a media asset."""
        try:
            results = await self.get_all(
                filters={"media_id": media_id},
                order_by="variant_size"
            )
            return [MediaVariant(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media variants by media {media_id}: {e}")
            return []

    async def get_media_variant_by_size(self, media_id: UUID, variant_size: str) -> Optional[MediaVariant]:
        """Get a specific variant by size."""
        try:
            results = await self.get_all(
                filters={"media_id": media_id, "variant_size": variant_size},
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return MediaVariant(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media variant by size {variant_size} for media {media_id}: {e}")
            return None

    async def update_media_variant(self, variant_id: UUID, variant_data: MediaVariantUpdate) -> Optional[MediaVariant]:
        """Update media variant data."""
        try:
            data = variant_data.model_dump(exclude_unset=True)
            
            result = await self.update(variant_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaVariant(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating media variant {variant_id}: {e}")
            return None

    async def delete_media_variant(self, variant_id: UUID) -> bool:
        """Delete media variant and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(variant_id)
            
        except Exception as e:
            logger.error(f"Error deleting media variant {variant_id}: {e}")
            return False

