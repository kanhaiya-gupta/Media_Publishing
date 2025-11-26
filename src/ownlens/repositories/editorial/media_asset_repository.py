"""
OwnLens - Editorial Domain: Media Asset Repository

Repository for editorial_media_assets table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.media_asset import MediaAsset, MediaAssetCreate, MediaAssetUpdate

logger = logging.getLogger(__name__)


class MediaAssetRepository(BaseRepository[MediaAsset]):
    """Repository for editorial_media_assets table."""

    def __init__(self, connection_manager):
        """Initialize the media asset repository."""
        super().__init__(connection_manager, table_name="editorial_media_assets")
        self.media_assets_table = "editorial_media_assets"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "media_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_media_assets"

    # ============================================================================
    # MEDIA ASSET OPERATIONS
    # ============================================================================

    async def create_media_asset(self, asset_data: MediaAssetCreate) -> Optional[MediaAsset]:
        """
        Create a new media asset record.
        
        Args:
            asset_data: Media asset creation data (dict or Pydantic model)
            
        Returns:
            Created media asset with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(asset_data, table_name=self.media_assets_table)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaAsset(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating media asset: {e}")
            return None

    async def get_media_asset_by_id(self, media_id: UUID) -> Optional[MediaAsset]:
        """Get media asset by ID."""
        try:
            result = await self.get_by_id(media_id)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaAsset(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media asset by ID {media_id}: {e}")
            return None

    async def get_media_asset_by_code(self, media_code: str, brand_id: Optional[UUID] = None) -> Optional[MediaAsset]:
        """Get media asset by code."""
        try:
            filters = {"media_code": media_code}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return MediaAsset(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media asset by code {media_code}: {e}")
            return None

    async def get_media_assets_by_brand(self, brand_id: UUID, media_type: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[MediaAsset]:
        """Get all media assets for a brand."""
        try:
            filters = {"brand_id": brand_id}
            if media_type:
                filters["media_type"] = media_type
            
            results = await self.get_all(
                filters=filters,
                order_by="uploaded_at",
                limit=limit,
                offset=offset
            )
            return [MediaAsset(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media assets by brand {brand_id}: {e}")
            return []

    async def get_media_assets_by_type(self, media_type: str, brand_id: Optional[UUID] = None, limit: int = 100) -> List[MediaAsset]:
        """Get media assets by type."""
        try:
            filters = {"media_type": media_type}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="uploaded_at",
                limit=limit
            )
            return [MediaAsset(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media assets by type {media_type}: {e}")
            return []

    async def get_featured_media_assets(self, brand_id: Optional[UUID] = None, limit: int = 100) -> List[MediaAsset]:
        """Get featured media assets."""
        try:
            filters = {"is_featured": True}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="uploaded_at",
                limit=limit
            )
            return [MediaAsset(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting featured media assets: {e}")
            return []

    async def update_media_asset(self, media_id: UUID, asset_data: MediaAssetUpdate) -> Optional[MediaAsset]:
        """Update media asset data."""
        try:
            data = asset_data.model_dump(exclude_unset=True)
            
            result = await self.update(media_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaAsset(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating media asset {media_id}: {e}")
            return None

    async def delete_media_asset(self, media_id: UUID) -> bool:
        """Delete media asset and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(media_id)
            
        except Exception as e:
            logger.error(f"Error deleting media asset {media_id}: {e}")
            return False

