"""
OwnLens - Editorial Domain: Media Collection Item Repository

Repository for editorial_media_collection_items table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.media_collection_item import MediaCollectionItem, MediaCollectionItemCreate, MediaCollectionItemUpdate

logger = logging.getLogger(__name__)


class MediaCollectionItemRepository(BaseRepository[MediaCollectionItem]):
    """Repository for editorial_media_collection_items table."""

    def __init__(self, connection_manager):
        """Initialize the media collection item repository."""
        super().__init__(connection_manager, table_name="editorial_media_collection_items")
        self.media_collection_items_table = "editorial_media_collection_items"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "item_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_media_collection_items"

    # ============================================================================
    # MEDIA COLLECTION ITEM OPERATIONS
    # ============================================================================

    async def create_media_collection_item(self, item_data: MediaCollectionItemCreate) -> Optional[MediaCollectionItem]:
        """
        Create a new media collection item record.
        
        Args:
            item_data: Media collection item creation data (dict or Pydantic model)
            
        Returns:
            Created media collection item with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(item_data, table_name=self.media_collection_items_table)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollectionItem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating media collection item: {e}")
            return None

    async def get_media_collection_item_by_id(self, item_id: UUID) -> Optional[MediaCollectionItem]:
        """Get media collection item by ID."""
        try:
            result = await self.get_by_id(item_id)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollectionItem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media collection item by ID {item_id}: {e}")
            return None

    async def get_media_collection_items_by_collection(self, collection_id: UUID) -> List[MediaCollectionItem]:
        """Get all items in a media collection."""
        try:
            results = await self.get_all(
                filters={"collection_id": collection_id},
                order_by="display_order"
            )
            return [MediaCollectionItem(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media collection items by collection {collection_id}: {e}")
            return []

    async def get_media_collection_items_by_media(self, media_id: UUID) -> List[MediaCollectionItem]:
        """Get all collections containing a media asset."""
        try:
            results = await self.get_all(
                filters={"media_id": media_id},
                order_by="added_at"
            )
            return [MediaCollectionItem(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media collection items by media {media_id}: {e}")
            return []

    async def update_media_collection_item(self, item_id: UUID, item_data: MediaCollectionItemUpdate) -> Optional[MediaCollectionItem]:
        """Update media collection item data."""
        try:
            data = item_data.model_dump(exclude_unset=True)
            
            result = await self.update(item_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollectionItem(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating media collection item {item_id}: {e}")
            return None

    async def delete_media_collection_item(self, item_id: UUID) -> bool:
        """Delete media collection item and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(item_id)
            
        except Exception as e:
            logger.error(f"Error deleting media collection item {item_id}: {e}")
            return False

