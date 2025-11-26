"""
OwnLens - Editorial Domain: Media Collection Repository

Repository for editorial_media_collections table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.media_collection import MediaCollection, MediaCollectionCreate, MediaCollectionUpdate

logger = logging.getLogger(__name__)


class MediaCollectionRepository(BaseRepository[MediaCollection]):
    """Repository for editorial_media_collections table."""

    def __init__(self, connection_manager):
        """Initialize the media collection repository."""
        super().__init__(connection_manager, table_name="editorial_media_collections")
        self.media_collections_table = "editorial_media_collections"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "collection_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_media_collections"

    # ============================================================================
    # MEDIA COLLECTION OPERATIONS
    # ============================================================================

    async def create_media_collection(self, collection_data: MediaCollectionCreate) -> Optional[MediaCollection]:
        """
        Create a new media collection record.
        
        Args:
            collection_data: Media collection creation data (dict or Pydantic model)
            
        Returns:
            Created media collection with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(collection_data, table_name=self.media_collections_table)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollection(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating media collection: {e}")
            return None

    async def get_media_collection_by_id(self, collection_id: UUID) -> Optional[MediaCollection]:
        """Get media collection by ID."""
        try:
            result = await self.get_by_id(collection_id)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollection(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media collection by ID {collection_id}: {e}")
            return None

    async def get_media_collections_by_brand(self, brand_id: UUID, limit: int = 100, offset: int = 0) -> List[MediaCollection]:
        """Get all media collections for a brand."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id},
                order_by="collection_name",
                limit=limit,
                offset=offset
            )
            return [MediaCollection(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media collections by brand {brand_id}: {e}")
            return []

    async def update_media_collection(self, collection_id: UUID, collection_data: MediaCollectionUpdate) -> Optional[MediaCollection]:
        """Update media collection data."""
        try:
            data = collection_data.model_dump(exclude_unset=True)
            
            result = await self.update(collection_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaCollection(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating media collection {collection_id}: {e}")
            return None

    async def delete_media_collection(self, collection_id: UUID) -> bool:
        """Delete media collection and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(collection_id)
            
        except Exception as e:
            logger.error(f"Error deleting media collection {collection_id}: {e}")
            return False

