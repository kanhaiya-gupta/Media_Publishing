"""
OwnLens - Editorial Domain: Media Usage Repository

Repository for editorial_media_usage table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.media_usage import MediaUsage, MediaUsageCreate, MediaUsageUpdate

logger = logging.getLogger(__name__)


class MediaUsageRepository(BaseRepository[MediaUsage]):
    """Repository for editorial_media_usage table."""

    def __init__(self, connection_manager):
        """Initialize the media usage repository."""
        super().__init__(connection_manager, table_name="editorial_media_usage")
        self.media_usage_table = "editorial_media_usage"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "usage_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_media_usage"

    # ============================================================================
    # MEDIA USAGE OPERATIONS
    # ============================================================================

    async def create_media_usage(self, usage_data: MediaUsageCreate) -> Optional[MediaUsage]:
        """
        Create a new media usage record.
        
        Args:
            usage_data: Media usage creation data (dict or Pydantic model)
            
        Returns:
            Created media usage with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(usage_data, table_name=self.media_usage_table)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating media usage: {e}")
            return None

    async def get_media_usage_by_id(self, usage_id: UUID) -> Optional[MediaUsage]:
        """Get media usage by ID."""
        try:
            result = await self.get_by_id(usage_id)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting media usage by ID {usage_id}: {e}")
            return None

    async def get_media_usage_by_media(self, media_id: UUID, limit: int = 100, offset: int = 0) -> List[MediaUsage]:
        """Get all usage records for a media asset."""
        try:
            results = await self.get_all(
                filters={"media_id": media_id},
                order_by="usage_date",
                limit=limit,
                offset=offset
            )
            return [MediaUsage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media usage by media {media_id}: {e}")
            return []

    async def get_media_usage_by_article(self, article_id: UUID) -> List[MediaUsage]:
        """Get all media usage for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="usage_date"
            )
            return [MediaUsage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting media usage by article {article_id}: {e}")
            return []

    async def update_media_usage(self, usage_id: UUID, usage_data: MediaUsageUpdate) -> Optional[MediaUsage]:
        """Update media usage data."""
        try:
            data = usage_data.model_dump(exclude_unset=True)
            
            result = await self.update(usage_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return MediaUsage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating media usage {usage_id}: {e}")
            return None

    async def delete_media_usage(self, usage_id: UUID) -> bool:
        """Delete media usage and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(usage_id)
            
        except Exception as e:
            logger.error(f"Error deleting media usage {usage_id}: {e}")
            return False

