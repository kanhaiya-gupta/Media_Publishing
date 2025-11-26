"""
OwnLens - Editorial Domain: Content Media Repository

Repository for editorial_content_media table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.content_media import ContentMedia, ContentMediaCreate, ContentMediaUpdate

logger = logging.getLogger(__name__)


class ContentMediaRepository(BaseRepository[ContentMedia]):
    """Repository for editorial_content_media table."""

    def __init__(self, connection_manager):
        """Initialize the content media repository."""
        super().__init__(connection_manager, table_name="editorial_content_media")
        self.content_media_table = "editorial_content_media"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "content_media_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_content_media"

    # ============================================================================
    # CONTENT MEDIA OPERATIONS
    # ============================================================================

    async def create_content_media(self, content_media_data: Union[ContentMediaCreate, Dict[str, Any]]) -> Optional[ContentMedia]:
        """
        Create a new content media record.
        
        Args:
            content_media_data: Content media creation data (dict or Pydantic model)
            
        Returns:
            Created content media with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(content_media_data, table_name=self.content_media_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Map content_media_id to relationship_id if present (table uses content_media_id as PK, model uses relationship_id)
                # BaseRepository may add content_media_id to the result, but ContentMedia model only has relationship_id
                if 'content_media_id' in result:
                    if 'relationship_id' not in result:
                        # Map content_media_id to relationship_id
                        result['relationship_id'] = result['content_media_id']
                    # Remove content_media_id as model doesn't have it (only has relationship_id)
                    result.pop('content_media_id', None)
                return ContentMedia(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating content media: {e}")
            return None

    async def get_content_media_by_id(self, content_media_id: UUID) -> Optional[ContentMedia]:
        """Get content media by ID."""
        try:
            result = await self.get_by_id(content_media_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentMedia(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content media by ID {content_media_id}: {e}")
            return None

    async def get_content_media_by_article(self, article_id: UUID) -> List[ContentMedia]:
        """Get all media for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="position_in_section"
            )
            return [ContentMedia(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content media by article {article_id}: {e}")
            return []

    async def get_content_media_by_media(self, media_id: UUID) -> List[ContentMedia]:
        """Get all content using a specific media asset."""
        try:
            results = await self.get_all(
                filters={"media_id": media_id},
                order_by="linked_at"
            )
            return [ContentMedia(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content media by media {media_id}: {e}")
            return []

    async def update_content_media(self, content_media_id: UUID, content_media_data: ContentMediaUpdate) -> Optional[ContentMedia]:
        """Update content media data."""
        try:
            data = content_media_data.model_dump(exclude_unset=True)
            
            result = await self.update(content_media_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentMedia(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating content media {content_media_id}: {e}")
            return None

    async def delete_content_media(self, content_media_id: UUID) -> bool:
        """Delete content media and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(content_media_id)
            
        except Exception as e:
            logger.error(f"Error deleting content media {content_media_id}: {e}")
            return False

