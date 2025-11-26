"""
OwnLens - Editorial Domain: Content Version Repository

Repository for editorial_content_versions table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.content_version import ContentVersion, ContentVersionCreate, ContentVersionUpdate

logger = logging.getLogger(__name__)


class ContentVersionRepository(BaseRepository[ContentVersion]):
    """Repository for editorial_content_versions table."""

    def __init__(self, connection_manager):
        """Initialize the content version repository."""
        super().__init__(connection_manager, table_name="editorial_content_versions")
        self.content_versions_table = "editorial_content_versions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "version_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_content_versions"

    # ============================================================================
    # CONTENT VERSION OPERATIONS
    # ============================================================================

    async def create_content_version(self, version_data: ContentVersionCreate) -> Optional[ContentVersion]:
        """
        Create a new content version record.
        
        Args:
            version_data: Content version creation data (dict or Pydantic model)
            
        Returns:
            Created content version with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(version_data, table_name=self.content_versions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentVersion(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating content version: {e}")
            return None

    async def get_content_version_by_id(self, version_id: UUID) -> Optional[ContentVersion]:
        """Get content version by ID."""
        try:
            result = await self.get_by_id(version_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentVersion(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting content version by ID {version_id}: {e}")
            return None

    async def get_content_versions_by_article(self, article_id: UUID, limit: int = 100, offset: int = 0) -> List[ContentVersion]:
        """Get all versions for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="version_number",
                limit=limit,
                offset=offset
            )
            return [ContentVersion(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting content versions by article {article_id}: {e}")
            return []

    async def get_latest_content_version(self, article_id: UUID) -> Optional[ContentVersion]:
        """Get the latest version for an article."""
        try:
            results = await self.get_all(
                filters={"article_id": article_id},
                order_by="version_number",
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return ContentVersion(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest content version for article {article_id}: {e}")
            return None

    async def update_content_version(self, version_id: UUID, version_data: ContentVersionUpdate) -> Optional[ContentVersion]:
        """Update content version data."""
        try:
            data = version_data.model_dump(exclude_unset=True)
            
            result = await self.update(version_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ContentVersion(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating content version {version_id}: {e}")
            return None

    async def delete_content_version(self, version_id: UUID) -> bool:
        """Delete content version and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(version_id)
            
        except Exception as e:
            logger.error(f"Error deleting content version {version_id}: {e}")
            return False

