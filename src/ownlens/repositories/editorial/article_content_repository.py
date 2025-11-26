"""
OwnLens - Editorial Domain: Article Content Repository

Repository for editorial_article_content table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.article_content import ArticleContent, ArticleContentCreate, ArticleContentUpdate

logger = logging.getLogger(__name__)


class ArticleContentRepository(BaseRepository[ArticleContent]):
    """Repository for editorial_article_content table."""

    def __init__(self, connection_manager):
        """Initialize the article content repository."""
        super().__init__(connection_manager, table_name="editorial_article_content")
        self.article_content_table = "editorial_article_content"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "content_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_article_content"

    # ============================================================================
    # ARTICLE CONTENT OPERATIONS
    # ============================================================================

    async def create_article_content(self, content_data: ArticleContentCreate) -> Optional[ArticleContent]:
        """
        Create a new article content record.
        
        Args:
            content_data: Article content creation data (dict or Pydantic model)
            
        Returns:
            Created article content with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(content_data, table_name=self.article_content_table)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticleContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating article content: {e}")
            return None

    async def get_article_content_by_id(self, content_id: UUID) -> Optional[ArticleContent]:
        """Get article content by ID."""
        try:
            result = await self.get_by_id(content_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticleContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article content by ID {content_id}: {e}")
            return None

    async def get_article_content_by_article(self, article_id: UUID) -> Optional[ArticleContent]:
        """Get article content by article ID."""
        try:
            result = await self.find_by_field("article_id", article_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticleContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article content by article {article_id}: {e}")
            return None

    async def update_article_content(self, content_id: UUID, content_data: ArticleContentUpdate) -> Optional[ArticleContent]:
        """Update article content data."""
        try:
            data = content_data.model_dump(exclude_unset=True)
            
            result = await self.update(content_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ArticleContent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating article content {content_id}: {e}")
            return None

    async def delete_article_content(self, content_id: UUID) -> bool:
        """Delete article content and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(content_id)
            
        except Exception as e:
            logger.error(f"Error deleting article content {content_id}: {e}")
            return False

