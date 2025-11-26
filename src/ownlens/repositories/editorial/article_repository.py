"""
OwnLens - Editorial Domain: Article Repository

Repository for editorial_articles table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.editorial.article import Article, ArticleCreate, ArticleUpdate

logger = logging.getLogger(__name__)


class ArticleRepository(BaseRepository[Article]):
    """Repository for editorial_articles table."""

    def __init__(self, connection_manager):
        """Initialize the article repository."""
        super().__init__(connection_manager, table_name="editorial_articles")
        self.articles_table = "editorial_articles"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "article_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_articles"

    # ============================================================================
    # ARTICLE OPERATIONS
    # ============================================================================

    async def create_article(self, article_data: Union[ArticleCreate, Dict[str, Any]]) -> Optional[Article]:
        """
        Create a new article record.
        
        Args:
            article_data: Article creation data (Pydantic model or dict)
            
        Returns:
            Created article with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(article_data, table_name=self.articles_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Article(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating article: {e}")
            return None

    async def get_article_by_id(self, article_id: UUID) -> Optional[Article]:
        """Get article by ID."""
        try:
            result = await self.get_by_id(article_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Article(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article by ID {article_id}: {e}")
            return None

    async def get_article_by_external_id(self, external_id: str, brand_id: Optional[UUID] = None) -> Optional[Article]:
        """Get article by external ID."""
        try:
            filters = {"external_article_id": external_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return Article(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting article by external ID {external_id}: {e}")
            return None

    async def get_articles_by_author(self, author_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[Article]:
        """Get all articles by an author."""
        try:
            filters = {"author_id": author_id}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="publish_date",
                limit=limit,
                offset=offset
            )
            return [Article(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting articles by author {author_id}: {e}")
            return []

    async def get_articles_by_brand(self, brand_id: UUID, status: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[Article]:
        """Get all articles for a brand."""
        try:
            filters = {"brand_id": brand_id}
            if status:
                filters["status"] = status
            
            results = await self.get_all(
                filters=filters,
                order_by="publish_date",
                limit=limit,
                offset=offset
            )
            return [Article(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting articles by brand {brand_id}: {e}")
            return []

    async def get_published_articles(self, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[Article]:
        """Get all published articles."""
        try:
            filters = {"status": "published"}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(
                filters=filters,
                order_by="publish_date",
                limit=limit,
                offset=offset
            )
            return [Article(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting published articles: {e}")
            return []

    async def get_articles_by_category(self, category_id: UUID, brand_id: Optional[UUID] = None, limit: int = 100, offset: int = 0) -> List[Article]:
        """Get all articles in a category."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.articles_table)
                .where_contains("category_ids", category_id)
            )
            
            if brand_id:
                query_builder = query_builder.where_equals("brand_id", brand_id)
            
            query, params = (
                query_builder
                .order_by("publish_date")
                .limit(limit)
                .offset(offset)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [Article(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting articles by category {category_id}: {e}")
            return []

    async def update_article(self, article_id: UUID, article_data: ArticleUpdate) -> Optional[Article]:
        """Update article data."""
        try:
            data = article_data.model_dump(exclude_unset=True)
            
            result = await self.update(article_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Article(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating article {article_id}: {e}")
            return None

    async def delete_article(self, article_id: UUID) -> bool:
        """Delete article and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(article_id)
            
        except Exception as e:
            logger.error(f"Error deleting article {article_id}: {e}")
            return False

