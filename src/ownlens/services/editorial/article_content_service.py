"""
OwnLens - Editorial Domain: Article Content Service

Service for article content management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import ArticleContentRepository
from ...models.editorial.article_content import ArticleContent, ArticleContentCreate, ArticleContentUpdate

logger = logging.getLogger(__name__)


class ArticleContentService(BaseService[ArticleContent, ArticleContentCreate, ArticleContentUpdate, ArticleContent]):
    """Service for article content management."""
    
    def __init__(self, repository: ArticleContentRepository, service_name: str = None):
        """Initialize the article content service."""
        super().__init__(repository, service_name or "ArticleContentService")
        self.repository: ArticleContentRepository = repository
    
    def get_model_class(self):
        """Get the ArticleContent model class."""
        return ArticleContent
    
    def get_create_model_class(self):
        """Get the ArticleContentCreate model class."""
        return ArticleContentCreate
    
    def get_update_model_class(self):
        """Get the ArticleContentUpdate model class."""
        return ArticleContentUpdate
    
    def get_in_db_model_class(self):
        """Get the ArticleContent model class."""
        return ArticleContent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for article content operations."""
        return True  # Content validation is typically handled at the model level
    
    async def create_article_content(self, content_data: ArticleContentCreate) -> ArticleContent:
        """Create new article content."""
        try:
            await self.validate_input(content_data, "create")
            await self.validate_business_rules(content_data, "create")
            
            result = await self.repository.create_article_content(content_data)
            if not result:
                raise NotFoundError("Failed to create article content", "CREATE_FAILED")
            
            self.log_operation("create_article_content", {"content_id": str(result.content_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_article_content", "create")
            raise
    
    async def get_article_content_by_article(self, article_id: UUID) -> Optional[ArticleContent]:
        """Get content for an article."""
        try:
            return await self.repository.get_content_by_article(article_id)
        except Exception as e:
            await self.handle_error(e, "get_article_content_by_article", "read")
            raise








