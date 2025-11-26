"""
OwnLens - Editorial Domain: Article Service

Service for article management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import ArticleRepository
from ...models.editorial.article import Article, ArticleCreate, ArticleUpdate

logger = logging.getLogger(__name__)


class ArticleService(BaseService[Article, ArticleCreate, ArticleUpdate, Article]):
    """Service for article management."""
    
    def __init__(self, repository: ArticleRepository, service_name: str = None):
        """Initialize the article service."""
        super().__init__(repository, service_name or "ArticleService")
        self.repository: ArticleRepository = repository
    
    def get_model_class(self):
        """Get the Article model class."""
        return Article
    
    def get_create_model_class(self):
        """Get the ArticleCreate model class."""
        return ArticleCreate
    
    def get_update_model_class(self):
        """Get the ArticleUpdate model class."""
        return ArticleUpdate
    
    def get_in_db_model_class(self):
        """Get the Article model class."""
        return Article
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for article operations."""
        try:
            if operation == "create":
                # Check for duplicate article code within brand
                if hasattr(data, 'article_code') and hasattr(data, 'brand_id'):
                    article_code = data.article_code
                    brand_id = data.brand_id
                else:
                    article_code = data.get('article_code') if isinstance(data, dict) else None
                    brand_id = data.get('brand_id') if isinstance(data, dict) else None
                
                if article_code and brand_id:
                    existing = await self.repository.get_article_by_code(article_code, brand_id)
                    if existing:
                        raise ValidationError(
                            f"Article with code '{article_code}' already exists for this brand",
                            "DUPLICATE_ARTICLE_CODE"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_article(self, article_data: ArticleCreate) -> Article:
        """Create a new article."""
        try:
            await self.validate_input(article_data, "create")
            await self.validate_business_rules(article_data, "create")
            
            result = await self.repository.create_article(article_data)
            if not result:
                raise NotFoundError("Failed to create article", "CREATE_FAILED")
            
            self.log_operation("create_article", {"article_id": str(result.article_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_article", "create")
            raise
    
    async def get_article_by_id(self, article_id: UUID) -> Article:
        """Get article by ID."""
        try:
            result = await self.repository.get_article_by_id(article_id)
            if not result:
                raise NotFoundError(f"Article with ID {article_id} not found", "ARTICLE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_article_by_id", "read")
            raise
    
    async def get_articles_by_author(self, author_id: UUID, limit: int = 100) -> List[Article]:
        """Get articles by author."""
        try:
            return await self.repository.get_articles_by_author(author_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_articles_by_author", "read")
            return []
    
    async def get_articles_by_brand(self, brand_id: UUID, limit: int = 100) -> List[Article]:
        """Get articles by brand."""
        try:
            return await self.repository.get_articles_by_brand(brand_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_articles_by_brand", "read")
            return []
    
    async def update_article(self, article_id: UUID, article_data: ArticleUpdate) -> Article:
        """Update article."""
        try:
            await self.get_article_by_id(article_id)
            await self.validate_input(article_data, "update")
            await self.validate_business_rules(article_data, "update")
            
            result = await self.repository.update_article(article_id, article_data)
            if not result:
                raise NotFoundError(f"Failed to update article {article_id}", "UPDATE_FAILED")
            
            self.log_operation("update_article", {"article_id": str(article_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_article", "update")
            raise

