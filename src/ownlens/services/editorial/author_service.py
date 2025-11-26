"""
OwnLens - Editorial Domain: Author Service

Service for author management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import AuthorRepository
from ...models.editorial.author import Author, AuthorCreate, AuthorUpdate

logger = logging.getLogger(__name__)


class AuthorService(BaseService[Author, AuthorCreate, AuthorUpdate, Author]):
    """Service for author management."""
    
    def __init__(self, repository: AuthorRepository, service_name: str = None):
        """Initialize the author service."""
        super().__init__(repository, service_name or "AuthorService")
        self.repository: AuthorRepository = repository
    
    def get_model_class(self):
        """Get the Author model class."""
        return Author
    
    def get_create_model_class(self):
        """Get the AuthorCreate model class."""
        return AuthorCreate
    
    def get_update_model_class(self):
        """Get the AuthorUpdate model class."""
        return AuthorUpdate
    
    def get_in_db_model_class(self):
        """Get the Author model class."""
        return Author
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for author operations."""
        return True  # Authors typically don't have complex business rules
    
    async def create_author(self, author_data: AuthorCreate) -> Author:
        """Create a new author."""
        try:
            await self.validate_input(author_data, "create")
            await self.validate_business_rules(author_data, "create")
            
            result = await self.repository.create_author(author_data)
            if not result:
                raise NotFoundError("Failed to create author", "CREATE_FAILED")
            
            self.log_operation("create_author", {"author_id": str(result.author_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_author", "create")
            raise
    
    async def get_author_by_id(self, author_id: UUID) -> Author:
        """Get author by ID."""
        try:
            result = await self.repository.get_author_by_id(author_id)
            if not result:
                raise NotFoundError(f"Author with ID {author_id} not found", "AUTHOR_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_author_by_id", "read")
            raise
    
    async def get_authors_by_brand(self, brand_id: UUID) -> List[Author]:
        """Get authors by brand."""
        try:
            return await self.repository.get_authors_by_brand(brand_id)
        except Exception as e:
            await self.handle_error(e, "get_authors_by_brand", "read")
            return []

