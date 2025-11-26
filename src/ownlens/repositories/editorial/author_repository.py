"""
OwnLens - Editorial Domain: Author Repository

Repository for editorial_authors table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.editorial.author import Author, AuthorCreate, AuthorUpdate

logger = logging.getLogger(__name__)


class AuthorRepository(BaseRepository[Author]):
    """Repository for editorial_authors table."""

    def __init__(self, connection_manager):
        """Initialize the author repository."""
        super().__init__(connection_manager, table_name="editorial_authors")
        self.authors_table = "editorial_authors"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "author_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "editorial_authors"

    # ============================================================================
    # AUTHOR OPERATIONS
    # ============================================================================

    async def create_author(self, author_data: AuthorCreate) -> Optional[Author]:
        """
        Create a new author record.
        
        Args:
            author_data: Author creation data (dict or Pydantic model)
            
        Returns:
            Created author with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(author_data, table_name=self.authors_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Author(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating author: {e}")
            return None

    async def get_author_by_id(self, author_id: UUID) -> Optional[Author]:
        """Get author by ID."""
        try:
            result = await self.get_by_id(author_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Author(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting author by ID {author_id}: {e}")
            return None

    async def get_author_by_email(self, email: str, brand_id: Optional[UUID] = None) -> Optional[Author]:
        """Get author by email."""
        try:
            filters = {"email": email}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return Author(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting author by email {email}: {e}")
            return None

    async def get_authors_by_brand(self, brand_id: UUID, active_only: bool = True) -> List[Author]:
        """Get all authors for a brand."""
        try:
            filters = {"brand_id": brand_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(filters=filters, order_by="author_name")
            return [Author(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting authors by brand {brand_id}: {e}")
            return []

    async def get_active_authors(self, brand_id: Optional[UUID] = None) -> List[Author]:
        """Get all active authors."""
        try:
            filters = {"is_active": True}
            if brand_id:
                filters["brand_id"] = brand_id
            
            results = await self.get_all(filters=filters, order_by="author_name")
            return [Author(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active authors: {e}")
            return []

    async def update_author(self, author_id: UUID, author_data: AuthorUpdate) -> Optional[Author]:
        """Update author data."""
        try:
            data = author_data.model_dump(exclude_unset=True)
            
            result = await self.update(author_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Author(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating author {author_id}: {e}")
            return None

    async def delete_author(self, author_id: UUID) -> bool:
        """Delete author and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(author_id)
            
        except Exception as e:
            logger.error(f"Error deleting author {author_id}: {e}")
            return False

