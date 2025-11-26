"""
OwnLens - Base Domain: Category Repository

Repository for categories table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ..shared.query_builder import select_all
from ...models.base.category import Category, CategoryCreate, CategoryUpdate

logger = logging.getLogger(__name__)


class CategoryRepository(BaseRepository[Category]):
    """Repository for categories table."""

    def __init__(self, connection_manager):
        """Initialize the category repository."""
        super().__init__(connection_manager, table_name="categories")
        self.categories_table = "categories"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "category_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "categories"

    # ============================================================================
    # CATEGORY OPERATIONS
    # ============================================================================

    async def create_category(self, category_data: CategoryCreate) -> Optional[Category]:
        """
        Create a new category record.
        
        Args:
            category_data: Category creation data (dict or CategoryCreate model)
            
        Returns:
            Created category with generated ID, or None if creation failed
        """
        try:
            # Convert dict to CategoryCreate model if needed (this runs validators)
            if isinstance(category_data, dict):
                category_data = CategoryCreate.model_validate(category_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(category_data, table_name=self.categories_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Category(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating category: {e}")
            return None

    async def get_category_by_id(self, category_id: UUID) -> Optional[Category]:
        """Get category by ID."""
        try:
            result = await self.get_by_id(category_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Category(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting category by ID {category_id}: {e}")
            return None

    async def get_category_by_code(self, category_code: str) -> Optional[Category]:
        """Get category by code."""
        try:
            result = await self.find_by_field("category_code", category_code)
            if result:
                result = self._convert_json_to_lists(result)
                return Category(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting category by code {category_code}: {e}")
            return None

    async def get_categories_by_parent(self, parent_category_id: UUID) -> List[Category]:
        """Get all subcategories for a parent category."""
        try:
            results = await self.find_all_by_field("parent_category_id", parent_category_id)
            return [Category(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting categories by parent {parent_category_id}: {e}")
            return []

    async def get_root_categories(self) -> List[Category]:
        """Get all root categories (categories without a parent)."""
        try:
            query, params = (
                select_all()
                .table(self.categories_table)
                .where_is_null("parent_category_id")
                .order_by("category_name")
                .build()
            )
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [Category(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting root categories: {e}")
            return []

    async def update_category(self, category_id: UUID, category_data: CategoryUpdate) -> Optional[Category]:
        """Update category data."""
        try:
            data = category_data.model_dump(exclude_unset=True)
            
            result = await self.update(category_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Category(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating category {category_id}: {e}")
            return None

    async def delete_category(self, category_id: UUID) -> bool:
        """Delete category and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(category_id)
            
        except Exception as e:
            logger.error(f"Error deleting category {category_id}: {e}")
            return False
