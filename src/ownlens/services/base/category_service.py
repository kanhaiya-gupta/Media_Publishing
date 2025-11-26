"""
OwnLens - Base Domain: Category Service

Service for category management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.base import CategoryRepository
from ...models.base.category import Category, CategoryCreate, CategoryUpdate

logger = logging.getLogger(__name__)


class CategoryService(BaseService[Category, CategoryCreate, CategoryUpdate, Category]):
    """Service for category management."""
    
    def __init__(self, repository: CategoryRepository, service_name: str = None):
        """Initialize the category service."""
        super().__init__(repository, service_name or "CategoryService")
        self.repository: CategoryRepository = repository
    
    def get_model_class(self):
        """Get the Category model class."""
        return Category
    
    def get_create_model_class(self):
        """Get the CategoryCreate model class."""
        return CategoryCreate
    
    def get_update_model_class(self):
        """Get the CategoryUpdate model class."""
        return CategoryUpdate
    
    def get_in_db_model_class(self):
        """Get the Category model class."""
        return Category
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for category operations."""
        try:
            if operation == "create":
                # Check for duplicate category code within brand
                if hasattr(data, 'category_code') and hasattr(data, 'brand_id'):
                    category_code = data.category_code
                    brand_id = data.brand_id
                else:
                    category_code = data.get('category_code') if isinstance(data, dict) else None
                    brand_id = data.get('brand_id') if isinstance(data, dict) else None
                
                if category_code and brand_id:
                    existing = await self.repository.get_category_by_code(brand_id, category_code)
                    if existing:
                        raise ValidationError(
                            f"Category with code '{category_code}' already exists for this brand",
                            "DUPLICATE_CATEGORY_CODE"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_category(self, category_data: CategoryCreate) -> Category:
        """Create a new category."""
        try:
            await self.validate_input(category_data, "create")
            await self.validate_business_rules(category_data, "create")
            
            result = await self.repository.create_category(category_data)
            if not result:
                raise NotFoundError("Failed to create category", "CREATE_FAILED")
            
            self.log_operation("create_category", {"category_id": str(result.category_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_category", "create")
            raise
    
    async def get_category_by_id(self, category_id: UUID) -> Category:
        """Get category by ID."""
        try:
            result = await self.repository.get_category_by_id(category_id)
            if not result:
                raise NotFoundError(f"Category with ID {category_id} not found", "CATEGORY_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_category_by_id", "read")
            raise
    
    async def get_categories_by_brand(self, brand_id: UUID) -> List[Category]:
        """Get all categories for a brand."""
        try:
            return await self.repository.get_categories_by_brand(brand_id)
        except Exception as e:
            await self.handle_error(e, "get_categories_by_brand", "read")
            return []
    
    async def update_category(self, category_id: UUID, category_data: CategoryUpdate) -> Category:
        """Update category."""
        try:
            await self.get_category_by_id(category_id)
            await self.validate_input(category_data, "update")
            await self.validate_business_rules(category_data, "update")
            
            result = await self.repository.update_category(category_id, category_data)
            if not result:
                raise NotFoundError(f"Failed to update category {category_id}", "UPDATE_FAILED")
            
            self.log_operation("update_category", {"category_id": str(category_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_category", "update")
            raise
    
    async def delete_category(self, category_id: UUID) -> bool:
        """Delete category."""
        try:
            await self.get_category_by_id(category_id)
            result = await self.repository.delete_category(category_id)
            if result:
                self.log_operation("delete_category", {"category_id": str(category_id)})
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "delete_category", "delete")
            raise

