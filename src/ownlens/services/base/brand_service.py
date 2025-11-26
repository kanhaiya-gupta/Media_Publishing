"""
OwnLens - Base Domain: Brand Service

Service for brand management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.base import BrandRepository
from ...models.base.brand import Brand, BrandCreate, BrandUpdate

logger = logging.getLogger(__name__)


class BrandService(BaseService[Brand, BrandCreate, BrandUpdate, Brand]):
    """Service for brand management."""
    
    def __init__(self, repository: BrandRepository, service_name: str = None):
        """Initialize the brand service."""
        super().__init__(repository, service_name or "BrandService")
        self.repository: BrandRepository = repository
    
    def get_model_class(self):
        """Get the Brand model class."""
        return Brand
    
    def get_create_model_class(self):
        """Get the BrandCreate model class."""
        return BrandCreate
    
    def get_update_model_class(self):
        """Get the BrandUpdate model class."""
        return BrandUpdate
    
    def get_in_db_model_class(self):
        """Get the Brand model class."""
        return Brand
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for brand operations."""
        try:
            if operation == "create":
                if hasattr(data, 'brand_code'):
                    brand_code = data.brand_code
                else:
                    brand_code = data.get('brand_code') if isinstance(data, dict) else None
                
                if brand_code:
                    existing = await self.repository.get_brand_by_code(brand_code)
                    if existing:
                        raise ValidationError(
                            f"Brand with code '{brand_code}' already exists",
                            "DUPLICATE_BRAND_CODE"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_brand(self, brand_data: BrandCreate) -> Brand:
        """Create a new brand."""
        try:
            await self.validate_input(brand_data, "create")
            await self.validate_business_rules(brand_data, "create")
            
            result = await self.repository.create_brand(brand_data)
            if not result:
                raise NotFoundError("Failed to create brand", "CREATE_FAILED")
            
            self.log_operation("create_brand", {"brand_id": str(result.brand_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_brand", "create")
            raise
    
    async def get_brand_by_id(self, brand_id: UUID) -> Brand:
        """Get brand by ID."""
        try:
            result = await self.repository.get_brand_by_id(brand_id)
            if not result:
                raise NotFoundError(f"Brand with ID {brand_id} not found", "BRAND_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_brand_by_id", "read")
            raise
    
    async def get_brands_by_company(self, company_id: UUID) -> List[Brand]:
        """Get all brands for a company."""
        try:
            return await self.repository.get_brands_by_company(company_id)
        except Exception as e:
            await self.handle_error(e, "get_brands_by_company", "read")
            return []
    
    async def update_brand(self, brand_id: UUID, brand_data: BrandUpdate) -> Brand:
        """Update brand."""
        try:
            await self.get_brand_by_id(brand_id)
            await self.validate_input(brand_data, "update")
            await self.validate_business_rules(brand_data, "update")
            
            result = await self.repository.update_brand(brand_id, brand_data)
            if not result:
                raise NotFoundError(f"Failed to update brand {brand_id}", "UPDATE_FAILED")
            
            self.log_operation("update_brand", {"brand_id": str(brand_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_brand", "update")
            raise
    
    async def delete_brand(self, brand_id: UUID) -> bool:
        """Delete brand."""
        try:
            await self.get_brand_by_id(brand_id)
            result = await self.repository.delete_brand(brand_id)
            if result:
                self.log_operation("delete_brand", {"brand_id": str(brand_id)})
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "delete_brand", "delete")
            raise

