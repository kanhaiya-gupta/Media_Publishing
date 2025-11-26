"""
OwnLens - Base Domain: Brand Repository

Repository for brands table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.brand import Brand, BrandCreate, BrandUpdate

logger = logging.getLogger(__name__)


class BrandRepository(BaseRepository[Brand]):
    """Repository for brands table."""

    def __init__(self, connection_manager):
        """Initialize the brand repository."""
        super().__init__(connection_manager, table_name="brands")
        self.brands_table = "brands"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "brand_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "brands"

    # ============================================================================
    # BRAND OPERATIONS
    # ============================================================================

    async def create_brand(self, brand_data: BrandCreate) -> Optional[Brand]:
        """
        Create a new brand record.
        
        Args:
            brand_data: Brand creation data (dict or BrandCreate model)
            
        Returns:
            Created brand with generated ID, or None if creation failed
        """
        try:
            # Convert dict to BrandCreate model if needed (this runs validators)
            if isinstance(brand_data, dict):
                brand_data = BrandCreate.model_validate(brand_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(brand_data, table_name=self.brands_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Brand(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating brand: {e}")
            return None

    async def get_brand_by_id(self, brand_id: UUID) -> Optional[Brand]:
        """Get brand by ID."""
        try:
            result = await self.get_by_id(brand_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Brand(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting brand by ID {brand_id}: {e}")
            return None

    async def get_brand_by_code(self, brand_code: str) -> Optional[Brand]:
        """Get brand by code."""
        try:
            result = await self.find_by_field("brand_code", brand_code)
            if result:
                result = self._convert_json_to_lists(result)
                return Brand(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting brand by code {brand_code}: {e}")
            return None

    async def get_brands_by_company(self, company_id: UUID) -> List[Brand]:
        """Get all brands for a company."""
        try:
            results = await self.find_all_by_field("company_id", company_id)
            return [Brand(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting brands by company {company_id}: {e}")
            return []

    async def get_active_brands(self, company_id: Optional[UUID] = None) -> List[Brand]:
        """Get all active brands, optionally filtered by company."""
        try:
            filters = {"is_active": True}
            if company_id:
                filters["company_id"] = company_id
            results = await self.get_all(filters=filters, order_by="brand_name")
            return [Brand(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active brands: {e}")
            return []

    async def get_brands_by_country(self, country_code: str) -> List[Brand]:
        """Get brands by primary country code."""
        try:
            results = await self.find_all_by_field("primary_country_code", country_code)
            return [Brand(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting brands by country {country_code}: {e}")
            return []

    async def update_brand(self, brand_id: UUID, brand_data: BrandUpdate) -> Optional[Brand]:
        """Update brand data."""
        try:
            data = brand_data.model_dump(exclude_unset=True)
            
            result = await self.update(brand_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Brand(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating brand {brand_id}: {e}")
            return None

    async def delete_brand(self, brand_id: UUID) -> bool:
        """Delete brand and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(brand_id)
            
        except Exception as e:
            logger.error(f"Error deleting brand {brand_id}: {e}")
            return False
