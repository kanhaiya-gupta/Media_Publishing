"""
OwnLens - Base Domain: Brand Country Repository

Repository for brand_countries table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.brand_country import BrandCountry, BrandCountryCreate, BrandCountryUpdate

logger = logging.getLogger(__name__)


class BrandCountryRepository(BaseRepository[BrandCountry]):
    """Repository for brand_countries table."""

    def __init__(self, connection_manager):
        """Initialize the brand country repository."""
        super().__init__(connection_manager, table_name="brand_countries")
        self.brand_countries_table = "brand_countries"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "brand_country_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "brand_countries"

    # ============================================================================
    # BRAND COUNTRY OPERATIONS
    # ============================================================================

    async def create_brand_country(self, brand_country_data: Union[BrandCountryCreate, Dict[str, Any]]) -> Optional[BrandCountry]:
        """
        Create a new brand country record.
        
        Args:
            brand_country_data: Brand country creation data (dict or BrandCountryCreate model)
            
        Returns:
            Created brand country with generated ID, or None if creation failed
        """
        try:
            # Convert dict to BrandCountryCreate model if needed (this runs validators)
            if isinstance(brand_country_data, dict):
                brand_country_data = BrandCountryCreate.model_validate(brand_country_data)
            else:
                # If it's already a model but ID is None, re-validate to trigger validator
                if hasattr(brand_country_data, 'brand_country_id') and brand_country_data.brand_country_id is None:
                    # Re-validate to trigger the ID generator validator
                    brand_country_data = BrandCountryCreate.model_validate(brand_country_data.model_dump())
            
            # Store the generated brand_country_id from the validator (in case result doesn't have it)
            generated_brand_country_id = brand_country_data.brand_country_id
            # If still None, generate it manually
            if generated_brand_country_id is None:
                from uuid import uuid4
                generated_brand_country_id = uuid4()
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(brand_country_data, table_name=self.brand_countries_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure brand_country_id is present in result (use generated one if missing)
                if "brand_country_id" not in result or result.get("brand_country_id") is None:
                    result["brand_country_id"] = generated_brand_country_id
                return BrandCountry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating brand country: {e}")
            return None

    async def get_brand_country_by_id(self, brand_country_id: UUID) -> Optional[BrandCountry]:
        """Get brand country by ID."""
        try:
            result = await self.get_by_id(brand_country_id)
            if result:
                result = self._convert_json_to_lists(result)
                return BrandCountry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting brand country by ID {brand_country_id}: {e}")
            return None

    async def get_brand_countries_by_brand(self, brand_id: UUID) -> List[BrandCountry]:
        """Get all countries for a brand."""
        try:
            results = await self.find_all_by_field("brand_id", brand_id)
            return [BrandCountry(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting brand countries by brand {brand_id}: {e}")
            return []

    async def get_brand_countries_by_country(self, country_code: str) -> List[BrandCountry]:
        """Get all brands for a country."""
        try:
            results = await self.find_all_by_field("country_code", country_code)
            return [BrandCountry(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting brand countries by country {country_code}: {e}")
            return []

    async def get_brand_country(self, brand_id: UUID, country_code: str) -> Optional[BrandCountry]:
        """Get a specific brand-country relationship."""
        try:
            results = await self.get_all(
                filters={"brand_id": brand_id, "country_code": country_code}
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return BrandCountry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting brand country for brand {brand_id} and country {country_code}: {e}")
            return None

    async def update_brand_country(self, brand_country_id: UUID, brand_country_data: BrandCountryUpdate) -> Optional[BrandCountry]:
        """Update brand country data."""
        try:
            data = brand_country_data.model_dump(exclude_unset=True)
            
            result = await self.update(brand_country_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return BrandCountry(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating brand country {brand_country_id}: {e}")
            return None

    async def delete_brand_country(self, brand_country_id: UUID) -> bool:
        """Delete brand country and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(brand_country_id)
            
        except Exception as e:
            logger.error(f"Error deleting brand country {brand_country_id}: {e}")
            return False
