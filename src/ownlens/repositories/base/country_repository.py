"""
OwnLens - Base Domain: Country Repository

Repository for countries table.
Note: Uses country_code (CHAR(2)) as primary key, not UUID.
"""

from typing import Dict, List, Optional, Any
import logging

from ..shared import BaseRepository
from ...models.base.country import Country, CountryCreate, CountryUpdate

logger = logging.getLogger(__name__)


class CountryRepository(BaseRepository[Country]):
    """Repository for countries table."""

    def __init__(self, connection_manager):
        """Initialize the country repository."""
        super().__init__(connection_manager, table_name="countries")
        self.countries_table = "countries"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "country_code"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "countries"

    # ============================================================================
    # COUNTRY OPERATIONS
    # ============================================================================

    async def create_country(self, country_data: CountryCreate) -> Optional[Country]:
        """
        Create a new country record.
        
        Args:
            country_data: Country creation data (dict or Pydantic model)
            
        Returns:
            Created country with generated code, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(country_data, table_name=self.countries_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Country(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating country: {e}")
            return None

    async def get_country_by_id(self, country_code: str) -> Optional[Country]:
        """Get country by code."""
        try:
            result = await self.get_by_id(country_code)
            if result:
                result = self._convert_json_to_lists(result)
                return Country(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting country by code {country_code}: {e}")
            return None

    async def get_country_by_code(self, country_code: str) -> Optional[Country]:
        """Get country by code (alias for get_country_by_id)."""
        return await self.get_country_by_id(country_code)

    async def get_active_countries(self) -> List[Country]:
        """Get all active countries."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="country_name")
            return [Country(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active countries: {e}")
            return []

    async def get_countries_by_continent(self, continent_code: str) -> List[Country]:
        """Get countries by continent code."""
        try:
            results = await self.find_all_by_field("continent_code", continent_code)
            return [Country(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting countries by continent {continent_code}: {e}")
            return []

    async def update_country(self, country_code: str, country_data: CountryUpdate) -> Optional[Country]:
        """Update country data."""
        try:
            data = country_data.model_dump(exclude_unset=True)
            
            result = await self.update(country_code, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Country(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating country {country_code}: {e}")
            return None

    async def delete_country(self, country_code: str) -> bool:
        """Delete country and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(country_code)
            
        except Exception as e:
            logger.error(f"Error deleting country {country_code}: {e}")
            return False
