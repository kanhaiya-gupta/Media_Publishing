"""
OwnLens - Base Domain: City Repository

Repository for cities table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.city import City, CityCreate, CityUpdate

logger = logging.getLogger(__name__)


class CityRepository(BaseRepository[City]):
    """Repository for cities table."""

    def __init__(self, connection_manager):
        """Initialize the city repository."""
        super().__init__(connection_manager, table_name="cities")
        self.cities_table = "cities"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "city_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "cities"

    # ============================================================================
    # CITY OPERATIONS
    # ============================================================================

    async def create_city(self, city_data: CityCreate) -> Optional[City]:
        """
        Create a new city record.
        
        Args:
            city_data: City creation data (dict or CityCreate model)
            
        Returns:
            Created city with generated ID, or None if creation failed
        """
        try:
            # Convert dict to CityCreate model if needed (this runs validators)
            if isinstance(city_data, dict):
                city_data = CityCreate.model_validate(city_data)
            
            # Store the generated city_id from the validator (in case result doesn't have it)
            generated_city_id = city_data.city_id
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(city_data, table_name=self.cities_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Ensure city_id is present in result (use generated one if missing)
                if "city_id" not in result or result.get("city_id") is None:
                    result["city_id"] = generated_city_id
                return City(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating city: {e}")
            return None

    async def get_city_by_id(self, city_id: UUID) -> Optional[City]:
        """Get city by ID."""
        try:
            result = await self.get_by_id(city_id)
            if result:
                result = self._convert_json_to_lists(result)
                return City(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting city by ID {city_id}: {e}")
            return None

    async def get_cities_by_country(self, country_code: str) -> List[City]:
        """Get all cities for a country."""
        try:
            results = await self.find_all_by_field("country_code", country_code)
            return [City(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting cities by country {country_code}: {e}")
            return []

    async def get_cities_by_name(self, city_name: str, country_code: Optional[str] = None) -> List[City]:
        """Get cities by name, optionally filtered by country."""
        try:
            filters = {"city_name": city_name}
            if country_code:
                filters["country_code"] = country_code
            results = await self.get_all(filters=filters)
            return [City(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting cities by name {city_name}: {e}")
            return []

    async def update_city(self, city_id: UUID, city_data: CityUpdate) -> Optional[City]:
        """Update city data."""
        try:
            data = city_data.model_dump(exclude_unset=True)
            
            result = await self.update(city_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return City(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating city {city_id}: {e}")
            return None

    async def delete_city(self, city_id: UUID) -> bool:
        """Delete city and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(city_id)
            
        except Exception as e:
            logger.error(f"Error deleting city {city_id}: {e}")
            return False
