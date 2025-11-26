"""
OwnLens - Base Domain: Company Repository

Repository for companies table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.base.company import Company, CompanyCreate, CompanyUpdate

logger = logging.getLogger(__name__)


class CompanyRepository(BaseRepository[Company]):
    """Repository for companies table."""

    def __init__(self, connection_manager):
        """Initialize the company repository."""
        super().__init__(connection_manager, table_name="companies")
        self.companies_table = "companies"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "company_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "companies"

    # ============================================================================
    # COMPANY OPERATIONS
    # ============================================================================

    async def create_company(self, company_data: CompanyCreate) -> Optional[Company]:
        """
        Create a new company record.
        
        Args:
            company_data: Company creation data (dict or CompanyCreate model)
            
        Returns:
            Created company with generated ID, or None if creation failed
        """
        try:
            # Convert dict to CompanyCreate model if needed (this runs validators)
            if isinstance(company_data, dict):
                company_data = CompanyCreate.model_validate(company_data)
            
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(company_data, table_name=self.companies_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Company(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return None

    async def get_company_by_id(self, company_id: UUID) -> Optional[Company]:
        """Get company by ID."""
        try:
            result = await self.get_by_id(company_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Company(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting company by ID {company_id}: {e}")
            return None

    async def get_company_by_code(self, company_code: str) -> Optional[Company]:
        """Get company by code."""
        try:
            result = await self.find_by_field("company_code", company_code)
            if result:
                result = self._convert_json_to_lists(result)
                return Company(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting company by code {company_code}: {e}")
            return None

    async def get_active_companies(self) -> List[Company]:
        """Get all active companies."""
        try:
            results = await self.get_all(filters={"is_active": True}, order_by="company_name")
            return [Company(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active companies: {e}")
            return []

    async def get_companies_by_country(self, country_code: str) -> List[Company]:
        """Get companies by headquarters country code."""
        try:
            results = await self.find_all_by_field("headquarters_country_code", country_code)
            return [Company(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting companies by country {country_code}: {e}")
            return []

    async def update_company(self, company_id: UUID, company_data: CompanyUpdate) -> Optional[Company]:
        """Update company data."""
        try:
            data = company_data.model_dump(exclude_unset=True)
            
            result = await self.update(company_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Company(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating company {company_id}: {e}")
            return None

    async def delete_company(self, company_id: UUID) -> bool:
        """Delete company and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(company_id)
            
        except Exception as e:
            logger.error(f"Error deleting company {company_id}: {e}")
            return False

