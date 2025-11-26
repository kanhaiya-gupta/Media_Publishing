"""
OwnLens - Base Domain: Company Service

Service for company management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.base import CompanyRepository
from ...models.base.company import Company, CompanyCreate, CompanyUpdate

logger = logging.getLogger(__name__)


class CompanyService(BaseService[Company, CompanyCreate, CompanyUpdate, Company]):
    """
    Service for company management.
    
    Provides business logic for company operations including creation,
    updates, validation, and business rule enforcement.
    """
    
    def __init__(self, repository: CompanyRepository, service_name: str = None):
        """
        Initialize the company service.
        
        Args:
            repository: Company repository instance
            service_name: Optional service name
        """
        super().__init__(repository, service_name or "CompanyService")
        self.repository: CompanyRepository = repository
    
    # ==================== ABSTRACT METHOD IMPLEMENTATIONS ====================
    
    def get_model_class(self):
        """Get the Company model class."""
        return Company
    
    def get_create_model_class(self):
        """Get the CompanyCreate model class."""
        return CompanyCreate
    
    def get_update_model_class(self):
        """Get the CompanyUpdate model class."""
        return CompanyUpdate
    
    def get_in_db_model_class(self):
        """Get the Company model class (same as read model)."""
        return Company
    
    # ==================== BUSINESS RULE VALIDATION ====================
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """
        Validate business rules for company operations.
        
        Args:
            data: Company data
            operation: Operation being performed (create, update, delete)
            
        Returns:
            True if business rules pass
            
        Raises:
            ValidationError: If business rules fail
        """
        try:
            # Check for duplicate company code
            if operation == "create":
                if hasattr(data, 'company_code'):
                    company_code = data.company_code
                else:
                    company_code = data.get('company_code') if isinstance(data, dict) else None
                
                if company_code:
                    existing = await self.repository.get_company_by_code(company_code)
                    if existing:
                        raise ValidationError(
                            f"Company with code '{company_code}' already exists",
                            "DUPLICATE_COMPANY_CODE"
                        )
            
            # Validate country code if provided
            if hasattr(data, 'headquarters_country_code'):
                country_code = data.headquarters_country_code
            else:
                country_code = data.get('headquarters_country_code') if isinstance(data, dict) else None
            
            if country_code:
                # Could add validation against countries table here
                if len(country_code) != 2:
                    raise ValidationError(
                        "Country code must be 2 characters (ISO 3166-1 alpha-2)",
                        "INVALID_COUNTRY_CODE"
                    )
            
            return True
            
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    # ==================== COMPANY OPERATIONS ====================
    
    async def create_company(self, company_data: CompanyCreate) -> Company:
        """
        Create a new company.
        
        Args:
            company_data: Company creation data
            
        Returns:
            Created company
            
        Raises:
            ValidationError: If validation fails
            ConflictError: If company code already exists
        """
        try:
            # Validate input
            await self.validate_input(company_data, "create")
            await self.validate_business_rules(company_data, "create")
            
            # Create company
            result = await self.repository.create_company(company_data)
            
            if not result:
                raise NotFoundError("Failed to create company", "CREATE_FAILED")
            
            self.log_operation("create_company", {"company_id": str(result.company_id)})
            return result
            
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_company", "create")
            raise
    
    async def get_company_by_id(self, company_id: UUID) -> Company:
        """
        Get company by ID.
        
        Args:
            company_id: Company ID
            
        Returns:
            Company entity
            
        Raises:
            NotFoundError: If company not found
        """
        try:
            result = await self.repository.get_company_by_id(company_id)
            
            if not result:
                raise NotFoundError(f"Company with ID {company_id} not found", "COMPANY_NOT_FOUND")
            
            return result
            
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_company_by_id", "read")
            raise
    
    async def get_company_by_code(self, company_code: str) -> Optional[Company]:
        """
        Get company by code.
        
        Args:
            company_code: Company code
            
        Returns:
            Company entity or None if not found
        """
        try:
            return await self.repository.get_company_by_code(company_code)
        except Exception as e:
            await self.handle_error(e, "get_company_by_code", "read")
            raise
    
    async def get_active_companies(self) -> List[Company]:
        """
        Get all active companies.
        
        Returns:
            List of active companies
        """
        try:
            return await self.repository.get_active_companies()
        except Exception as e:
            await self.handle_error(e, "get_active_companies", "read")
            return []
    
    async def get_companies_by_country(self, country_code: str) -> List[Company]:
        """
        Get companies by headquarters country.
        
        Args:
            country_code: Country code
            
        Returns:
            List of companies
        """
        try:
            return await self.repository.get_companies_by_country(country_code)
        except Exception as e:
            await self.handle_error(e, "get_companies_by_country", "read")
            return []
    
    async def update_company(self, company_id: UUID, company_data: CompanyUpdate) -> Company:
        """
        Update company.
        
        Args:
            company_id: Company ID
            company_data: Updated company data
            
        Returns:
            Updated company
            
        Raises:
            NotFoundError: If company not found
            ValidationError: If validation fails
        """
        try:
            # Check if company exists
            existing = await self.get_company_by_id(company_id)
            
            # Validate input
            await self.validate_input(company_data, "update")
            await self.validate_business_rules(company_data, "update")
            
            # Update company
            result = await self.repository.update_company(company_id, company_data)
            
            if not result:
                raise NotFoundError(f"Failed to update company {company_id}", "UPDATE_FAILED")
            
            self.log_operation("update_company", {"company_id": str(company_id)})
            return result
            
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_company", "update")
            raise
    
    async def delete_company(self, company_id: UUID) -> bool:
        """
        Delete company.
        
        Args:
            company_id: Company ID
            
        Returns:
            True if deletion successful
            
        Raises:
            NotFoundError: If company not found
        """
        try:
            # Check if company exists
            await self.get_company_by_id(company_id)
            
            # Delete company
            result = await self.repository.delete_company(company_id)
            
            if result:
                self.log_operation("delete_company", {"company_id": str(company_id)})
            
            return result
            
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "delete_company", "delete")
            raise

