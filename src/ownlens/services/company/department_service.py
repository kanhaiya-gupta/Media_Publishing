"""
OwnLens - Company Domain: Department Service

Service for department management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.company import DepartmentRepository
from ...models.company.department import Department, DepartmentCreate, DepartmentUpdate

logger = logging.getLogger(__name__)


class DepartmentService(BaseService[Department, DepartmentCreate, DepartmentUpdate, Department]):
    """Service for department management."""
    
    def __init__(self, repository: DepartmentRepository, service_name: str = None):
        """Initialize the department service."""
        super().__init__(repository, service_name or "DepartmentService")
        self.repository: DepartmentRepository = repository
    
    def get_model_class(self):
        """Get the Department model class."""
        return Department
    
    def get_create_model_class(self):
        """Get the DepartmentCreate model class."""
        return DepartmentCreate
    
    def get_update_model_class(self):
        """Get the DepartmentUpdate model class."""
        return DepartmentUpdate
    
    def get_in_db_model_class(self):
        """Get the Department model class."""
        return Department
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for department operations."""
        try:
            if operation == "create":
                # Check for duplicate department code within company
                if hasattr(data, 'department_code') and hasattr(data, 'company_id'):
                    dept_code = data.department_code
                    company_id = data.company_id
                else:
                    dept_code = data.get('department_code') if isinstance(data, dict) else None
                    company_id = data.get('company_id') if isinstance(data, dict) else None
                
                if dept_code and company_id:
                    existing = await self.repository.get_department_by_code(company_id, dept_code)
                    if existing:
                        raise ValidationError(
                            f"Department with code '{dept_code}' already exists for this company",
                            "DUPLICATE_DEPARTMENT_CODE"
                        )
            
            # Validate employee count
            if hasattr(data, 'employee_count'):
                count = data.employee_count
            else:
                count = data.get('employee_count') if isinstance(data, dict) else None
            
            if count is not None and count < 0:
                raise ValidationError(
                    "Employee count must be non-negative",
                    "INVALID_EMPLOYEE_COUNT"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_department(self, department_data: DepartmentCreate) -> Department:
        """Create a new department."""
        try:
            await self.validate_input(department_data, "create")
            await self.validate_business_rules(department_data, "create")
            
            result = await self.repository.create_department(department_data)
            if not result:
                raise NotFoundError("Failed to create department", "CREATE_FAILED")
            
            self.log_operation("create_department", {"department_id": str(result.department_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_department", "create")
            raise
    
    async def get_department_by_id(self, department_id: UUID) -> Department:
        """Get department by ID."""
        try:
            result = await self.repository.get_department_by_id(department_id)
            if not result:
                raise NotFoundError(f"Department with ID {department_id} not found", "DEPARTMENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_department_by_id", "read")
            raise
    
    async def get_departments_by_company(self, company_id: UUID) -> List[Department]:
        """Get all departments for a company."""
        try:
            return await self.repository.get_departments_by_company(company_id)
        except Exception as e:
            await self.handle_error(e, "get_departments_by_company", "read")
            return []
    
    async def update_department(self, department_id: UUID, department_data: DepartmentUpdate) -> Department:
        """Update department."""
        try:
            await self.get_department_by_id(department_id)
            await self.validate_input(department_data, "update")
            await self.validate_business_rules(department_data, "update")
            
            result = await self.repository.update_department(department_id, department_data)
            if not result:
                raise NotFoundError(f"Failed to update department {department_id}", "UPDATE_FAILED")
            
            self.log_operation("update_department", {"department_id": str(department_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_department", "update")
            raise
    
    async def delete_department(self, department_id: UUID) -> bool:
        """Delete department."""
        try:
            await self.get_department_by_id(department_id)
            result = await self.repository.delete_department(department_id)
            if result:
                self.log_operation("delete_department", {"department_id": str(department_id)})
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "delete_department", "delete")
            raise

