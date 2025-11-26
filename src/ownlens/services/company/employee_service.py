"""
OwnLens - Company Domain: Employee Service

Service for employee management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.company import EmployeeRepository
from ...models.company.employee import Employee, EmployeeCreate, EmployeeUpdate

logger = logging.getLogger(__name__)


class EmployeeService(BaseService[Employee, EmployeeCreate, EmployeeUpdate, Employee]):
    """Service for employee management."""
    
    def __init__(self, repository: EmployeeRepository, service_name: str = None):
        """Initialize the employee service."""
        super().__init__(repository, service_name or "EmployeeService")
        self.repository: EmployeeRepository = repository
    
    def get_model_class(self):
        """Get the Employee model class."""
        return Employee
    
    def get_create_model_class(self):
        """Get the EmployeeCreate model class."""
        return EmployeeCreate
    
    def get_update_model_class(self):
        """Get the EmployeeUpdate model class."""
        return EmployeeUpdate
    
    def get_in_db_model_class(self):
        """Get the Employee model class."""
        return Employee
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for employee operations."""
        try:
            # Validate job level
            if hasattr(data, 'job_level'):
                level = data.job_level
            else:
                level = data.get('job_level') if isinstance(data, dict) else None
            
            if level and level not in ['entry', 'junior', 'mid', 'senior', 'lead', 'principal', 'director', 'vp', 'c_level']:
                raise ValidationError(
                    "Invalid job level",
                    "INVALID_JOB_LEVEL"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_employee(self, employee_data: EmployeeCreate) -> Employee:
        """Create a new employee."""
        try:
            await self.validate_input(employee_data, "create")
            await self.validate_business_rules(employee_data, "create")
            
            result = await self.repository.create_employee(employee_data)
            if not result:
                raise NotFoundError("Failed to create employee", "CREATE_FAILED")
            
            self.log_operation("create_employee", {"employee_id": str(result.employee_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_employee", "create")
            raise
    
    async def get_employee_by_id(self, employee_id: UUID) -> Employee:
        """Get employee by ID."""
        try:
            result = await self.repository.get_employee_by_id(employee_id)
            if not result:
                raise NotFoundError(f"Employee with ID {employee_id} not found", "EMPLOYEE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_employee_by_id", "read")
            raise
    
    async def get_employees_by_department(self, department_id: UUID) -> List[Employee]:
        """Get all employees in a department."""
        try:
            return await self.repository.get_employees_by_department(department_id)
        except Exception as e:
            await self.handle_error(e, "get_employees_by_department", "read")
            return []
    
    async def update_employee(self, employee_id: UUID, employee_data: EmployeeUpdate) -> Employee:
        """Update employee."""
        try:
            await self.get_employee_by_id(employee_id)
            await self.validate_input(employee_data, "update")
            await self.validate_business_rules(employee_data, "update")
            
            result = await self.repository.update_employee(employee_id, employee_data)
            if not result:
                raise NotFoundError(f"Failed to update employee {employee_id}", "UPDATE_FAILED")
            
            self.log_operation("update_employee", {"employee_id": str(employee_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_employee", "update")
            raise

