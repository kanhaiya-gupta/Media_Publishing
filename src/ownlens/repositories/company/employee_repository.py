"""
OwnLens - Company Domain: Employee Repository

Repository for company_employees table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.company.employee import Employee, EmployeeCreate, EmployeeUpdate

logger = logging.getLogger(__name__)


class EmployeeRepository(BaseRepository[Employee]):
    """Repository for company_employees table."""

    def __init__(self, connection_manager):
        """Initialize the employee repository."""
        super().__init__(connection_manager, table_name="company_employees")
        self.employees_table = "company_employees"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "employee_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_employees"

    # ============================================================================
    # EMPLOYEE OPERATIONS
    # ============================================================================

    async def create_employee(self, employee_data: EmployeeCreate) -> Optional[Employee]:
        """
        Create a new employee record.
        
        Args:
            employee_data: Employee creation data (dict or Pydantic model)
            
        Returns:
            Created employee with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(employee_data, table_name=self.employees_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Employee(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating employee: {e}")
            return None

    async def get_employee_by_id(self, employee_id: UUID) -> Optional[Employee]:
        """Get employee by ID."""
        try:
            result = await self.get_by_id(employee_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Employee(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting employee by ID {employee_id}: {e}")
            return None

    async def get_employee_by_user(self, user_id: UUID, company_id: Optional[UUID] = None) -> Optional[Employee]:
        """Get employee by user ID."""
        try:
            filters = {"user_id": user_id}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return Employee(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting employee by user {user_id}: {e}")
            return None

    async def get_employees_by_company(self, company_id: UUID, active_only: bool = True, limit: int = 100, offset: int = 0) -> List[Employee]:
        """Get all employees for a company."""
        try:
            filters = {"company_id": company_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(
                filters=filters,
                order_by="hire_date",
                limit=limit,
                offset=offset
            )
            return [Employee(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting employees by company {company_id}: {e}")
            return []

    async def get_employees_by_department(self, department_id: UUID, active_only: bool = True) -> List[Employee]:
        """Get all employees in a department."""
        try:
            filters = {"department_id": department_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(filters=filters, order_by="hire_date")
            return [Employee(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting employees by department {department_id}: {e}")
            return []

    async def get_active_employees(self, company_id: Optional[UUID] = None) -> List[Employee]:
        """Get all active employees."""
        try:
            filters = {"is_active": True}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="hire_date")
            return [Employee(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active employees: {e}")
            return []

    async def update_employee(self, employee_id: UUID, employee_data: EmployeeUpdate) -> Optional[Employee]:
        """Update employee data."""
        try:
            data = employee_data.model_dump(exclude_unset=True)
            
            result = await self.update(employee_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Employee(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating employee {employee_id}: {e}")
            return None

    async def delete_employee(self, employee_id: UUID) -> bool:
        """Delete employee and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(employee_id)
            
        except Exception as e:
            logger.error(f"Error deleting employee {employee_id}: {e}")
            return False

