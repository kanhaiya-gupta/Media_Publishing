"""
OwnLens - Company Domain: Department Repository

Repository for company_departments table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.company.department import Department, DepartmentCreate, DepartmentUpdate

logger = logging.getLogger(__name__)


class DepartmentRepository(BaseRepository[Department]):
    """Repository for company_departments table."""

    def __init__(self, connection_manager):
        """Initialize the department repository."""
        super().__init__(connection_manager, table_name="company_departments")
        self.departments_table = "company_departments"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "department_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_departments"

    # ============================================================================
    # DEPARTMENT OPERATIONS
    # ============================================================================

    async def create_department(self, department_data: DepartmentCreate) -> Optional[Department]:
        """
        Create a new department record.
        
        Args:
            department_data: Department creation data (dict or Pydantic model)
            
        Returns:
            Created department with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(department_data, table_name=self.departments_table)
            if result:
                result = self._convert_json_to_lists(result)
                return Department(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating department: {e}")
            return None

    async def get_department_by_id(self, department_id: UUID) -> Optional[Department]:
        """Get department by ID."""
        try:
            result = await self.get_by_id(department_id)
            if result:
                result = self._convert_json_to_lists(result)
                return Department(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting department by ID {department_id}: {e}")
            return None

    async def get_department_by_code(self, department_code: str, company_id: Optional[UUID] = None) -> Optional[Department]:
        """Get department by code."""
        try:
            filters = {"department_code": department_code}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return Department(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting department by code {department_code}: {e}")
            return None

    async def get_departments_by_company(self, company_id: UUID, active_only: bool = True) -> List[Department]:
        """Get all departments for a company."""
        try:
            filters = {"company_id": company_id}
            if active_only:
                filters["is_active"] = True
            
            results = await self.get_all(filters=filters, order_by="department_name")
            return [Department(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting departments by company {company_id}: {e}")
            return []

    async def get_active_departments(self, company_id: Optional[UUID] = None) -> List[Department]:
        """Get all active departments."""
        try:
            filters = {"is_active": True}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="department_name")
            return [Department(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active departments: {e}")
            return []

    async def update_department(self, department_id: UUID, department_data: DepartmentUpdate) -> Optional[Department]:
        """Update department data."""
        try:
            data = department_data.model_dump(exclude_unset=True)
            
            result = await self.update(department_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return Department(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating department {department_id}: {e}")
            return None

    async def delete_department(self, department_id: UUID) -> bool:
        """Delete department and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(department_id)
            
        except Exception as e:
            logger.error(f"Error deleting department {department_id}: {e}")
            return False

