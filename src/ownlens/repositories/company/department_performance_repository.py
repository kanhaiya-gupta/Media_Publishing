"""
OwnLens - Company Domain: Department Performance Repository

Repository for company_department_performance table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.company.department_performance import DepartmentPerformance, DepartmentPerformanceCreate, DepartmentPerformanceUpdate

logger = logging.getLogger(__name__)


class DepartmentPerformanceRepository(BaseRepository[DepartmentPerformance]):
    """Repository for company_department_performance table."""

    def __init__(self, connection_manager):
        """Initialize the department performance repository."""
        super().__init__(connection_manager, table_name="company_department_performance")
        self.department_performance_table = "company_department_performance"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "performance_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_department_performance"

    # ============================================================================
    # DEPARTMENT PERFORMANCE OPERATIONS
    # ============================================================================

    async def create_department_performance(self, performance_data: Union[DepartmentPerformanceCreate, Dict[str, Any]]) -> Optional[DepartmentPerformance]:
        """
        Create a new department performance record.
        
        Args:
            performance_data: Department performance creation data (dict or DepartmentPerformanceCreate model)
            
        Returns:
            Created department performance with generated ID, or None if creation failed
        """
        try:
            if isinstance(performance_data, dict):
                performance_data = DepartmentPerformanceCreate.model_validate(performance_data)
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.department_performance_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DepartmentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating department performance: {e}")
            return None

    async def get_department_performance_by_id(self, performance_id: UUID) -> Optional[DepartmentPerformance]:
        """Get department performance by ID."""
        try:
            result = await self.get_by_id(performance_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DepartmentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting department performance by ID {performance_id}: {e}")
            return None

    async def get_department_performance_by_department(self, department_id: UUID, company_id: Optional[UUID] = None) -> Optional[DepartmentPerformance]:
        """Get performance for a department."""
        try:
            filters = {"department_id": department_id}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="performance_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return DepartmentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting department performance by department {department_id}: {e}")
            return None

    async def get_top_performing_departments(self, company_id: UUID, limit: int = 100, order_by: str = "content_views") -> List[DepartmentPerformance]:
        """Get top performing departments."""
        try:
            results = await self.get_all(
                filters={"company_id": company_id},
                order_by=order_by,
                limit=limit
            )
            return [DepartmentPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top performing departments for company {company_id}: {e}")
            return []

    async def get_department_performance_by_date(self, performance_date: date, company_id: Optional[UUID] = None) -> List[DepartmentPerformance]:
        """Get department performance for a specific date."""
        try:
            filters = {"performance_date": performance_date}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="content_views")
            return [DepartmentPerformance(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting department performance by date {performance_date}: {e}")
            return []

    async def update_department_performance(self, performance_id: UUID, performance_data: DepartmentPerformanceUpdate) -> Optional[DepartmentPerformance]:
        """Update department performance data."""
        try:
            data = performance_data.model_dump(exclude_unset=True)
            
            result = await self.update(performance_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DepartmentPerformance(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating department performance {performance_id}: {e}")
            return None

    async def delete_department_performance(self, performance_id: UUID) -> bool:
        """Delete department performance and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(performance_id)
            
        except Exception as e:
            logger.error(f"Error deleting department performance {performance_id}: {e}")
            return False

