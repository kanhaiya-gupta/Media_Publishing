"""
OwnLens - Company Domain: Employee Engagement Repository

Repository for company_employee_engagement table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.company.employee_engagement import EmployeeEngagement, EmployeeEngagementCreate, EmployeeEngagementUpdate

logger = logging.getLogger(__name__)


class EmployeeEngagementRepository(BaseRepository[EmployeeEngagement]):
    """Repository for company_employee_engagement table."""

    def __init__(self, connection_manager):
        """Initialize the employee engagement repository."""
        super().__init__(connection_manager, table_name="company_employee_engagement")
        self.employee_engagement_table = "company_employee_engagement"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "engagement_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_employee_engagement"

    # ============================================================================
    # EMPLOYEE ENGAGEMENT OPERATIONS
    # ============================================================================

    async def create_employee_engagement(self, engagement_data: EmployeeEngagementCreate) -> Optional[EmployeeEngagement]:
        """
        Create a new employee engagement record.
        
        Args:
            engagement_data: Employee engagement creation data (dict or Pydantic model)
            
        Returns:
            Created employee engagement with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(engagement_data, table_name=self.employee_engagement_table)
            if result:
                result = self._convert_json_to_lists(result)
                return EmployeeEngagement(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating employee engagement: {e}")
            return None

    async def get_employee_engagement_by_id(self, engagement_id: UUID) -> Optional[EmployeeEngagement]:
        """Get employee engagement by ID."""
        try:
            result = await self.get_by_id(engagement_id)
            if result:
                result = self._convert_json_to_lists(result)
                return EmployeeEngagement(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting employee engagement by ID {engagement_id}: {e}")
            return None

    async def get_employee_engagement_by_employee(self, employee_id: UUID, company_id: Optional[UUID] = None) -> Optional[EmployeeEngagement]:
        """Get engagement for an employee."""
        try:
            filters = {"employee_id": employee_id}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="engagement_date", limit=1)
            if results:
                result = self._convert_json_to_lists(results[0])
                return EmployeeEngagement(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting employee engagement by employee {employee_id}: {e}")
            return None

    async def get_employee_engagement_by_department(self, department_id: UUID, limit: int = 100) -> List[EmployeeEngagement]:
        """Get all employee engagement for a department."""
        try:
            results = await self.get_all(
                filters={"department_id": department_id},
                order_by="avg_engagement_score",
                limit=limit
            )
            return [EmployeeEngagement(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting employee engagement by department {department_id}: {e}")
            return []

    async def get_high_engagement_employees(self, company_id: UUID, min_score: float = 0.7, limit: int = 100) -> List[EmployeeEngagement]:
        """Get employees with high engagement scores."""
        try:
            from ..shared.query_builder import select_all
            
            query, params = (
                select_all()
                .table(self.employee_engagement_table)
                .where_equals("company_id", company_id)
                .where_gte("avg_engagement_score", min_score)
                .order_by("avg_engagement_score")
                .limit(limit)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [EmployeeEngagement(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting high engagement employees for company {company_id}: {e}")
            return []

    async def get_employee_engagement_by_date(self, engagement_date: date, company_id: Optional[UUID] = None) -> List[EmployeeEngagement]:
        """Get employee engagement for a specific date."""
        try:
            filters = {"engagement_date": engagement_date}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="avg_engagement_score")
            return [EmployeeEngagement(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting employee engagement by date {engagement_date}: {e}")
            return []

    async def update_employee_engagement(self, engagement_id: UUID, engagement_data: EmployeeEngagementUpdate) -> Optional[EmployeeEngagement]:
        """Update employee engagement data."""
        try:
            data = engagement_data.model_dump(exclude_unset=True)
            
            result = await self.update(engagement_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return EmployeeEngagement(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating employee engagement {engagement_id}: {e}")
            return None

    async def delete_employee_engagement(self, engagement_id: UUID) -> bool:
        """Delete employee engagement and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(engagement_id)
            
        except Exception as e:
            logger.error(f"Error deleting employee engagement {engagement_id}: {e}")
            return False

