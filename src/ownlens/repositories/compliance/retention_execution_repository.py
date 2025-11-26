"""
OwnLens - Compliance Domain: Retention Execution Repository

Repository for compliance_retention_executions table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.retention_execution import RetentionExecution, RetentionExecutionCreate, RetentionExecutionUpdate

logger = logging.getLogger(__name__)


class RetentionExecutionRepository(BaseRepository[RetentionExecution]):
    """Repository for compliance_retention_executions table."""

    def __init__(self, connection_manager):
        """Initialize the retention execution repository."""
        super().__init__(connection_manager, table_name="compliance_retention_executions")
        self.retention_executions_table = "compliance_retention_executions"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "execution_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_retention_executions"

    # ============================================================================
    # RETENTION EXECUTION OPERATIONS
    # ============================================================================

    async def create_retention_execution(self, execution_data: Union[RetentionExecutionCreate, Dict[str, Any]]) -> Optional[RetentionExecution]:
        """
        Create a new retention execution record.
        
        Args:
            execution_data: Retention execution creation data (Pydantic model or dict)
            
        Returns:
            Created retention execution with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(execution_data, dict):
                execution_data = RetentionExecutionCreate.model_validate(execution_data)
            
            data = execution_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.retention_executions_table)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionExecution(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating retention execution: {e}")
            return None

    async def get_retention_execution_by_id(self, execution_id: UUID) -> Optional[RetentionExecution]:
        """Get retention execution by ID."""
        try:
            result = await self.get_by_id(execution_id)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionExecution(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting retention execution by ID {execution_id}: {e}")
            return None

    async def get_executions_by_policy(self, policy_id: UUID, limit: int = 100, offset: int = 0) -> List[RetentionExecution]:
        """Get all executions for a retention policy."""
        try:
            results = await self.get_all(
                filters={"policy_id": policy_id},
                order_by="execution_date",
                limit=limit,
                offset=offset
            )
            return [RetentionExecution(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting executions by policy {policy_id}: {e}")
            return []

    async def get_executions_by_status(self, status: str, limit: int = 100) -> List[RetentionExecution]:
        """Get executions by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="execution_date",
                limit=limit
            )
            return [RetentionExecution(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting executions by status {status}: {e}")
            return []

    async def get_executions_by_date(self, execution_date: date) -> List[RetentionExecution]:
        """Get executions for a specific date."""
        try:
            results = await self.get_all(
                filters={"execution_date": execution_date},
                order_by="execution_date"
            )
            return [RetentionExecution(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting executions by date {execution_date}: {e}")
            return []

    async def update_retention_execution(self, execution_id: UUID, execution_data: RetentionExecutionUpdate) -> Optional[RetentionExecution]:
        """Update retention execution data."""
        try:
            data = execution_data.model_dump(exclude_unset=True)
            
            result = await self.update(execution_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionExecution(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating retention execution {execution_id}: {e}")
            return None

    async def delete_retention_execution(self, execution_id: UUID) -> bool:
        """Delete retention execution and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(execution_id)
            
        except Exception as e:
            logger.error(f"Error deleting retention execution {execution_id}: {e}")
            return False

