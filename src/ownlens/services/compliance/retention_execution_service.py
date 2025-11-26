"""
OwnLens - Compliance Domain: Retention Execution Service

Service for retention execution management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import RetentionExecutionRepository
from ...models.compliance.retention_execution import RetentionExecution, RetentionExecutionCreate, RetentionExecutionUpdate

logger = logging.getLogger(__name__)


class RetentionExecutionService(BaseService[RetentionExecution, RetentionExecutionCreate, RetentionExecutionUpdate, RetentionExecution]):
    """Service for retention execution management."""
    
    def __init__(self, repository: RetentionExecutionRepository, service_name: str = None):
        """Initialize the retention execution service."""
        super().__init__(repository, service_name or "RetentionExecutionService")
        self.repository: RetentionExecutionRepository = repository
    
    def get_model_class(self):
        """Get the RetentionExecution model class."""
        return RetentionExecution
    
    def get_create_model_class(self):
        """Get the RetentionExecutionCreate model class."""
        return RetentionExecutionCreate
    
    def get_update_model_class(self):
        """Get the RetentionExecutionUpdate model class."""
        return RetentionExecutionUpdate
    
    def get_in_db_model_class(self):
        """Get the RetentionExecution model class."""
        return RetentionExecution
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for retention execution operations."""
        try:
            if operation == "create":
                # Validate policy ID
                if hasattr(data, 'policy_id'):
                    policy_id = data.policy_id
                else:
                    policy_id = data.get('policy_id') if isinstance(data, dict) else None
                
                if not policy_id:
                    raise ValidationError(
                        "Policy ID is required",
                        "INVALID_POLICY"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_retention_execution(self, execution_data: RetentionExecutionCreate) -> RetentionExecution:
        """Create a new retention execution."""
        try:
            await self.validate_input(execution_data, "create")
            await self.validate_business_rules(execution_data, "create")
            
            result = await self.repository.create_retention_execution(execution_data)
            if not result:
                raise NotFoundError("Failed to create retention execution", "CREATE_FAILED")
            
            self.log_operation("create_retention_execution", {"execution_id": str(result.execution_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_retention_execution", "create")
            raise
    
    async def get_retention_execution_by_id(self, execution_id: UUID) -> RetentionExecution:
        """Get retention execution by ID."""
        try:
            result = await self.repository.get_retention_execution_by_id(execution_id)
            if not result:
                raise NotFoundError(f"Retention execution with ID {execution_id} not found", "RETENTION_EXECUTION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_retention_execution_by_id", "read")
            raise
    
    async def get_retention_executions_by_policy(self, policy_id: UUID, limit: int = 100) -> List[RetentionExecution]:
        """Get retention executions for a policy."""
        try:
            return await self.repository.get_retention_executions_by_policy(policy_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_retention_executions_by_policy", "read")
            return []

