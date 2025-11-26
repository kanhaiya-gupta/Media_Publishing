"""
OwnLens - Compliance Domain: Retention Policy Service

Service for retention policy management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import RetentionPolicyRepository
from ...models.compliance.retention_policy import RetentionPolicy, RetentionPolicyCreate, RetentionPolicyUpdate

logger = logging.getLogger(__name__)


class RetentionPolicyService(BaseService[RetentionPolicy, RetentionPolicyCreate, RetentionPolicyUpdate, RetentionPolicy]):
    """Service for retention policy management."""
    
    def __init__(self, repository: RetentionPolicyRepository, service_name: str = None):
        """Initialize the retention policy service."""
        super().__init__(repository, service_name or "RetentionPolicyService")
        self.repository: RetentionPolicyRepository = repository
    
    def get_model_class(self):
        """Get the RetentionPolicy model class."""
        return RetentionPolicy
    
    def get_create_model_class(self):
        """Get the RetentionPolicyCreate model class."""
        return RetentionPolicyCreate
    
    def get_update_model_class(self):
        """Get the RetentionPolicyUpdate model class."""
        return RetentionPolicyUpdate
    
    def get_in_db_model_class(self):
        """Get the RetentionPolicy model class."""
        return RetentionPolicy
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for retention policy operations."""
        try:
            # Validate retention period is positive
            if hasattr(data, 'retention_days'):
                days = data.retention_days
            else:
                days = data.get('retention_days') if isinstance(data, dict) else None
            
            if days is not None and days <= 0:
                raise ValidationError(
                    "Retention period must be positive",
                    "INVALID_RETENTION_PERIOD"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_retention_policy(self, policy_data: RetentionPolicyCreate) -> RetentionPolicy:
        """Create a new retention policy."""
        try:
            await self.validate_input(policy_data, "create")
            await self.validate_business_rules(policy_data, "create")
            
            result = await self.repository.create_retention_policy(policy_data)
            if not result:
                raise NotFoundError("Failed to create retention policy", "CREATE_FAILED")
            
            self.log_operation("create_retention_policy", {"policy_id": str(result.policy_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_retention_policy", "create")
            raise
    
    async def get_retention_policy_by_id(self, policy_id: UUID) -> RetentionPolicy:
        """Get retention policy by ID."""
        try:
            result = await self.repository.get_retention_policy_by_id(policy_id)
            if not result:
                raise NotFoundError(f"Retention policy with ID {policy_id} not found", "RETENTION_POLICY_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_retention_policy_by_id", "read")
            raise
    
    async def get_active_retention_policies(self) -> List[RetentionPolicy]:
        """Get all active retention policies."""
        try:
            return await self.repository.get_active_retention_policies()
        except Exception as e:
            await self.handle_error(e, "get_active_retention_policies", "read")
            return []

