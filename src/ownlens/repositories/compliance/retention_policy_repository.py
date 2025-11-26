"""
OwnLens - Compliance Domain: Retention Policy Repository

Repository for compliance_retention_policies table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.compliance.retention_policy import RetentionPolicy, RetentionPolicyCreate, RetentionPolicyUpdate

logger = logging.getLogger(__name__)


class RetentionPolicyRepository(BaseRepository[RetentionPolicy]):
    """Repository for compliance_retention_policies table."""

    def __init__(self, connection_manager):
        """Initialize the retention policy repository."""
        super().__init__(connection_manager, table_name="compliance_retention_policies")
        self.retention_policies_table = "compliance_retention_policies"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "policy_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_retention_policies"

    # ============================================================================
    # RETENTION POLICY OPERATIONS
    # ============================================================================

    async def create_retention_policy(self, policy_data: Union[RetentionPolicyCreate, Dict[str, Any]]) -> Optional[RetentionPolicy]:
        """
        Create a new retention policy record.
        
        Args:
            policy_data: Retention policy creation data (Pydantic model or dict)
            
        Returns:
            Created retention policy with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(policy_data, dict):
                policy_data = RetentionPolicyCreate.model_validate(policy_data)
            
            data = policy_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.retention_policies_table)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionPolicy(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating retention policy: {e}")
            return None

    async def get_retention_policy_by_id(self, policy_id: UUID) -> Optional[RetentionPolicy]:
        """Get retention policy by ID."""
        try:
            result = await self.get_by_id(policy_id)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionPolicy(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting retention policy by ID {policy_id}: {e}")
            return None

    async def get_active_retention_policies(self) -> List[RetentionPolicy]:
        """Get all active retention policies."""
        try:
            results = await self.get_all(
                filters={"is_active": True},
                order_by="policy_name"
            )
            return [RetentionPolicy(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active retention policies: {e}")
            return []

    async def get_retention_policies_by_table(self, table_name: str) -> List[RetentionPolicy]:
        """Get retention policies for a specific table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name, "is_active": True},
                order_by="policy_name"
            )
            return [RetentionPolicy(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting retention policies by table {table_name}: {e}")
            return []

    async def update_retention_policy(self, policy_id: UUID, policy_data: RetentionPolicyUpdate) -> Optional[RetentionPolicy]:
        """Update retention policy data."""
        try:
            data = policy_data.model_dump(exclude_unset=True)
            
            result = await self.update(policy_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return RetentionPolicy(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating retention policy {policy_id}: {e}")
            return None

    async def delete_retention_policy(self, policy_id: UUID) -> bool:
        """Delete retention policy and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(policy_id)
            
        except Exception as e:
            logger.error(f"Error deleting retention policy {policy_id}: {e}")
            return False

