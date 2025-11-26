"""
OwnLens - Data Quality Domain: Quality Rule Repository

Repository for data_quality_rules table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
import logging

from ..shared import BaseRepository
from ...models.data_quality.quality_rule import QualityRule, QualityRuleCreate, QualityRuleUpdate

logger = logging.getLogger(__name__)


class QualityRuleRepository(BaseRepository[QualityRule]):
    """Repository for data_quality_rules table."""

    def __init__(self, connection_manager):
        """Initialize the quality rule repository."""
        super().__init__(connection_manager, table_name="data_quality_rules")
        self.quality_rules_table = "data_quality_rules"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "rule_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "data_quality_rules"

    # ============================================================================
    # QUALITY RULE OPERATIONS
    # ============================================================================

    async def create_quality_rule(self, rule_data: QualityRuleCreate) -> Optional[QualityRule]:
        """
        Create a new quality rule record.
        
        Args:
            rule_data: Quality rule creation data
            
        Returns:
            Created quality rule with generated ID, or None if creation failed
        """
        try:
            data = rule_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.quality_rules_table)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityRule(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating quality rule: {e}")
            return None

    async def get_quality_rule_by_id(self, rule_id: UUID) -> Optional[QualityRule]:
        """Get quality rule by ID."""
        try:
            result = await self.get_by_id(rule_id)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityRule(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting quality rule by ID {rule_id}: {e}")
            return None

    async def get_quality_rules_by_table(self, table_name: str) -> List[QualityRule]:
        """Get all quality rules for a table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="rule_name"
            )
            return [QualityRule(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality rules by table {table_name}: {e}")
            return []

    async def get_active_quality_rules(self) -> List[QualityRule]:
        """Get all active quality rules."""
        try:
            results = await self.get_all(
                filters={"is_active": True},
                order_by="rule_name"
            )
            return [QualityRule(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active quality rules: {e}")
            return []

    async def update_quality_rule(self, rule_id: UUID, rule_data: QualityRuleUpdate) -> Optional[QualityRule]:
        """Update quality rule data."""
        try:
            data = rule_data.model_dump(exclude_unset=True)
            
            result = await self.update(rule_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityRule(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating quality rule {rule_id}: {e}")
            return None

    async def delete_quality_rule(self, rule_id: UUID) -> bool:
        """Delete quality rule and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(rule_id)
            
        except Exception as e:
            logger.error(f"Error deleting quality rule {rule_id}: {e}")
            return False

