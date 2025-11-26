"""
OwnLens - Data Quality Domain: Quality Rule Service

Service for quality rule management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.data_quality import QualityRuleRepository
from ...models.data_quality.quality_rule import QualityRule, QualityRuleCreate, QualityRuleUpdate

logger = logging.getLogger(__name__)


class QualityRuleService(BaseService[QualityRule, QualityRuleCreate, QualityRuleUpdate, QualityRule]):
    """Service for quality rule management."""
    
    def __init__(self, repository: QualityRuleRepository, service_name: str = None):
        """Initialize the quality rule service."""
        super().__init__(repository, service_name or "QualityRuleService")
        self.repository: QualityRuleRepository = repository
    
    def get_model_class(self):
        """Get the QualityRule model class."""
        return QualityRule
    
    def get_create_model_class(self):
        """Get the QualityRuleCreate model class."""
        return QualityRuleCreate
    
    def get_update_model_class(self):
        """Get the QualityRuleUpdate model class."""
        return QualityRuleUpdate
    
    def get_in_db_model_class(self):
        """Get the QualityRule model class."""
        return QualityRule
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for quality rule operations."""
        return True  # Quality rules validation is typically handled at the model level
    
    async def create_quality_rule(self, rule_data: QualityRuleCreate) -> QualityRule:
        """Create a new quality rule."""
        try:
            await self.validate_input(rule_data, "create")
            await self.validate_business_rules(rule_data, "create")
            
            result = await self.repository.create_quality_rule(rule_data)
            if not result:
                raise NotFoundError("Failed to create quality rule", "CREATE_FAILED")
            
            self.log_operation("create_quality_rule", {"rule_id": str(result.rule_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_quality_rule", "create")
            raise
    
    async def get_quality_rule_by_id(self, rule_id: UUID) -> QualityRule:
        """Get quality rule by ID."""
        try:
            result = await self.repository.get_quality_rule_by_id(rule_id)
            if not result:
                raise NotFoundError(f"Quality rule with ID {rule_id} not found", "QUALITY_RULE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_quality_rule_by_id", "read")
            raise
    
    async def get_quality_rules_by_table(self, table_name: str) -> List[QualityRule]:
        """Get quality rules for a table."""
        try:
            return await self.repository.get_quality_rules_by_table(table_name)
        except Exception as e:
            await self.handle_error(e, "get_quality_rules_by_table", "read")
            return []

