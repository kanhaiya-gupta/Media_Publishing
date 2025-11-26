"""
OwnLens - Data Quality Domain: Quality Check Service

Service for quality check management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.data_quality import QualityCheckRepository
from ...models.data_quality.quality_check import QualityCheck, QualityCheckCreate, QualityCheckUpdate

logger = logging.getLogger(__name__)


class QualityCheckService(BaseService[QualityCheck, QualityCheckCreate, QualityCheckUpdate, QualityCheck]):
    """Service for quality check management."""
    
    def __init__(self, repository: QualityCheckRepository, service_name: str = None):
        """Initialize the quality check service."""
        super().__init__(repository, service_name or "QualityCheckService")
        self.repository: QualityCheckRepository = repository
    
    def get_model_class(self):
        """Get the QualityCheck model class."""
        return QualityCheck
    
    def get_create_model_class(self):
        """Get the QualityCheckCreate model class."""
        return QualityCheckCreate
    
    def get_update_model_class(self):
        """Get the QualityCheckUpdate model class."""
        return QualityCheckUpdate
    
    def get_in_db_model_class(self):
        """Get the QualityCheck model class."""
        return QualityCheck
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for quality check operations."""
        try:
            # Validate status
            if hasattr(data, 'status'):
                status = data.status
            else:
                status = data.get('status') if isinstance(data, dict) else None
            
            if status and status not in ['pending', 'running', 'completed', 'failed', 'skipped']:
                raise ValidationError(
                    "Invalid check status",
                    "INVALID_STATUS"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_quality_check(self, check_data: QualityCheckCreate) -> QualityCheck:
        """Create a new quality check."""
        try:
            await self.validate_input(check_data, "create")
            await self.validate_business_rules(check_data, "create")
            
            result = await self.repository.create_quality_check(check_data)
            if not result:
                raise NotFoundError("Failed to create quality check", "CREATE_FAILED")
            
            self.log_operation("create_quality_check", {"check_id": str(result.check_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_quality_check", "create")
            raise
    
    async def get_quality_checks_by_rule(self, rule_id: UUID, limit: int = 100) -> List[QualityCheck]:
        """Get quality checks for a rule."""
        try:
            return await self.repository.get_quality_checks_by_rule(rule_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_quality_checks_by_rule", "read")
            return []

