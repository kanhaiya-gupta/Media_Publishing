"""
OwnLens - Data Quality Domain: Validation Result Service

Service for validation result management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.data_quality import ValidationResultRepository
from ...models.data_quality.validation_result import ValidationResult, ValidationResultCreate, ValidationResultUpdate

logger = logging.getLogger(__name__)


class ValidationResultService(BaseService[ValidationResult, ValidationResultCreate, ValidationResultUpdate, ValidationResult]):
    """Service for validation result management."""
    
    def __init__(self, repository: ValidationResultRepository, service_name: str = None):
        """Initialize the validation result service."""
        super().__init__(repository, service_name or "ValidationResultService")
        self.repository: ValidationResultRepository = repository
    
    def get_model_class(self):
        """Get the ValidationResult model class."""
        return ValidationResult
    
    def get_create_model_class(self):
        """Get the ValidationResultCreate model class."""
        return ValidationResultCreate
    
    def get_update_model_class(self):
        """Get the ValidationResultUpdate model class."""
        return ValidationResultUpdate
    
    def get_in_db_model_class(self):
        """Get the ValidationResult model class."""
        return ValidationResult
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for validation result operations."""
        try:
            if operation == "create":
                # Validate table name
                if hasattr(data, 'table_name'):
                    table = data.table_name
                else:
                    table = data.get('table_name') if isinstance(data, dict) else None
                
                if not table or len(table.strip()) == 0:
                    raise ValidationError(
                        "Table name is required",
                        "INVALID_TABLE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_validation_result(self, result_data: ValidationResultCreate) -> ValidationResult:
        """Create a new validation result."""
        try:
            await self.validate_input(result_data, "create")
            await self.validate_business_rules(result_data, "create")
            
            result = await self.repository.create_validation_result(result_data)
            if not result:
                raise NotFoundError("Failed to create validation result", "CREATE_FAILED")
            
            self.log_operation("create_validation_result", {"validation_id": str(result.validation_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_validation_result", "create")
            raise
    
    async def get_validation_result_by_id(self, validation_id: UUID) -> ValidationResult:
        """Get validation result by ID."""
        try:
            result = await self.repository.get_validation_result_by_id(validation_id)
            if not result:
                raise NotFoundError(f"Validation result with ID {validation_id} not found", "VALIDATION_RESULT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_validation_result_by_id", "read")
            raise
    
    async def get_validation_results_by_rule(self, rule_id: UUID, limit: int = 100) -> List[ValidationResult]:
        """Get validation results for a rule."""
        try:
            return await self.repository.get_validation_results_by_rule(rule_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_validation_results_by_rule", "read")
            return []

