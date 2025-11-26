"""
OwnLens - ML Models Domain: Model A/B Test Service

Service for model A/B test management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import ModelABTestRepository
from ...models.ml_models.model_ab_test import ModelABTest, ModelABTestCreate, ModelABTestUpdate

logger = logging.getLogger(__name__)


class ModelABTestService(BaseService[ModelABTest, ModelABTestCreate, ModelABTestUpdate, ModelABTest]):
    """Service for model A/B test management."""
    
    def __init__(self, repository: ModelABTestRepository, service_name: str = None):
        """Initialize the model A/B test service."""
        super().__init__(repository, service_name or "ModelABTestService")
        self.repository: ModelABTestRepository = repository
    
    def get_model_class(self):
        """Get the ModelABTest model class."""
        return ModelABTest
    
    def get_create_model_class(self):
        """Get the ModelABTestCreate model class."""
        return ModelABTestCreate
    
    def get_update_model_class(self):
        """Get the ModelABTestUpdate model class."""
        return ModelABTestUpdate
    
    def get_in_db_model_class(self):
        """Get the ModelABTest model class."""
        return ModelABTest
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for model A/B test operations."""
        try:
            if operation == "create":
                # Validate test name
                if hasattr(data, 'test_name'):
                    name = data.test_name
                else:
                    name = data.get('test_name') if isinstance(data, dict) else None
                
                if not name or len(name.strip()) == 0:
                    raise ValidationError(
                        "Test name is required",
                        "INVALID_NAME"
                    )
                
                # Validate traffic split
                if hasattr(data, 'traffic_split_a') and hasattr(data, 'traffic_split_b'):
                    split_a = data.traffic_split_a
                    split_b = data.traffic_split_b
                else:
                    split_a = data.get('traffic_split_a') if isinstance(data, dict) else None
                    split_b = data.get('traffic_split_b') if isinstance(data, dict) else None
                
                if split_a is not None and split_b is not None:
                    if abs(split_a + split_b - 1.0) > 0.01:  # Allow small floating point errors
                        raise ValidationError(
                            "Traffic split A and B must sum to 1.0",
                            "INVALID_TRAFFIC_SPLIT"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_model_ab_test(self, test_data: ModelABTestCreate) -> ModelABTest:
        """Create a new model A/B test."""
        try:
            await self.validate_input(test_data, "create")
            await self.validate_business_rules(test_data, "create")
            
            result = await self.repository.create_model_ab_test(test_data)
            if not result:
                raise NotFoundError("Failed to create model A/B test", "CREATE_FAILED")
            
            self.log_operation("create_model_ab_test", {"test_id": str(result.test_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_model_ab_test", "create")
            raise
    
    async def get_model_ab_test_by_id(self, test_id: UUID) -> ModelABTest:
        """Get model A/B test by ID."""
        try:
            result = await self.repository.get_model_ab_test_by_id(test_id)
            if not result:
                raise NotFoundError(f"Model A/B test with ID {test_id} not found", "MODEL_AB_TEST_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_model_ab_test_by_id", "read")
            raise
    
    async def get_model_ab_tests_by_company(self, company_id: UUID, limit: int = 100) -> List[ModelABTest]:
        """Get model A/B tests for a company."""
        try:
            return await self.repository.get_model_ab_tests_by_company(company_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_model_ab_tests_by_company", "read")
            return []

