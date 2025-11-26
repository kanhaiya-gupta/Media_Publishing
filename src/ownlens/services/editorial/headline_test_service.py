"""
OwnLens - Editorial Domain: Headline Test Service

Service for headline A/B test management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import HeadlineTestRepository
from ...models.editorial.headline_test import HeadlineTest, HeadlineTestCreate, HeadlineTestUpdate

logger = logging.getLogger(__name__)


class HeadlineTestService(BaseService[HeadlineTest, HeadlineTestCreate, HeadlineTestUpdate, HeadlineTest]):
    """Service for headline A/B test management."""
    
    def __init__(self, repository: HeadlineTestRepository, service_name: str = None):
        """Initialize the headline test service."""
        super().__init__(repository, service_name or "HeadlineTestService")
        self.repository: HeadlineTestRepository = repository
    
    def get_model_class(self):
        """Get the HeadlineTest model class."""
        return HeadlineTest
    
    def get_create_model_class(self):
        """Get the HeadlineTestCreate model class."""
        return HeadlineTestCreate
    
    def get_update_model_class(self):
        """Get the HeadlineTestUpdate model class."""
        return HeadlineTestUpdate
    
    def get_in_db_model_class(self):
        """Get the HeadlineTest model class."""
        return HeadlineTest
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for headline test operations."""
        try:
            if operation == "create":
                # Validate headline variant A
                if hasattr(data, 'headline_variant_a'):
                    headline = data.headline_variant_a
                else:
                    headline = data.get('headline_variant_a') if isinstance(data, dict) else None
                
                if not headline or len(headline.strip()) == 0:
                    raise ValidationError(
                        "Headline variant A is required",
                        "INVALID_HEADLINE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_headline_test(self, test_data: HeadlineTestCreate) -> HeadlineTest:
        """Create a new headline test."""
        try:
            await self.validate_input(test_data, "create")
            await self.validate_business_rules(test_data, "create")
            
            result = await self.repository.create_headline_test(test_data)
            if not result:
                raise NotFoundError("Failed to create headline test", "CREATE_FAILED")
            
            self.log_operation("create_headline_test", {"test_id": str(result.test_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_headline_test", "create")
            raise
    
    async def get_headline_test_by_id(self, test_id: UUID) -> HeadlineTest:
        """Get headline test by ID."""
        try:
            result = await self.repository.get_headline_test_by_id(test_id)
            if not result:
                raise NotFoundError(f"Headline test with ID {test_id} not found", "HEADLINE_TEST_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_headline_test_by_id", "read")
            raise
    
    async def get_headline_tests_by_article(self, article_id: UUID, limit: int = 100) -> List[HeadlineTest]:
        """Get headline tests for an article."""
        try:
            return await self.repository.get_headline_tests_by_article(article_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_headline_tests_by_article", "read")
            return []

