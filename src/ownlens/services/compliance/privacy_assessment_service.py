"""
OwnLens - Compliance Domain: Privacy Assessment Service

Service for privacy assessment management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import PrivacyAssessmentRepository
from ...models.compliance.privacy_assessment import PrivacyAssessment, PrivacyAssessmentCreate, PrivacyAssessmentUpdate

logger = logging.getLogger(__name__)


class PrivacyAssessmentService(BaseService[PrivacyAssessment, PrivacyAssessmentCreate, PrivacyAssessmentUpdate, PrivacyAssessment]):
    """Service for privacy assessment management."""
    
    def __init__(self, repository: PrivacyAssessmentRepository, service_name: str = None):
        """Initialize the privacy assessment service."""
        super().__init__(repository, service_name or "PrivacyAssessmentService")
        self.repository: PrivacyAssessmentRepository = repository
    
    def get_model_class(self):
        """Get the PrivacyAssessment model class."""
        return PrivacyAssessment
    
    def get_create_model_class(self):
        """Get the PrivacyAssessmentCreate model class."""
        return PrivacyAssessmentCreate
    
    def get_update_model_class(self):
        """Get the PrivacyAssessmentUpdate model class."""
        return PrivacyAssessmentUpdate
    
    def get_in_db_model_class(self):
        """Get the PrivacyAssessment model class."""
        return PrivacyAssessment
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for privacy assessment operations."""
        try:
            if operation == "create":
                # Validate assessment name
                if hasattr(data, 'assessment_name'):
                    name = data.assessment_name
                else:
                    name = data.get('assessment_name') if isinstance(data, dict) else None
                
                if not name or len(name.strip()) == 0:
                    raise ValidationError(
                        "Assessment name is required",
                        "INVALID_NAME"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_privacy_assessment(self, assessment_data: PrivacyAssessmentCreate) -> PrivacyAssessment:
        """Create a new privacy assessment."""
        try:
            await self.validate_input(assessment_data, "create")
            await self.validate_business_rules(assessment_data, "create")
            
            result = await self.repository.create_privacy_assessment(assessment_data)
            if not result:
                raise NotFoundError("Failed to create privacy assessment", "CREATE_FAILED")
            
            self.log_operation("create_privacy_assessment", {"assessment_id": str(result.assessment_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_privacy_assessment", "create")
            raise
    
    async def get_privacy_assessment_by_id(self, assessment_id: UUID) -> PrivacyAssessment:
        """Get privacy assessment by ID."""
        try:
            result = await self.repository.get_privacy_assessment_by_id(assessment_id)
            if not result:
                raise NotFoundError(f"Privacy assessment with ID {assessment_id} not found", "PRIVACY_ASSESSMENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_privacy_assessment_by_id", "read")
            raise
    
    async def get_privacy_assessments_by_company(self, company_id: UUID, limit: int = 100) -> List[PrivacyAssessment]:
        """Get privacy assessments for a company."""
        try:
            return await self.repository.get_privacy_assessments_by_company(company_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_privacy_assessments_by_company", "read")
            return []

