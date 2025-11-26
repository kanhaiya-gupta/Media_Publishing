"""
OwnLens - Data Quality Domain: Quality Alert Service

Service for quality alert management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.data_quality import QualityAlertRepository
from ...models.data_quality.quality_alert import QualityAlert, QualityAlertCreate, QualityAlertUpdate

logger = logging.getLogger(__name__)


class QualityAlertService(BaseService[QualityAlert, QualityAlertCreate, QualityAlertUpdate, QualityAlert]):
    """Service for quality alert management."""
    
    def __init__(self, repository: QualityAlertRepository, service_name: str = None):
        """Initialize the quality alert service."""
        super().__init__(repository, service_name or "QualityAlertService")
        self.repository: QualityAlertRepository = repository
    
    def get_model_class(self):
        """Get the QualityAlert model class."""
        return QualityAlert
    
    def get_create_model_class(self):
        """Get the QualityAlertCreate model class."""
        return QualityAlertCreate
    
    def get_update_model_class(self):
        """Get the QualityAlertUpdate model class."""
        return QualityAlertUpdate
    
    def get_in_db_model_class(self):
        """Get the QualityAlert model class."""
        return QualityAlert
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for quality alert operations."""
        try:
            if operation == "create":
                # Validate alert title
                if hasattr(data, 'alert_title'):
                    title = data.alert_title
                else:
                    title = data.get('alert_title') if isinstance(data, dict) else None
                
                if not title or len(title.strip()) == 0:
                    raise ValidationError(
                        "Alert title is required",
                        "INVALID_TITLE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_quality_alert(self, alert_data: QualityAlertCreate) -> QualityAlert:
        """Create a new quality alert."""
        try:
            await self.validate_input(alert_data, "create")
            await self.validate_business_rules(alert_data, "create")
            
            result = await self.repository.create_quality_alert(alert_data)
            if not result:
                raise NotFoundError("Failed to create quality alert", "CREATE_FAILED")
            
            self.log_operation("create_quality_alert", {"alert_id": str(result.alert_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_quality_alert", "create")
            raise
    
    async def get_quality_alert_by_id(self, alert_id: UUID) -> QualityAlert:
        """Get quality alert by ID."""
        try:
            result = await self.repository.get_quality_alert_by_id(alert_id)
            if not result:
                raise NotFoundError(f"Quality alert with ID {alert_id} not found", "QUALITY_ALERT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_quality_alert_by_id", "read")
            raise
    
    async def get_quality_alerts_by_company(self, company_id: UUID, limit: int = 100) -> List[QualityAlert]:
        """Get quality alerts for a company."""
        try:
            return await self.repository.get_quality_alerts_by_company(company_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_quality_alerts_by_company", "read")
            return []

