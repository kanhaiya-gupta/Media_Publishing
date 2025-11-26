"""
OwnLens - Audit Domain: Compliance Event Service

Service for compliance event management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import ComplianceEventRepository
from ...models.audit.compliance_event import ComplianceEvent, ComplianceEventCreate, ComplianceEventUpdate

logger = logging.getLogger(__name__)


class ComplianceEventService(BaseService[ComplianceEvent, ComplianceEventCreate, ComplianceEventUpdate, ComplianceEvent]):
    """Service for compliance event management."""
    
    def __init__(self, repository: ComplianceEventRepository, service_name: str = None):
        """Initialize the compliance event service."""
        super().__init__(repository, service_name or "ComplianceEventService")
        self.repository: ComplianceEventRepository = repository
    
    def get_model_class(self):
        """Get the ComplianceEvent model class."""
        return ComplianceEvent
    
    def get_create_model_class(self):
        """Get the ComplianceEventCreate model class."""
        return ComplianceEventCreate
    
    def get_update_model_class(self):
        """Get the ComplianceEventUpdate model class."""
        return ComplianceEventUpdate
    
    def get_in_db_model_class(self):
        """Get the ComplianceEvent model class."""
        return ComplianceEvent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for compliance event operations."""
        try:
            if operation == "create":
                # Validate compliance type
                if hasattr(data, 'compliance_type'):
                    compliance_type = data.compliance_type
                else:
                    compliance_type = data.get('compliance_type') if isinstance(data, dict) else None
                
                if not compliance_type or len(compliance_type.strip()) == 0:
                    raise ValidationError(
                        "Compliance type is required",
                        "INVALID_COMPLIANCE_TYPE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_compliance_event(self, event_data: ComplianceEventCreate) -> ComplianceEvent:
        """Create a new compliance event."""
        try:
            await self.validate_input(event_data, "create")
            await self.validate_business_rules(event_data, "create")
            
            result = await self.repository.create_compliance_event(event_data)
            if not result:
                raise NotFoundError("Failed to create compliance event", "CREATE_FAILED")
            
            self.log_operation("create_compliance_event", {"event_id": str(result.event_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_compliance_event", "create")
            raise
    
    async def get_compliance_event_by_id(self, event_id: UUID) -> ComplianceEvent:
        """Get compliance event by ID."""
        try:
            result = await self.repository.get_compliance_event_by_id(event_id)
            if not result:
                raise NotFoundError(f"Compliance event with ID {event_id} not found", "COMPLIANCE_EVENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_compliance_event_by_id", "read")
            raise
    
    async def get_compliance_events_by_company(self, company_id: UUID, limit: int = 100) -> List[ComplianceEvent]:
        """Get compliance events for a company."""
        try:
            return await self.repository.get_compliance_events_by_company(company_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_compliance_events_by_company", "read")
            return []

