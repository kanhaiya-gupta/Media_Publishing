"""
OwnLens - Compliance Domain: Breach Incident Service

Service for breach incident management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import BreachIncidentRepository
from ...models.compliance.breach_incident import BreachIncident, BreachIncidentCreate, BreachIncidentUpdate

logger = logging.getLogger(__name__)


class BreachIncidentService(BaseService[BreachIncident, BreachIncidentCreate, BreachIncidentUpdate, BreachIncident]):
    """Service for breach incident management."""
    
    def __init__(self, repository: BreachIncidentRepository, service_name: str = None):
        """Initialize the breach incident service."""
        super().__init__(repository, service_name or "BreachIncidentService")
        self.repository: BreachIncidentRepository = repository
    
    def get_model_class(self):
        """Get the BreachIncident model class."""
        return BreachIncident
    
    def get_create_model_class(self):
        """Get the BreachIncidentCreate model class."""
        return BreachIncidentCreate
    
    def get_update_model_class(self):
        """Get the BreachIncidentUpdate model class."""
        return BreachIncidentUpdate
    
    def get_in_db_model_class(self):
        """Get the BreachIncident model class."""
        return BreachIncident
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for breach incident operations."""
        try:
            if operation == "create":
                # Validate incident description
                if hasattr(data, 'description'):
                    desc = data.description
                else:
                    desc = data.get('description') if isinstance(data, dict) else None
                
                if not desc or len(desc.strip()) == 0:
                    raise ValidationError(
                        "Incident description is required",
                        "INVALID_DESCRIPTION"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_breach_incident(self, incident_data: BreachIncidentCreate) -> BreachIncident:
        """Create a new breach incident."""
        try:
            await self.validate_input(incident_data, "create")
            await self.validate_business_rules(incident_data, "create")
            
            result = await self.repository.create_breach_incident(incident_data)
            if not result:
                raise NotFoundError("Failed to create breach incident", "CREATE_FAILED")
            
            self.log_operation("create_breach_incident", {"incident_id": str(result.incident_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_breach_incident", "create")
            raise
    
    async def get_breach_incident_by_id(self, incident_id: UUID) -> BreachIncident:
        """Get breach incident by ID."""
        try:
            result = await self.repository.get_breach_incident_by_id(incident_id)
            if not result:
                raise NotFoundError(f"Breach incident with ID {incident_id} not found", "BREACH_INCIDENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_breach_incident_by_id", "read")
            raise
    
    async def get_breach_incidents_by_company(self, company_id: UUID, limit: int = 100) -> List[BreachIncident]:
        """Get breach incidents for a company."""
        try:
            return await self.repository.get_breach_incidents_by_company(company_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_breach_incidents_by_company", "read")
            return []

