"""
OwnLens - Audit Domain: Security Event Service

Service for security event tracking.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import SecurityEventRepository
from ...models.audit.security_event import SecurityEvent, SecurityEventCreate, SecurityEventUpdate

logger = logging.getLogger(__name__)


class SecurityEventService(BaseService[SecurityEvent, SecurityEventCreate, SecurityEventUpdate, SecurityEvent]):
    """Service for security event tracking."""
    
    def __init__(self, repository: SecurityEventRepository, service_name: str = None):
        """Initialize the security event service."""
        super().__init__(repository, service_name or "SecurityEventService")
        self.repository: SecurityEventRepository = repository
    
    def get_model_class(self):
        """Get the SecurityEvent model class."""
        return SecurityEvent
    
    def get_create_model_class(self):
        """Get the SecurityEventCreate model class."""
        return SecurityEventCreate
    
    def get_update_model_class(self):
        """Get the SecurityEventUpdate model class."""
        return SecurityEventUpdate
    
    def get_in_db_model_class(self):
        """Get the SecurityEvent model class."""
        return SecurityEvent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for security event operations."""
        try:
            # Validate severity
            if hasattr(data, 'severity'):
                severity = data.severity
            else:
                severity = data.get('severity') if isinstance(data, dict) else None
            
            if severity and severity not in ['low', 'medium', 'high', 'critical']:
                raise ValidationError(
                    "Invalid severity level",
                    "INVALID_SEVERITY"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_security_event(self, event_data: SecurityEventCreate) -> SecurityEvent:
        """Create a new security event."""
        try:
            await self.validate_input(event_data, "create")
            await self.validate_business_rules(event_data, "create")
            
            result = await self.repository.create_security_event(event_data)
            if not result:
                raise NotFoundError("Failed to create security event", "CREATE_FAILED")
            
            self.log_operation("create_security_event", {"event_id": str(result.event_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_security_event", "create")
            raise
    
    async def get_security_events_by_severity(self, severity: str, limit: int = 100) -> List[SecurityEvent]:
        """Get security events by severity."""
        try:
            return await self.repository.get_security_events_by_severity(severity, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_security_events_by_severity", "read")
            return []

