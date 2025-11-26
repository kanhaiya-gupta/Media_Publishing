"""
OwnLens - Audit Domain: Audit Log Service

Service for audit log management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import AuditLogRepository
from ...models.audit.audit_log import AuditLog, AuditLogCreate, AuditLogUpdate

logger = logging.getLogger(__name__)


class AuditLogService(BaseService[AuditLog, AuditLogCreate, AuditLogUpdate, AuditLog]):
    """Service for audit log management."""
    
    def __init__(self, repository: AuditLogRepository, service_name: str = None):
        """Initialize the audit log service."""
        super().__init__(repository, service_name or "AuditLogService")
        self.repository: AuditLogRepository = repository
    
    def get_model_class(self):
        """Get the AuditLog model class."""
        return AuditLog
    
    def get_create_model_class(self):
        """Get the AuditLogCreate model class."""
        return AuditLogCreate
    
    def get_update_model_class(self):
        """Get the AuditLogUpdate model class."""
        return AuditLogUpdate
    
    def get_in_db_model_class(self):
        """Get the AuditLog model class."""
        return AuditLog
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for audit log operations."""
        return True  # Audit logs are typically append-only
    
    async def create_audit_log(self, log_data: AuditLogCreate) -> AuditLog:
        """Create a new audit log entry."""
        try:
            await self.validate_input(log_data, "create")
            await self.validate_business_rules(log_data, "create")
            
            result = await self.repository.create_audit_log(log_data)
            if not result:
                raise NotFoundError("Failed to create audit log", "CREATE_FAILED")
            
            self.log_operation("create_audit_log", {"log_id": str(result.log_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_audit_log", "create")
            raise
    
    async def get_audit_log_by_id(self, log_id: UUID) -> AuditLog:
        """Get audit log by ID."""
        try:
            result = await self.repository.get_audit_log_by_id(log_id)
            if not result:
                raise NotFoundError(f"Audit log with ID {log_id} not found", "AUDIT_LOG_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_audit_log_by_id", "read")
            raise
    
    async def get_audit_logs_by_user(self, user_id: UUID, limit: int = 100) -> List[AuditLog]:
        """Get audit logs for a user."""
        try:
            return await self.repository.get_audit_logs_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_audit_logs_by_user", "read")
            return []
    
    async def get_audit_logs_by_date_range(self, start_date: date, end_date: date, user_id: Optional[UUID] = None) -> List[AuditLog]:
        """Get audit logs within a date range."""
        try:
            return await self.repository.get_audit_logs_by_date_range(start_date, end_date, user_id)
        except Exception as e:
            await self.handle_error(e, "get_audit_logs_by_date_range", "read")
            return []

