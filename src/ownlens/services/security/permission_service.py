"""
OwnLens - Security Domain: Permission Service

Service for permission management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.security import PermissionRepository
from ...models.security.permission import Permission, PermissionCreate, PermissionUpdate

logger = logging.getLogger(__name__)


class PermissionService(BaseService[Permission, PermissionCreate, PermissionUpdate, Permission]):
    """Service for permission management."""
    
    def __init__(self, repository: PermissionRepository, service_name: str = None):
        """Initialize the permission service."""
        super().__init__(repository, service_name or "PermissionService")
        self.repository: PermissionRepository = repository
    
    def get_model_class(self):
        """Get the Permission model class."""
        return Permission
    
    def get_create_model_class(self):
        """Get the PermissionCreate model class."""
        return PermissionCreate
    
    def get_update_model_class(self):
        """Get the PermissionUpdate model class."""
        return PermissionUpdate
    
    def get_in_db_model_class(self):
        """Get the Permission model class."""
        return Permission
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for permission operations."""
        try:
            if operation == "create":
                # Check for duplicate permission name
                if hasattr(data, 'permission_name'):
                    perm_name = data.permission_name
                else:
                    perm_name = data.get('permission_name') if isinstance(data, dict) else None
                
                if perm_name:
                    existing = await self.repository.get_permission_by_name(perm_name)
                    if existing:
                        raise ValidationError(
                            f"Permission with name '{perm_name}' already exists",
                            "DUPLICATE_PERMISSION_NAME"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_permission(self, permission_data: PermissionCreate) -> Permission:
        """Create a new permission."""
        try:
            await self.validate_input(permission_data, "create")
            await self.validate_business_rules(permission_data, "create")
            
            result = await self.repository.create_permission(permission_data)
            if not result:
                raise NotFoundError("Failed to create permission", "CREATE_FAILED")
            
            self.log_operation("create_permission", {"permission_id": str(result.permission_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_permission", "create")
            raise
    
    async def get_permission_by_id(self, permission_id: UUID) -> Permission:
        """Get permission by ID."""
        try:
            result = await self.repository.get_permission_by_id(permission_id)
            if not result:
                raise NotFoundError(f"Permission with ID {permission_id} not found", "PERMISSION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_permission_by_id", "read")
            raise
    
    async def get_all_permissions(self) -> List[Permission]:
        """Get all permissions."""
        try:
            return await self.repository.get_all_permissions()
        except Exception as e:
            await self.handle_error(e, "get_all_permissions", "read")
            return []

