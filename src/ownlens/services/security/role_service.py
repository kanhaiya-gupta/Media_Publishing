"""
OwnLens - Security Domain: Role Service

Service for role management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.security import RoleRepository
from ...models.security.role import Role, RoleCreate, RoleUpdate

logger = logging.getLogger(__name__)


class RoleService(BaseService[Role, RoleCreate, RoleUpdate, Role]):
    """Service for role management."""
    
    def __init__(self, repository: RoleRepository, service_name: str = None):
        """Initialize the role service."""
        super().__init__(repository, service_name or "RoleService")
        self.repository: RoleRepository = repository
    
    def get_model_class(self):
        """Get the Role model class."""
        return Role
    
    def get_create_model_class(self):
        """Get the RoleCreate model class."""
        return RoleCreate
    
    def get_update_model_class(self):
        """Get the RoleUpdate model class."""
        return RoleUpdate
    
    def get_in_db_model_class(self):
        """Get the Role model class."""
        return Role
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for role operations."""
        try:
            if operation == "create":
                # Check for duplicate role name
                if hasattr(data, 'role_name'):
                    role_name = data.role_name
                else:
                    role_name = data.get('role_name') if isinstance(data, dict) else None
                
                if role_name:
                    existing = await self.repository.get_role_by_name(role_name)
                    if existing:
                        raise ValidationError(
                            f"Role with name '{role_name}' already exists",
                            "DUPLICATE_ROLE_NAME"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_role(self, role_data: RoleCreate) -> Role:
        """Create a new role."""
        try:
            await self.validate_input(role_data, "create")
            await self.validate_business_rules(role_data, "create")
            
            result = await self.repository.create_role(role_data)
            if not result:
                raise NotFoundError("Failed to create role", "CREATE_FAILED")
            
            self.log_operation("create_role", {"role_id": str(result.role_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_role", "create")
            raise
    
    async def get_role_by_id(self, role_id: UUID) -> Role:
        """Get role by ID."""
        try:
            result = await self.repository.get_role_by_id(role_id)
            if not result:
                raise NotFoundError(f"Role with ID {role_id} not found", "ROLE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_role_by_id", "read")
            raise
    
    async def get_all_roles(self) -> List[Role]:
        """Get all roles."""
        try:
            return await self.repository.get_all_roles()
        except Exception as e:
            await self.handle_error(e, "get_all_roles", "read")
            return []
    
    async def update_role(self, role_id: UUID, role_data: RoleUpdate) -> Role:
        """Update role."""
        try:
            await self.get_role_by_id(role_id)
            await self.validate_input(role_data, "update")
            await self.validate_business_rules(role_data, "update")
            
            result = await self.repository.update_role(role_id, role_data)
            if not result:
                raise NotFoundError(f"Failed to update role {role_id}", "UPDATE_FAILED")
            
            self.log_operation("update_role", {"role_id": str(role_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_role", "update")
            raise

