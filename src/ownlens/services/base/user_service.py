"""
OwnLens - Base Domain: User Service

Service for user management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.base import UserRepository
from ...models.base.user import User, UserCreate, UserUpdate

logger = logging.getLogger(__name__)


class UserService(BaseService[User, UserCreate, UserUpdate, User]):
    """Service for user management."""
    
    def __init__(self, repository: UserRepository, service_name: str = None):
        """Initialize the user service."""
        super().__init__(repository, service_name or "UserService")
        self.repository: UserRepository = repository
    
    def get_model_class(self):
        """Get the User model class."""
        return User
    
    def get_create_model_class(self):
        """Get the UserCreate model class."""
        return UserCreate
    
    def get_update_model_class(self):
        """Get the UserUpdate model class."""
        return UserUpdate
    
    def get_in_db_model_class(self):
        """Get the User model class."""
        return User
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user operations."""
        try:
            if operation == "create":
                # Check for duplicate email
                if hasattr(data, 'email'):
                    email = data.email
                else:
                    email = data.get('email') if isinstance(data, dict) else None
                
                if email:
                    existing = await self.repository.get_user_by_email(email)
                    if existing:
                        raise ValidationError(
                            f"User with email '{email}' already exists",
                            "DUPLICATE_EMAIL"
                        )
                
                # Check for duplicate username
                if hasattr(data, 'username'):
                    username = data.username
                else:
                    username = data.get('username') if isinstance(data, dict) else None
                
                if username:
                    existing = await self.repository.get_user_by_username(username)
                    if existing:
                        raise ValidationError(
                            f"User with username '{username}' already exists",
                            "DUPLICATE_USERNAME"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_user(self, user_data: UserCreate) -> User:
        """Create a new user."""
        try:
            await self.validate_input(user_data, "create")
            await self.validate_business_rules(user_data, "create")
            
            result = await self.repository.create_user(user_data)
            if not result:
                raise NotFoundError("Failed to create user", "CREATE_FAILED")
            
            self.log_operation("create_user", {"user_id": str(result.user_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user", "create")
            raise
    
    async def get_user_by_id(self, user_id: UUID) -> User:
        """Get user by ID."""
        try:
            result = await self.repository.get_user_by_id(user_id)
            if not result:
                raise NotFoundError(f"User with ID {user_id} not found", "USER_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_by_id", "read")
            raise
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        try:
            return await self.repository.get_user_by_email(email)
        except Exception as e:
            await self.handle_error(e, "get_user_by_email", "read")
            raise
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        try:
            return await self.repository.get_user_by_username(username)
        except Exception as e:
            await self.handle_error(e, "get_user_by_username", "read")
            raise
    
    async def update_user(self, user_id: UUID, user_data: UserUpdate) -> User:
        """Update user."""
        try:
            await self.get_user_by_id(user_id)
            await self.validate_input(user_data, "update")
            await self.validate_business_rules(user_data, "update")
            
            result = await self.repository.update_user(user_id, user_data)
            if not result:
                raise NotFoundError(f"Failed to update user {user_id}", "UPDATE_FAILED")
            
            self.log_operation("update_user", {"user_id": str(user_id)})
            return result
        except (NotFoundError, ValidationError):
            raise
        except Exception as e:
            await self.handle_error(e, "update_user", "update")
            raise
    
    async def delete_user(self, user_id: UUID) -> bool:
        """Delete user."""
        try:
            await self.get_user_by_id(user_id)
            result = await self.repository.delete_user(user_id)
            if result:
                self.log_operation("delete_user", {"user_id": str(user_id)})
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "delete_user", "delete")
            raise

