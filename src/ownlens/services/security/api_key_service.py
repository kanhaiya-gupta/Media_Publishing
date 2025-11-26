"""
OwnLens - Security Domain: API Key Service

Service for API key management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.security import ApiKeyRepository
from ...models.security.api_key import ApiKey, ApiKeyCreate, ApiKeyUpdate

logger = logging.getLogger(__name__)


class ApiKeyService(BaseService[ApiKey, ApiKeyCreate, ApiKeyUpdate, ApiKey]):
    """Service for API key management."""
    
    def __init__(self, repository: ApiKeyRepository, service_name: str = None):
        """Initialize the API key service."""
        super().__init__(repository, service_name or "ApiKeyService")
        self.repository: ApiKeyRepository = repository
    
    def get_model_class(self):
        """Get the ApiKey model class."""
        return ApiKey
    
    def get_create_model_class(self):
        """Get the ApiKeyCreate model class."""
        return ApiKeyCreate
    
    def get_update_model_class(self):
        """Get the ApiKeyUpdate model class."""
        return ApiKeyUpdate
    
    def get_in_db_model_class(self):
        """Get the ApiKey model class."""
        return ApiKey
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for API key operations."""
        try:
            if operation == "create":
                # Validate expiration date is in the future
                if hasattr(data, 'expires_at'):
                    expires_at = data.expires_at
                else:
                    expires_at = data.get('expires_at') if isinstance(data, dict) else None
                
                if expires_at:
                    from datetime import datetime
                    if isinstance(expires_at, str):
                        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    if expires_at < datetime.utcnow():
                        raise ValidationError(
                            "API key expiration date must be in the future",
                            "INVALID_EXPIRATION_DATE"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_api_key(self, api_key_data: ApiKeyCreate) -> ApiKey:
        """Create a new API key."""
        try:
            await self.validate_input(api_key_data, "create")
            await self.validate_business_rules(api_key_data, "create")
            
            result = await self.repository.create_api_key(api_key_data)
            if not result:
                raise NotFoundError("Failed to create API key", "CREATE_FAILED")
            
            self.log_operation("create_api_key", {"api_key_id": str(result.api_key_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_api_key", "create")
            raise
    
    async def get_api_key_by_id(self, api_key_id: UUID) -> ApiKey:
        """Get API key by ID."""
        try:
            result = await self.repository.get_api_key_by_id(api_key_id)
            if not result:
                raise NotFoundError(f"API key with ID {api_key_id} not found", "API_KEY_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_api_key_by_id", "read")
            raise
    
    async def get_api_keys_by_user(self, user_id: UUID, active_only: bool = True) -> List[ApiKey]:
        """Get API keys for a user."""
        try:
            return await self.repository.get_api_keys_by_user(user_id, active_only=active_only)
        except Exception as e:
            await self.handle_error(e, "get_api_keys_by_user", "read")
            return []
    
    async def revoke_api_key(self, api_key_id: UUID) -> ApiKey:
        """Revoke an API key."""
        try:
            api_key = await self.get_api_key_by_id(api_key_id)
            
            from ...models.security.api_key import ApiKeyUpdate
            update_data = ApiKeyUpdate(is_active=False)
            
            result = await self.repository.update_api_key(api_key_id, update_data)
            if not result:
                raise NotFoundError(f"Failed to revoke API key {api_key_id}", "UPDATE_FAILED")
            
            self.log_operation("revoke_api_key", {"api_key_id": str(api_key_id)})
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "revoke_api_key", "update")
            raise

