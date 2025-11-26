"""
OwnLens - Audit Domain: Data Access Service

Service for data access management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import DataAccessRepository
from ...models.audit.data_access import DataAccess, DataAccessCreate, DataAccessUpdate

logger = logging.getLogger(__name__)


class DataAccessService(BaseService[DataAccess, DataAccessCreate, DataAccessUpdate, DataAccess]):
    """Service for data access management."""
    
    def __init__(self, repository: DataAccessRepository, service_name: str = None):
        """Initialize the data access service."""
        super().__init__(repository, service_name or "DataAccessService")
        self.repository: DataAccessRepository = repository
    
    def get_model_class(self):
        """Get the DataAccess model class."""
        return DataAccess
    
    def get_create_model_class(self):
        """Get the DataAccessCreate model class."""
        return DataAccessCreate
    
    def get_update_model_class(self):
        """Get the DataAccessUpdate model class."""
        return DataAccessUpdate
    
    def get_in_db_model_class(self):
        """Get the DataAccess model class."""
        return DataAccess
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for data access operations."""
        try:
            if operation == "create":
                # Validate resource type
                if hasattr(data, 'resource_type'):
                    resource_type = data.resource_type
                else:
                    resource_type = data.get('resource_type') if isinstance(data, dict) else None
                
                if not resource_type or len(resource_type.strip()) == 0:
                    raise ValidationError(
                        "Resource type is required",
                        "INVALID_RESOURCE_TYPE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_data_access(self, access_data: DataAccessCreate) -> DataAccess:
        """Create a new data access record."""
        try:
            await self.validate_input(access_data, "create")
            await self.validate_business_rules(access_data, "create")
            
            result = await self.repository.create_data_access(access_data)
            if not result:
                raise NotFoundError("Failed to create data access record", "CREATE_FAILED")
            
            self.log_operation("create_data_access", {"access_id": str(result.access_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_data_access", "create")
            raise
    
    async def get_data_access_by_id(self, access_id: UUID) -> DataAccess:
        """Get data access by ID."""
        try:
            result = await self.repository.get_data_access_by_id(access_id)
            if not result:
                raise NotFoundError(f"Data access with ID {access_id} not found", "DATA_ACCESS_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_data_access_by_id", "read")
            raise
    
    async def get_data_access_by_user(self, user_id: UUID, limit: int = 100) -> List[DataAccess]:
        """Get data access records for a user."""
        try:
            return await self.repository.get_data_access_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_data_access_by_user", "read")
            return []

