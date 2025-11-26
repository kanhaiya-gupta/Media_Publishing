"""
OwnLens - Compliance Domain: Data Subject Request Service

Service for data subject request management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import DataSubjectRequestRepository
from ...models.compliance.data_subject_request import DataSubjectRequest, DataSubjectRequestCreate, DataSubjectRequestUpdate

logger = logging.getLogger(__name__)


class DataSubjectRequestService(BaseService[DataSubjectRequest, DataSubjectRequestCreate, DataSubjectRequestUpdate, DataSubjectRequest]):
    """Service for data subject request management."""
    
    def __init__(self, repository: DataSubjectRequestRepository, service_name: str = None):
        """Initialize the data subject request service."""
        super().__init__(repository, service_name or "DataSubjectRequestService")
        self.repository: DataSubjectRequestRepository = repository
    
    def get_model_class(self):
        """Get the DataSubjectRequest model class."""
        return DataSubjectRequest
    
    def get_create_model_class(self):
        """Get the DataSubjectRequestCreate model class."""
        return DataSubjectRequestCreate
    
    def get_update_model_class(self):
        """Get the DataSubjectRequestUpdate model class."""
        return DataSubjectRequestUpdate
    
    def get_in_db_model_class(self):
        """Get the DataSubjectRequest model class."""
        return DataSubjectRequest
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for data subject request operations."""
        try:
            # Validate request type
            if hasattr(data, 'request_type'):
                request_type = data.request_type
            else:
                request_type = data.get('request_type') if isinstance(data, dict) else None
            
            if request_type and request_type not in ['access', 'rectification', 'erasure', 'portability', 'objection', 'restriction']:
                raise ValidationError(
                    "Invalid request type",
                    "INVALID_REQUEST_TYPE"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_data_subject_request(self, request_data: DataSubjectRequestCreate) -> DataSubjectRequest:
        """Create a new data subject request."""
        try:
            await self.validate_input(request_data, "create")
            await self.validate_business_rules(request_data, "create")
            
            result = await self.repository.create_data_subject_request(request_data)
            if not result:
                raise NotFoundError("Failed to create data subject request", "CREATE_FAILED")
            
            self.log_operation("create_data_subject_request", {"request_id": str(result.request_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_data_subject_request", "create")
            raise
    
    async def get_data_subject_request_by_id(self, request_id: UUID) -> DataSubjectRequest:
        """Get data subject request by ID."""
        try:
            result = await self.repository.get_data_subject_request_by_id(request_id)
            if not result:
                raise NotFoundError(f"Data subject request with ID {request_id} not found", "DATA_SUBJECT_REQUEST_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_data_subject_request_by_id", "read")
            raise
    
    async def get_pending_requests(self, limit: int = 100) -> List[DataSubjectRequest]:
        """Get pending data subject requests."""
        try:
            return await self.repository.get_pending_requests(limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_pending_requests", "read")
            return []

