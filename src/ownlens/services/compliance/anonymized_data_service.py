"""
OwnLens - Compliance Domain: Anonymized Data Service

Service for anonymized data management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import AnonymizedDataRepository
from ...models.compliance.anonymized_data import AnonymizedData, AnonymizedDataCreate, AnonymizedDataUpdate

logger = logging.getLogger(__name__)


class AnonymizedDataService(BaseService[AnonymizedData, AnonymizedDataCreate, AnonymizedDataUpdate, AnonymizedData]):
    """Service for anonymized data management."""
    
    def __init__(self, repository: AnonymizedDataRepository, service_name: str = None):
        """Initialize the anonymized data service."""
        super().__init__(repository, service_name or "AnonymizedDataService")
        self.repository: AnonymizedDataRepository = repository
    
    def get_model_class(self):
        """Get the AnonymizedData model class."""
        return AnonymizedData
    
    def get_create_model_class(self):
        """Get the AnonymizedDataCreate model class."""
        return AnonymizedDataCreate
    
    def get_update_model_class(self):
        """Get the AnonymizedDataUpdate model class."""
        return AnonymizedDataUpdate
    
    def get_in_db_model_class(self):
        """Get the AnonymizedData model class."""
        return AnonymizedData
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for anonymized data operations."""
        try:
            if operation == "create":
                # Validate original table name
                if hasattr(data, 'original_table_name'):
                    table = data.original_table_name
                else:
                    table = data.get('original_table_name') if isinstance(data, dict) else None
                
                if not table or len(table.strip()) == 0:
                    raise ValidationError(
                        "Original table name is required",
                        "INVALID_TABLE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_anonymized_data(self, data_record: AnonymizedDataCreate) -> AnonymizedData:
        """Create a new anonymized data record."""
        try:
            await self.validate_input(data_record, "create")
            await self.validate_business_rules(data_record, "create")
            
            result = await self.repository.create_anonymized_data(data_record)
            if not result:
                raise NotFoundError("Failed to create anonymized data record", "CREATE_FAILED")
            
            self.log_operation("create_anonymized_data", {"anonymization_id": str(result.anonymization_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_anonymized_data", "create")
            raise
    
    async def get_anonymized_data_by_id(self, anonymization_id: UUID) -> AnonymizedData:
        """Get anonymized data by ID."""
        try:
            result = await self.repository.get_anonymized_data_by_id(anonymization_id)
            if not result:
                raise NotFoundError(f"Anonymized data with ID {anonymization_id} not found", "ANONYMIZED_DATA_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_anonymized_data_by_id", "read")
            raise
    
    async def get_anonymized_data_by_request(self, request_id: UUID, limit: int = 100) -> List[AnonymizedData]:
        """Get anonymized data by request ID."""
        try:
            return await self.repository.get_anonymized_data_by_request(request_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_anonymized_data_by_request", "read")
            return []

