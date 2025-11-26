"""
OwnLens - Audit Domain: Data Lineage Service

Service for data lineage management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import DataLineageRepository
from ...models.audit.data_lineage import DataLineage, DataLineageCreate, DataLineageUpdate

logger = logging.getLogger(__name__)


class DataLineageService(BaseService[DataLineage, DataLineageCreate, DataLineageUpdate, DataLineage]):
    """Service for data lineage management."""
    
    def __init__(self, repository: DataLineageRepository, service_name: str = None):
        """Initialize the data lineage service."""
        super().__init__(repository, service_name or "DataLineageService")
        self.repository: DataLineageRepository = repository
    
    def get_model_class(self):
        """Get the DataLineage model class."""
        return DataLineage
    
    def get_create_model_class(self):
        """Get the DataLineageCreate model class."""
        return DataLineageCreate
    
    def get_update_model_class(self):
        """Get the DataLineageUpdate model class."""
        return DataLineageUpdate
    
    def get_in_db_model_class(self):
        """Get the DataLineage model class."""
        return DataLineage
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for data lineage operations."""
        try:
            if operation == "create":
                # Validate source and destination identifiers
                if hasattr(data, 'source_identifier') and hasattr(data, 'destination_identifier'):
                    source = data.source_identifier
                    dest = data.destination_identifier
                else:
                    source = data.get('source_identifier') if isinstance(data, dict) else None
                    dest = data.get('destination_identifier') if isinstance(data, dict) else None
                
                if not source or len(source.strip()) == 0:
                    raise ValidationError(
                        "Source identifier is required",
                        "INVALID_SOURCE"
                    )
                
                if not dest or len(dest.strip()) == 0:
                    raise ValidationError(
                        "Destination identifier is required",
                        "INVALID_DESTINATION"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_data_lineage(self, lineage_data: DataLineageCreate) -> DataLineage:
        """Create a new data lineage record."""
        try:
            await self.validate_input(lineage_data, "create")
            await self.validate_business_rules(lineage_data, "create")
            
            result = await self.repository.create_data_lineage(lineage_data)
            if not result:
                raise NotFoundError("Failed to create data lineage", "CREATE_FAILED")
            
            self.log_operation("create_data_lineage", {"lineage_id": str(result.lineage_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_data_lineage", "create")
            raise
    
    async def get_data_lineage_by_id(self, lineage_id: UUID) -> DataLineage:
        """Get data lineage by ID."""
        try:
            result = await self.repository.get_data_lineage_by_id(lineage_id)
            if not result:
                raise NotFoundError(f"Data lineage with ID {lineage_id} not found", "DATA_LINEAGE_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_data_lineage_by_id", "read")
            raise
    
    async def get_data_lineage_by_source(self, source_type: str, source_identifier: str, limit: int = 100) -> List[DataLineage]:
        """Get data lineage by source."""
        try:
            return await self.repository.get_data_lineage_by_source(source_type, source_identifier, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_data_lineage_by_source", "read")
            return []

