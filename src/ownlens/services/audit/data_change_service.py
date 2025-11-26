"""
OwnLens - Audit Domain: Data Change Service

Service for data change tracking.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.audit import DataChangeRepository
from ...models.audit.data_change import DataChange, DataChangeCreate, DataChangeUpdate

logger = logging.getLogger(__name__)


class DataChangeService(BaseService[DataChange, DataChangeCreate, DataChangeUpdate, DataChange]):
    """Service for data change tracking."""
    
    def __init__(self, repository: DataChangeRepository, service_name: str = None):
        """Initialize the data change service."""
        super().__init__(repository, service_name or "DataChangeService")
        self.repository: DataChangeRepository = repository
    
    def get_model_class(self):
        """Get the DataChange model class."""
        return DataChange
    
    def get_create_model_class(self):
        """Get the DataChangeCreate model class."""
        return DataChangeCreate
    
    def get_update_model_class(self):
        """Get the DataChangeUpdate model class."""
        return DataChangeUpdate
    
    def get_in_db_model_class(self):
        """Get the DataChange model class."""
        return DataChange
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for data change operations."""
        return True  # Data changes are typically append-only
    
    async def create_data_change(self, change_data: DataChangeCreate) -> DataChange:
        """Create a new data change entry."""
        try:
            await self.validate_input(change_data, "create")
            await self.validate_business_rules(change_data, "create")
            
            result = await self.repository.create_data_change(change_data)
            if not result:
                raise NotFoundError("Failed to create data change", "CREATE_FAILED")
            
            self.log_operation("create_data_change", {"change_id": str(result.change_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_data_change", "create")
            raise
    
    async def get_data_changes_by_table(self, table_name: str, limit: int = 100) -> List[DataChange]:
        """Get data changes for a table."""
        try:
            return await self.repository.get_data_changes_by_table(table_name, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_data_changes_by_table", "read")
            return []
    
    async def get_data_changes_by_record(self, table_name: str, record_id: UUID) -> List[DataChange]:
        """Get all changes for a specific record."""
        try:
            return await self.repository.get_data_changes_by_record(table_name, record_id)
        except Exception as e:
            await self.handle_error(e, "get_data_changes_by_record", "read")
            return []

