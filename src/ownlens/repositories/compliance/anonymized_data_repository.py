"""
OwnLens - Compliance Domain: Anonymized Data Repository

Repository for compliance_anonymized_data table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.anonymized_data import AnonymizedData, AnonymizedDataCreate, AnonymizedDataUpdate

logger = logging.getLogger(__name__)


class AnonymizedDataRepository(BaseRepository[AnonymizedData]):
    """Repository for compliance_anonymized_data table."""

    def __init__(self, connection_manager):
        """Initialize the anonymized data repository."""
        super().__init__(connection_manager, table_name="compliance_anonymized_data")
        self.anonymized_data_table = "compliance_anonymized_data"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "anonymization_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_anonymized_data"

    # ============================================================================
    # ANONYMIZED DATA OPERATIONS
    # ============================================================================

    async def create_anonymized_data(self, anonymized_data: Union[AnonymizedDataCreate, Dict[str, Any]]) -> Optional[AnonymizedData]:
        """
        Create a new anonymized data record.
        
        Args:
            anonymized_data: Anonymized data creation data (Pydantic model or dict)
            
        Returns:
            Created anonymized data with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(anonymized_data, dict):
                anonymized_data = AnonymizedDataCreate.model_validate(anonymized_data)
            
            data = anonymized_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.anonymized_data_table)
            if result:
                result = self._convert_json_to_lists(result)
                return AnonymizedData(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating anonymized data: {e}")
            return None

    async def get_anonymized_data_by_id(self, anonymization_id: UUID) -> Optional[AnonymizedData]:
        """Get anonymized data by ID."""
        try:
            result = await self.get_by_id(anonymization_id)
            if result:
                result = self._convert_json_to_lists(result)
                return AnonymizedData(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting anonymized data by ID {anonymization_id}: {e}")
            return None

    async def get_anonymized_data_by_user(self, user_id: UUID) -> List[AnonymizedData]:
        """Get all anonymized data for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id},
                order_by="anonymization_date"
            )
            return [AnonymizedData(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting anonymized data by user {user_id}: {e}")
            return []

    async def get_anonymized_data_by_table(self, table_name: str, limit: int = 100) -> List[AnonymizedData]:
        """Get anonymized data for a specific table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="anonymization_date",
                limit=limit
            )
            return [AnonymizedData(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting anonymized data by table {table_name}: {e}")
            return []

    async def update_anonymized_data(self, anonymization_id: UUID, anonymized_data: AnonymizedDataUpdate) -> Optional[AnonymizedData]:
        """Update anonymized data."""
        try:
            data = anonymized_data.model_dump(exclude_unset=True)
            
            result = await self.update(anonymization_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return AnonymizedData(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating anonymized data {anonymization_id}: {e}")
            return None

    async def delete_anonymized_data(self, anonymization_id: UUID) -> bool:
        """Delete anonymized data and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(anonymization_id)
            
        except Exception as e:
            logger.error(f"Error deleting anonymized data {anonymization_id}: {e}")
            return False

