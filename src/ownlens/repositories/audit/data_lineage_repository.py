"""
OwnLens - Audit Domain: Data Lineage Repository

Repository for audit_data_lineage table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.audit.data_lineage import DataLineage, DataLineageCreate, DataLineageUpdate

logger = logging.getLogger(__name__)


class DataLineageRepository(BaseRepository[DataLineage]):
    """Repository for audit_data_lineage table."""

    def __init__(self, connection_manager):
        """Initialize the data lineage repository."""
        super().__init__(connection_manager, table_name="audit_data_lineage")
        self.data_lineage_table = "audit_data_lineage"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "lineage_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "audit_data_lineage"

    # ============================================================================
    # DATA LINEAGE OPERATIONS
    # ============================================================================

    async def create_data_lineage(self, lineage_data: Union[DataLineageCreate, Dict[str, Any]]) -> Optional[DataLineage]:
        """
        Create a new data lineage record.
        
        Args:
            lineage_data: Data lineage creation data (Pydantic model or dict)
            
        Returns:
            Created data lineage with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(lineage_data, table_name=self.data_lineage_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DataLineage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating data lineage: {e}")
            return None

    async def get_data_lineage_by_id(self, lineage_id: UUID) -> Optional[DataLineage]:
        """Get data lineage by ID."""
        try:
            result = await self.get_by_id(lineage_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DataLineage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting data lineage by ID {lineage_id}: {e}")
            return None

    async def get_lineage_by_source(self, source_table: str, source_record_id: UUID) -> List[DataLineage]:
        """Get lineage for a source record."""
        try:
            results = await self.get_all(
                filters={"source_table": source_table, "source_record_id": source_record_id},
                order_by="lineage_date"
            )
            return [DataLineage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting lineage by source {source_table}/{source_record_id}: {e}")
            return []

    async def get_lineage_by_target(self, target_table: str, target_record_id: UUID) -> List[DataLineage]:
        """Get lineage for a target record."""
        try:
            results = await self.get_all(
                filters={"target_table": target_table, "target_record_id": target_record_id},
                order_by="lineage_date"
            )
            return [DataLineage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting lineage by target {target_table}/{target_record_id}: {e}")
            return []

    async def get_lineage_by_transformation(self, transformation_type: str, limit: int = 100) -> List[DataLineage]:
        """Get lineage by transformation type."""
        try:
            results = await self.get_all(
                filters={"transformation_type": transformation_type},
                order_by="lineage_date",
                limit=limit
            )
            return [DataLineage(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting lineage by transformation {transformation_type}: {e}")
            return []

    async def update_data_lineage(self, lineage_id: UUID, lineage_data: DataLineageUpdate) -> Optional[DataLineage]:
        """Update data lineage data."""
        try:
            data = lineage_data.model_dump(exclude_unset=True)
            
            result = await self.update(lineage_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DataLineage(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating data lineage {lineage_id}: {e}")
            return None

    async def delete_data_lineage(self, lineage_id: UUID) -> bool:
        """Delete data lineage and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(lineage_id)
            
        except Exception as e:
            logger.error(f"Error deleting data lineage {lineage_id}: {e}")
            return False

