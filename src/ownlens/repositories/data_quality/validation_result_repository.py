"""
OwnLens - Data Quality Domain: Validation Result Repository

Repository for data_validation_results table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.data_quality.validation_result import ValidationResult, ValidationResultCreate, ValidationResultUpdate

logger = logging.getLogger(__name__)


class ValidationResultRepository(BaseRepository[ValidationResult]):
    """Repository for data_validation_results table."""

    def __init__(self, connection_manager):
        """Initialize the validation result repository."""
        super().__init__(connection_manager, table_name="data_validation_results")
        self.validation_results_table = "data_validation_results"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "validation_id"  # Database uses validation_id, not result_id

    def get_table_name(self) -> str:
        """Get the table name."""
        return "data_validation_results"

    # ============================================================================
    # VALIDATION RESULT OPERATIONS
    # ============================================================================

    async def create_validation_result(self, result_data: ValidationResultCreate) -> Optional[ValidationResult]:
        """
        Create a new validation result record.
        
        Args:
            result_data: Validation result creation data
            
        Returns:
            Created validation result with generated ID, or None if creation failed
        """
        try:
            data = result_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.validation_results_table)
            if result:
                result = self._convert_json_to_lists(result)
                # Map result_id to validation_id if present (database uses validation_id, but some code might use result_id)
                if 'result_id' in result and 'validation_id' not in result:
                    result['validation_id'] = result['result_id']
                    result.pop('result_id', None)
                return ValidationResult(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating validation result: {e}")
            return None

    async def get_validation_result_by_id(self, result_id: UUID) -> Optional[ValidationResult]:
        """Get validation result by ID."""
        try:
            result = await self.get_by_id(result_id)
            if result:
                result = self._convert_json_to_lists(result)
                return ValidationResult(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting validation result by ID {result_id}: {e}")
            return None

    async def get_validation_results_by_table(self, table_name: str, limit: int = 100, offset: int = 0) -> List[ValidationResult]:
        """Get all validation results for a table."""
        try:
            results = await self.get_all(
                filters={"table_name": table_name},
                order_by="validation_timestamp",
                limit=limit,
                offset=offset
            )
            return [ValidationResult(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting validation results by table {table_name}: {e}")
            return []

    async def get_validation_results_by_status(self, status: str, limit: int = 100) -> List[ValidationResult]:
        """Get validation results by status."""
        try:
            results = await self.get_all(
                filters={"validation_status": status},
                order_by="validation_timestamp",
                limit=limit
            )
            return [ValidationResult(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting validation results by status {status}: {e}")
            return []

    async def get_validation_results_by_date_range(self, start_date: date, end_date: date, table_name: Optional[str] = None) -> List[ValidationResult]:
        """Get validation results within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.validation_results_table)
                .where_gte("validation_timestamp", start_date)
                .where_lte("validation_timestamp", end_date)
            )
            
            if table_name:
                query_builder = query_builder.where_equals("table_name", table_name)
            
            query, params = query_builder.order_by("validation_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [ValidationResult(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting validation results by date range: {e}")
            return []

    async def update_validation_result(self, result_id: UUID, result_data: ValidationResultUpdate) -> Optional[ValidationResult]:
        """Update validation result data."""
        try:
            data = result_data.model_dump(exclude_unset=True)
            
            result = await self.update(result_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return ValidationResult(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating validation result {result_id}: {e}")
            return None

    async def delete_validation_result(self, result_id: UUID) -> bool:
        """Delete validation result and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(result_id)
            
        except Exception as e:
            logger.error(f"Error deleting validation result {result_id}: {e}")
            return False

