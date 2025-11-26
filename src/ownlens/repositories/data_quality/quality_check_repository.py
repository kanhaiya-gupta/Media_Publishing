"""
OwnLens - Data Quality Domain: Quality Check Repository

Repository for data_quality_checks table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.data_quality.quality_check import QualityCheck, QualityCheckCreate, QualityCheckUpdate

logger = logging.getLogger(__name__)


class QualityCheckRepository(BaseRepository[QualityCheck]):
    """Repository for data_quality_checks table."""

    def __init__(self, connection_manager):
        """Initialize the quality check repository."""
        super().__init__(connection_manager, table_name="data_quality_checks")
        self.quality_checks_table = "data_quality_checks"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "check_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "data_quality_checks"

    # ============================================================================
    # QUALITY CHECK OPERATIONS
    # ============================================================================

    async def create_quality_check(self, check_data: QualityCheckCreate) -> Optional[QualityCheck]:
        """
        Create a new quality check record.
        
        Args:
            check_data: Quality check creation data
            
        Returns:
            Created quality check with generated ID, or None if creation failed
        """
        try:
            data = check_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.quality_checks_table)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityCheck(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating quality check: {e}")
            return None

    async def get_quality_check_by_id(self, check_id: UUID) -> Optional[QualityCheck]:
        """Get quality check by ID."""
        try:
            result = await self.get_by_id(check_id)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityCheck(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting quality check by ID {check_id}: {e}")
            return None

    async def get_quality_checks_by_rule(self, rule_id: UUID, limit: int = 100, offset: int = 0) -> List[QualityCheck]:
        """Get all quality checks for a rule."""
        try:
            results = await self.get_all(
                filters={"rule_id": rule_id},
                order_by="check_timestamp",
                limit=limit,
                offset=offset
            )
            return [QualityCheck(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality checks by rule {rule_id}: {e}")
            return []

    async def get_quality_checks_by_status(self, status: str, limit: int = 100) -> List[QualityCheck]:
        """Get quality checks by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="check_timestamp",
                limit=limit
            )
            return [QualityCheck(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality checks by status {status}: {e}")
            return []

    async def get_quality_checks_by_date_range(self, start_date: date, end_date: date, rule_id: Optional[UUID] = None) -> List[QualityCheck]:
        """Get quality checks within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.quality_checks_table)
                .where_gte("check_timestamp", start_date)
                .where_lte("check_timestamp", end_date)
            )
            
            if rule_id:
                query_builder = query_builder.where_equals("rule_id", rule_id)
            
            query, params = query_builder.order_by("check_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [QualityCheck(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting quality checks by date range: {e}")
            return []

    async def update_quality_check(self, check_id: UUID, check_data: QualityCheckUpdate) -> Optional[QualityCheck]:
        """Update quality check data."""
        try:
            data = check_data.model_dump(exclude_unset=True)
            
            result = await self.update(check_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityCheck(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating quality check {check_id}: {e}")
            return None

    async def delete_quality_check(self, check_id: UUID) -> bool:
        """Delete quality check and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(check_id)
            
        except Exception as e:
            logger.error(f"Error deleting quality check {check_id}: {e}")
            return False

