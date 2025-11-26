"""
OwnLens - Data Quality Domain: Quality Alert Repository

Repository for data_quality_alerts table.
"""

from typing import Dict, List, Optional, Any
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.data_quality.quality_alert import QualityAlert, QualityAlertCreate, QualityAlertUpdate

logger = logging.getLogger(__name__)


class QualityAlertRepository(BaseRepository[QualityAlert]):
    """Repository for data_quality_alerts table."""

    def __init__(self, connection_manager):
        """Initialize the quality alert repository."""
        super().__init__(connection_manager, table_name="data_quality_alerts")
        self.quality_alerts_table = "data_quality_alerts"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "alert_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "data_quality_alerts"

    # ============================================================================
    # QUALITY ALERT OPERATIONS
    # ============================================================================

    async def create_quality_alert(self, alert_data: QualityAlertCreate) -> Optional[QualityAlert]:
        """
        Create a new quality alert record.
        
        Args:
            alert_data: Quality alert creation data
            
        Returns:
            Created quality alert with generated ID, or None if creation failed
        """
        try:
            data = alert_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.quality_alerts_table)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityAlert(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating quality alert: {e}")
            return None

    async def get_quality_alert_by_id(self, alert_id: UUID) -> Optional[QualityAlert]:
        """Get quality alert by ID."""
        try:
            result = await self.get_by_id(alert_id)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityAlert(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting quality alert by ID {alert_id}: {e}")
            return None

    async def get_quality_alerts_by_rule(self, rule_id: UUID, limit: int = 100) -> List[QualityAlert]:
        """Get all quality alerts for a rule."""
        try:
            results = await self.get_all(
                filters={"rule_id": rule_id},
                order_by="alert_timestamp",
                limit=limit
            )
            return [QualityAlert(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality alerts by rule {rule_id}: {e}")
            return []

    async def get_quality_alerts_by_severity(self, severity: str, limit: int = 100) -> List[QualityAlert]:
        """Get quality alerts by severity."""
        try:
            results = await self.get_all(
                filters={"severity": severity},
                order_by="alert_timestamp",
                limit=limit
            )
            return [QualityAlert(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting quality alerts by severity {severity}: {e}")
            return []

    async def get_active_quality_alerts(self, limit: int = 100) -> List[QualityAlert]:
        """Get active quality alerts."""
        try:
            results = await self.get_all(
                filters={"status": "active"},
                order_by="alert_timestamp",
                limit=limit
            )
            return [QualityAlert(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active quality alerts: {e}")
            return []

    async def get_quality_alerts_by_date_range(self, start_date: date, end_date: date, rule_id: Optional[UUID] = None) -> List[QualityAlert]:
        """Get quality alerts within a date range."""
        try:
            from ..shared.query_builder import select_all
            
            query_builder = (
                select_all()
                .table(self.quality_alerts_table)
                .where_gte("alert_timestamp", start_date)
                .where_lte("alert_timestamp", end_date)
            )
            
            if rule_id:
                query_builder = query_builder.where_equals("rule_id", rule_id)
            
            query, params = query_builder.order_by("alert_timestamp").build()
            
            result = await self.connection_manager.execute_query(query, params)
            rows = result.get("rows", []) if result else []
            return [QualityAlert(**self._convert_json_to_lists(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting quality alerts by date range: {e}")
            return []

    async def update_quality_alert(self, alert_id: UUID, alert_data: QualityAlertUpdate) -> Optional[QualityAlert]:
        """Update quality alert data."""
        try:
            data = alert_data.model_dump(exclude_unset=True)
            
            result = await self.update(alert_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return QualityAlert(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating quality alert {alert_id}: {e}")
            return None

    async def delete_quality_alert(self, alert_id: UUID) -> bool:
        """Delete quality alert and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(alert_id)
            
        except Exception as e:
            logger.error(f"Error deleting quality alert {alert_id}: {e}")
            return False

