"""
OwnLens - Company Domain: Communications Analytics Repository

Repository for company_communications_analytics table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseRepository
from ...models.company.communications_analytics import CommunicationsAnalytics, CommunicationsAnalyticsCreate, CommunicationsAnalyticsUpdate

logger = logging.getLogger(__name__)


class CommunicationsAnalyticsRepository(BaseRepository[CommunicationsAnalytics]):
    """Repository for company_communications_analytics table."""

    def __init__(self, connection_manager):
        """Initialize the communications analytics repository."""
        super().__init__(connection_manager, table_name="company_communications_analytics")
        self.communications_analytics_table = "company_communications_analytics"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "analytics_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "company_communications_analytics"

    # ============================================================================
    # COMMUNICATIONS ANALYTICS OPERATIONS
    # ============================================================================

    async def create_communications_analytics(self, analytics_data: Union[CommunicationsAnalyticsCreate, Dict[str, Any]]) -> Optional[CommunicationsAnalytics]:
        """
        Create a new communications analytics record.
        
        Args:
            analytics_data: Communications analytics creation data (dict or CommunicationsAnalyticsCreate model)
            
        Returns:
            Created communications analytics with generated ID, or None if creation failed
        """
        try:
            if isinstance(analytics_data, dict):
                analytics_data = CommunicationsAnalyticsCreate.model_validate(analytics_data)
            data = analytics_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.communications_analytics_table)
            if result:
                result = self._convert_json_to_lists(result)
                return CommunicationsAnalytics(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating communications analytics: {e}")
            return None

    async def get_communications_analytics_by_id(self, analytics_id: UUID) -> Optional[CommunicationsAnalytics]:
        """Get communications analytics by ID."""
        try:
            result = await self.get_by_id(analytics_id)
            if result:
                result = self._convert_json_to_lists(result)
                return CommunicationsAnalytics(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting communications analytics by ID {analytics_id}: {e}")
            return None

    async def get_communications_analytics_by_company(self, company_id: UUID, limit: int = 100, offset: int = 0) -> List[CommunicationsAnalytics]:
        """Get all communications analytics for a company."""
        try:
            results = await self.get_all(
                filters={"company_id": company_id},
                order_by="analytics_date",
                limit=limit,
                offset=offset
            )
            return [CommunicationsAnalytics(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting communications analytics by company {company_id}: {e}")
            return []

    async def get_communications_analytics_by_date(self, analytics_date: date, company_id: Optional[UUID] = None) -> List[CommunicationsAnalytics]:
        """Get communications analytics for a specific date."""
        try:
            filters = {"analytics_date": analytics_date}
            if company_id:
                filters["company_id"] = company_id
            
            results = await self.get_all(filters=filters, order_by="total_content_views")
            return [CommunicationsAnalytics(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting communications analytics by date {analytics_date}: {e}")
            return []

    async def update_communications_analytics(self, analytics_id: UUID, analytics_data: CommunicationsAnalyticsUpdate) -> Optional[CommunicationsAnalytics]:
        """Update communications analytics data."""
        try:
            data = analytics_data.model_dump(exclude_unset=True)
            
            result = await self.update(analytics_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return CommunicationsAnalytics(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating communications analytics {analytics_id}: {e}")
            return None

    async def delete_communications_analytics(self, analytics_id: UUID) -> bool:
        """Delete communications analytics and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(analytics_id)
            
        except Exception as e:
            logger.error(f"Error deleting communications analytics {analytics_id}: {e}")
            return False

