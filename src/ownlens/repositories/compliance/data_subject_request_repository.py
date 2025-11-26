"""
OwnLens - Compliance Domain: Data Subject Request Repository

Repository for compliance_data_subject_requests table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.data_subject_request import DataSubjectRequest, DataSubjectRequestCreate, DataSubjectRequestUpdate

logger = logging.getLogger(__name__)


class DataSubjectRequestRepository(BaseRepository[DataSubjectRequest]):
    """Repository for compliance_data_subject_requests table."""

    def __init__(self, connection_manager):
        """Initialize the data subject request repository."""
        super().__init__(connection_manager, table_name="compliance_data_subject_requests")
        self.data_subject_requests_table = "compliance_data_subject_requests"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "request_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_data_subject_requests"

    # ============================================================================
    # DATA SUBJECT REQUEST OPERATIONS
    # ============================================================================

    async def create_data_subject_request(self, request_data: Union[DataSubjectRequestCreate, Dict[str, Any]]) -> Optional[DataSubjectRequest]:
        """
        Create a new data subject request record.
        
        Args:
            request_data: Data subject request creation data (Pydantic model or dict)
            
        Returns:
            Created data subject request with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(request_data, dict):
                request_data = DataSubjectRequestCreate.model_validate(request_data)
            
            data = request_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.data_subject_requests_table)
            if result:
                result = self._convert_json_to_lists(result)
                return DataSubjectRequest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating data subject request: {e}")
            return None

    async def get_data_subject_request_by_id(self, request_id: UUID) -> Optional[DataSubjectRequest]:
        """Get data subject request by ID."""
        try:
            result = await self.get_by_id(request_id)
            if result:
                result = self._convert_json_to_lists(result)
                return DataSubjectRequest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting data subject request by ID {request_id}: {e}")
            return None

    async def get_requests_by_user(self, user_id: UUID, request_type: Optional[str] = None) -> List[DataSubjectRequest]:
        """Get all requests for a user."""
        try:
            filters = {"user_id": user_id}
            if request_type:
                filters["request_type"] = request_type
            
            results = await self.get_all(filters=filters, order_by="request_date")
            return [DataSubjectRequest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting requests by user {user_id}: {e}")
            return []

    async def get_requests_by_status(self, status: str, limit: int = 100) -> List[DataSubjectRequest]:
        """Get requests by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="request_date",
                limit=limit
            )
            return [DataSubjectRequest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting requests by status {status}: {e}")
            return []

    async def get_pending_requests(self, limit: int = 100) -> List[DataSubjectRequest]:
        """Get pending requests."""
        try:
            results = await self.get_all(
                filters={"status": "pending"},
                order_by="request_date",
                limit=limit
            )
            return [DataSubjectRequest(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting pending requests: {e}")
            return []

    async def update_data_subject_request(self, request_id: UUID, request_data: DataSubjectRequestUpdate) -> Optional[DataSubjectRequest]:
        """Update data subject request data."""
        try:
            data = request_data.model_dump(exclude_unset=True)
            
            result = await self.update(request_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return DataSubjectRequest(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating data subject request {request_id}: {e}")
            return None

    async def delete_data_subject_request(self, request_id: UUID) -> bool:
        """Delete data subject request and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(request_id)
            
        except Exception as e:
            logger.error(f"Error deleting data subject request {request_id}: {e}")
            return False

