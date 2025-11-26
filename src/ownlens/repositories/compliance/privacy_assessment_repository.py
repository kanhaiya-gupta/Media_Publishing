"""
OwnLens - Compliance Domain: Privacy Assessment Repository

Repository for compliance_privacy_assessments table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.privacy_assessment import PrivacyAssessment, PrivacyAssessmentCreate, PrivacyAssessmentUpdate

logger = logging.getLogger(__name__)


class PrivacyAssessmentRepository(BaseRepository[PrivacyAssessment]):
    """Repository for compliance_privacy_assessments table."""

    def __init__(self, connection_manager):
        """Initialize the privacy assessment repository."""
        super().__init__(connection_manager, table_name="compliance_privacy_assessments")
        self.privacy_assessments_table = "compliance_privacy_assessments"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "assessment_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_privacy_assessments"

    # ============================================================================
    # PRIVACY ASSESSMENT OPERATIONS
    # ============================================================================

    async def create_privacy_assessment(self, assessment_data: Union[PrivacyAssessmentCreate, Dict[str, Any]]) -> Optional[PrivacyAssessment]:
        """
        Create a new privacy assessment record.
        
        Args:
            assessment_data: Privacy assessment creation data (Pydantic model or dict)
            
        Returns:
            Created privacy assessment with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(assessment_data, dict):
                assessment_data = PrivacyAssessmentCreate.model_validate(assessment_data)
            
            data = assessment_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.privacy_assessments_table)
            if result:
                result = self._convert_json_to_lists(result)
                return PrivacyAssessment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating privacy assessment: {e}")
            return None

    async def get_privacy_assessment_by_id(self, assessment_id: UUID) -> Optional[PrivacyAssessment]:
        """Get privacy assessment by ID."""
        try:
            result = await self.get_by_id(assessment_id)
            if result:
                result = self._convert_json_to_lists(result)
                return PrivacyAssessment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting privacy assessment by ID {assessment_id}: {e}")
            return None

    async def get_assessments_by_status(self, status: str, limit: int = 100) -> List[PrivacyAssessment]:
        """Get assessments by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="assessment_date",
                limit=limit
            )
            return [PrivacyAssessment(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting assessments by status {status}: {e}")
            return []

    async def get_assessments_by_date(self, assessment_date: date) -> List[PrivacyAssessment]:
        """Get assessments for a specific date."""
        try:
            results = await self.get_all(
                filters={"assessment_date": assessment_date},
                order_by="assessment_date"
            )
            return [PrivacyAssessment(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting assessments by date {assessment_date}: {e}")
            return []

    async def update_privacy_assessment(self, assessment_id: UUID, assessment_data: PrivacyAssessmentUpdate) -> Optional[PrivacyAssessment]:
        """Update privacy assessment data."""
        try:
            data = assessment_data.model_dump(exclude_unset=True)
            
            result = await self.update(assessment_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return PrivacyAssessment(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating privacy assessment {assessment_id}: {e}")
            return None

    async def delete_privacy_assessment(self, assessment_id: UUID) -> bool:
        """Delete privacy assessment and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(assessment_id)
            
        except Exception as e:
            logger.error(f"Error deleting privacy assessment {assessment_id}: {e}")
            return False

