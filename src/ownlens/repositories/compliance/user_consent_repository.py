"""
OwnLens - Compliance Domain: User Consent Repository

Repository for compliance_user_consent table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.compliance.user_consent import UserConsent, UserConsentCreate, UserConsentUpdate

logger = logging.getLogger(__name__)


class UserConsentRepository(BaseRepository[UserConsent]):
    """Repository for compliance_user_consent table."""

    def __init__(self, connection_manager):
        """Initialize the user consent repository."""
        super().__init__(connection_manager, table_name="compliance_user_consent")
        self.user_consent_table = "compliance_user_consent"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "consent_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "compliance_user_consent"

    # ============================================================================
    # USER CONSENT OPERATIONS
    # ============================================================================

    async def create_user_consent(self, consent_data: Union[UserConsentCreate, Dict[str, Any]]) -> Optional[UserConsent]:
        """
        Create a new user consent record.
        
        Args:
            consent_data: User consent creation data (Pydantic model or dict)
            
        Returns:
            Created user consent with generated ID, or None if creation failed
        """
        try:
            # Handle both Pydantic model and dict
            if isinstance(consent_data, dict):
                consent_data = UserConsentCreate.model_validate(consent_data)
            
            data = consent_data.model_dump(exclude_unset=True)
            
            result = await self.create(data, table_name=self.user_consent_table)
            if result:
                result = self._convert_json_to_lists(result)
                return UserConsent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating user consent: {e}")
            return None

    async def get_user_consent_by_id(self, consent_id: UUID) -> Optional[UserConsent]:
        """Get user consent by ID."""
        try:
            result = await self.get_by_id(consent_id)
            if result:
                result = self._convert_json_to_lists(result)
                return UserConsent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting user consent by ID {consent_id}: {e}")
            return None

    async def get_consents_by_user(self, user_id: UUID, purpose: Optional[str] = None) -> List[UserConsent]:
        """Get all consents for a user."""
        try:
            filters = {"user_id": user_id}
            if purpose:
                filters["purpose"] = purpose
            
            results = await self.get_all(filters=filters, order_by="consent_date")
            return [UserConsent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting consents by user {user_id}: {e}")
            return []

    async def get_active_consents(self, user_id: UUID) -> List[UserConsent]:
        """Get active consents for a user."""
        try:
            results = await self.get_all(
                filters={"user_id": user_id, "is_active": True},
                order_by="consent_date"
            )
            return [UserConsent(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting active consents for user {user_id}: {e}")
            return []

    async def update_user_consent(self, consent_id: UUID, consent_data: UserConsentUpdate) -> Optional[UserConsent]:
        """Update user consent data."""
        try:
            data = consent_data.model_dump(exclude_unset=True)
            
            result = await self.update(consent_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return UserConsent(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating user consent {consent_id}: {e}")
            return None

    async def delete_user_consent(self, consent_id: UUID) -> bool:
        """Delete user consent and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(consent_id)
            
        except Exception as e:
            logger.error(f"Error deleting user consent {consent_id}: {e}")
            return False

