"""
OwnLens - Compliance Domain: User Consent Service

Service for user consent management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.compliance import UserConsentRepository
from ...models.compliance.user_consent import UserConsent, UserConsentCreate, UserConsentUpdate

logger = logging.getLogger(__name__)


class UserConsentService(BaseService[UserConsent, UserConsentCreate, UserConsentUpdate, UserConsent]):
    """Service for user consent management."""
    
    def __init__(self, repository: UserConsentRepository, service_name: str = None):
        """Initialize the user consent service."""
        super().__init__(repository, service_name or "UserConsentService")
        self.repository: UserConsentRepository = repository
    
    def get_model_class(self):
        """Get the UserConsent model class."""
        return UserConsent
    
    def get_create_model_class(self):
        """Get the UserConsentCreate model class."""
        return UserConsentCreate
    
    def get_update_model_class(self):
        """Get the UserConsentUpdate model class."""
        return UserConsentUpdate
    
    def get_in_db_model_class(self):
        """Get the UserConsent model class."""
        return UserConsent
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user consent operations."""
        try:
            # Validate withdrawal date is after consent date
            if hasattr(data, 'withdrawal_date') and hasattr(data, 'consent_date'):
                withdrawal = data.withdrawal_date
                consent = data.consent_date
            else:
                withdrawal = data.get('withdrawal_date') if isinstance(data, dict) else None
                consent = data.get('consent_date') if isinstance(data, dict) else None
            
            if withdrawal and consent:
                from datetime import datetime
                if isinstance(withdrawal, str):
                    withdrawal = datetime.fromisoformat(withdrawal.replace('Z', '+00:00'))
                if isinstance(consent, str):
                    consent = datetime.fromisoformat(consent.replace('Z', '+00:00'))
                if withdrawal < consent:
                    raise ValidationError(
                        "Withdrawal date must be after consent date",
                        "INVALID_WITHDRAWAL_DATE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_user_consent(self, consent_data: UserConsentCreate) -> UserConsent:
        """Create new user consent."""
        try:
            await self.validate_input(consent_data, "create")
            await self.validate_business_rules(consent_data, "create")
            
            result = await self.repository.create_user_consent(consent_data)
            if not result:
                raise NotFoundError("Failed to create user consent", "CREATE_FAILED")
            
            self.log_operation("create_user_consent", {"consent_id": str(result.consent_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user_consent", "create")
            raise
    
    async def get_user_consent_by_id(self, consent_id: UUID) -> UserConsent:
        """Get user consent by ID."""
        try:
            result = await self.repository.get_user_consent_by_id(consent_id)
            if not result:
                raise NotFoundError(f"User consent with ID {consent_id} not found", "USER_CONSENT_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_consent_by_id", "read")
            raise
    
    async def get_active_consents(self, user_id: UUID) -> List[UserConsent]:
        """Get active consents for a user."""
        try:
            return await self.repository.get_active_consents(user_id)
        except Exception as e:
            await self.handle_error(e, "get_active_consents", "read")
            return []

