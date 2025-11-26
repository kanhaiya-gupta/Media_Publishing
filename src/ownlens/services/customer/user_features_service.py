"""
OwnLens - Customer Domain: User Features Service

Service for user features management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import UserFeaturesRepository
from ...models.customer.user_features import UserFeatures, UserFeaturesCreate, UserFeaturesUpdate

logger = logging.getLogger(__name__)


class UserFeaturesService(BaseService[UserFeatures, UserFeaturesCreate, UserFeaturesUpdate, UserFeatures]):
    """Service for user features management."""
    
    def __init__(self, repository: UserFeaturesRepository, service_name: str = None):
        """Initialize the user features service."""
        super().__init__(repository, service_name or "UserFeaturesService")
        self.repository: UserFeaturesRepository = repository
    
    def get_model_class(self):
        """Get the UserFeatures model class."""
        return UserFeatures
    
    def get_create_model_class(self):
        """Get the UserFeaturesCreate model class."""
        return UserFeaturesCreate
    
    def get_update_model_class(self):
        """Get the UserFeaturesUpdate model class."""
        return UserFeaturesUpdate
    
    def get_in_db_model_class(self):
        """Get the UserFeatures model class."""
        return UserFeatures
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for user features operations."""
        try:
            # Validate engagement score range
            if hasattr(data, 'engagement_score'):
                score = data.engagement_score
            else:
                score = data.get('engagement_score') if isinstance(data, dict) else None
            
            if score is not None and (score < 0 or score > 1):
                raise ValidationError(
                    "Engagement score must be between 0 and 1",
                    "INVALID_ENGAGEMENT_SCORE"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_user_features(self, features_data: UserFeaturesCreate) -> UserFeatures:
        """Create new user features."""
        try:
            await self.validate_input(features_data, "create")
            await self.validate_business_rules(features_data, "create")
            
            result = await self.repository.create_user_features(features_data)
            if not result:
                raise NotFoundError("Failed to create user features", "CREATE_FAILED")
            
            self.log_operation("create_user_features", {"user_feature_id": str(result.user_feature_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_user_features", "create")
            raise
    
    async def get_user_features_by_id(self, user_feature_id: UUID) -> UserFeatures:
        """Get user features by ID."""
        try:
            result = await self.repository.get_user_features_by_id(user_feature_id)
            if not result:
                raise NotFoundError(f"User features with ID {user_feature_id} not found", "USER_FEATURES_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_user_features_by_id", "read")
            raise
    
    async def get_features_by_user(self, user_id: UUID) -> Optional[UserFeatures]:
        """Get latest features for a user."""
        try:
            return await self.repository.get_features_by_user(user_id)
        except Exception as e:
            await self.handle_error(e, "get_features_by_user", "read")
            raise

