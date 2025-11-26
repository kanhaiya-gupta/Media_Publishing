"""
OwnLens - Configuration Domain: Feature Flag Service

Service for feature flag management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.configuration import FeatureFlagRepository
from ...models.configuration.feature_flag import FeatureFlag, FeatureFlagCreate, FeatureFlagUpdate

logger = logging.getLogger(__name__)


class FeatureFlagService(BaseService[FeatureFlag, FeatureFlagCreate, FeatureFlagUpdate, FeatureFlag]):
    """Service for feature flag management."""
    
    def __init__(self, repository: FeatureFlagRepository, service_name: str = None):
        """Initialize the feature flag service."""
        super().__init__(repository, service_name or "FeatureFlagService")
        self.repository: FeatureFlagRepository = repository
    
    def get_model_class(self):
        """Get the FeatureFlag model class."""
        return FeatureFlag
    
    def get_create_model_class(self):
        """Get the FeatureFlagCreate model class."""
        return FeatureFlagCreate
    
    def get_update_model_class(self):
        """Get the FeatureFlagUpdate model class."""
        return FeatureFlagUpdate
    
    def get_in_db_model_class(self):
        """Get the FeatureFlag model class."""
        return FeatureFlag
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for feature flag operations."""
        try:
            if operation == "create":
                # Check for duplicate feature flag key
                if hasattr(data, 'flag_key'):
                    flag_key = data.flag_key
                else:
                    flag_key = data.get('flag_key') if isinstance(data, dict) else None
                
                if flag_key:
                    existing = await self.repository.get_feature_flag_by_key(flag_key)
                    if existing:
                        raise ValidationError(
                            f"Feature flag with key '{flag_key}' already exists",
                            "DUPLICATE_FLAG_KEY"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_feature_flag(self, flag_data: FeatureFlagCreate) -> FeatureFlag:
        """Create a new feature flag."""
        try:
            await self.validate_input(flag_data, "create")
            await self.validate_business_rules(flag_data, "create")
            
            result = await self.repository.create_feature_flag(flag_data)
            if not result:
                raise NotFoundError("Failed to create feature flag", "CREATE_FAILED")
            
            self.log_operation("create_feature_flag", {"flag_id": str(result.flag_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_feature_flag", "create")
            raise
    
    async def get_feature_flag_by_id(self, flag_id: UUID) -> FeatureFlag:
        """Get feature flag by ID."""
        try:
            result = await self.repository.get_feature_flag_by_id(flag_id)
            if not result:
                raise NotFoundError(f"Feature flag with ID {flag_id} not found", "FEATURE_FLAG_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_feature_flag_by_id", "read")
            raise
    
    async def get_feature_flag_by_key(self, flag_key: str) -> Optional[FeatureFlag]:
        """Get feature flag by key."""
        try:
            return await self.repository.get_feature_flag_by_key(flag_key)
        except Exception as e:
            await self.handle_error(e, "get_feature_flag_by_key", "read")
            raise
    
    async def get_active_feature_flags(self) -> List[FeatureFlag]:
        """Get all active feature flags."""
        try:
            return await self.repository.get_active_feature_flags()
        except Exception as e:
            await self.handle_error(e, "get_active_feature_flags", "read")
            return []

