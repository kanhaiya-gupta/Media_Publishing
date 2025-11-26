"""
OwnLens - Configuration Domain: System Setting Service

Service for system setting management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.configuration import SystemSettingRepository
from ...models.configuration.system_setting import SystemSetting, SystemSettingCreate, SystemSettingUpdate

logger = logging.getLogger(__name__)


class SystemSettingService(BaseService[SystemSetting, SystemSettingCreate, SystemSettingUpdate, SystemSetting]):
    """Service for system setting management."""
    
    def __init__(self, repository: SystemSettingRepository, service_name: str = None):
        """Initialize the system setting service."""
        super().__init__(repository, service_name or "SystemSettingService")
        self.repository: SystemSettingRepository = repository
    
    def get_model_class(self):
        """Get the SystemSetting model class."""
        return SystemSetting
    
    def get_create_model_class(self):
        """Get the SystemSettingCreate model class."""
        return SystemSettingCreate
    
    def get_update_model_class(self):
        """Get the SystemSettingUpdate model class."""
        return SystemSettingUpdate
    
    def get_in_db_model_class(self):
        """Get the SystemSetting model class."""
        return SystemSetting
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for system setting operations."""
        try:
            if operation == "create":
                # Check for duplicate setting key
                if hasattr(data, 'setting_key'):
                    setting_key = data.setting_key
                else:
                    setting_key = data.get('setting_key') if isinstance(data, dict) else None
                
                if setting_key:
                    existing = await self.repository.get_system_setting_by_key(setting_key)
                    if existing:
                        raise ValidationError(
                            f"System setting with key '{setting_key}' already exists",
                            "DUPLICATE_SETTING_KEY"
                        )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_system_setting(self, setting_data: SystemSettingCreate) -> SystemSetting:
        """Create a new system setting."""
        try:
            await self.validate_input(setting_data, "create")
            await self.validate_business_rules(setting_data, "create")
            
            result = await self.repository.create_system_setting(setting_data)
            if not result:
                raise NotFoundError("Failed to create system setting", "CREATE_FAILED")
            
            self.log_operation("create_system_setting", {"setting_id": str(result.setting_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_system_setting", "create")
            raise
    
    async def get_system_setting_by_id(self, setting_id: UUID) -> SystemSetting:
        """Get system setting by ID."""
        try:
            result = await self.repository.get_system_setting_by_id(setting_id)
            if not result:
                raise NotFoundError(f"System setting with ID {setting_id} not found", "SYSTEM_SETTING_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_system_setting_by_id", "read")
            raise
    
    async def get_system_setting_by_key(self, setting_key: str) -> Optional[SystemSetting]:
        """Get system setting by key."""
        try:
            return await self.repository.get_system_setting_by_key(setting_key)
        except Exception as e:
            await self.handle_error(e, "get_system_setting_by_key", "read")
            raise
    
    async def get_system_settings_by_category(self, category: str) -> List[SystemSetting]:
        """Get system settings by category."""
        try:
            return await self.repository.get_system_settings_by_category(category)
        except Exception as e:
            await self.handle_error(e, "get_system_settings_by_category", "read")
            return []

