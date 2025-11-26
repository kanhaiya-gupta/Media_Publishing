"""
OwnLens - Configuration Domain: System Setting Model

System setting validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class SystemSettingBase(BaseEntityWithCompanyBrand):
    """Base system setting schema"""

    setting_id: UUID = Field(description="Setting ID")
    setting_name: str = Field(max_length=255, description="Setting name")
    setting_code: str = Field(max_length=100, description="Setting code")
    setting_category: str = Field(max_length=100, description="Setting category")
    setting_value: str = Field(description="Setting value")
    setting_type: str = Field(max_length=50, description="Setting type")
    default_value: Optional[str] = Field(default=None, description="Default value")
    description: Optional[str] = Field(default=None, description="Setting description")
    is_encrypted: bool = Field(default=False, description="Is value encrypted")
    is_secret: bool = Field(default=False, description="Is value secret")
    environment: Optional[str] = Field(default=None, max_length=50, description="Environment")
    is_active: bool = Field(default=True, description="Is setting active")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")
    updated_by: Optional[UUID] = Field(default=None, description="Updated by user ID")

    @field_validator("setting_code")
    @classmethod
    def validate_setting_code(cls, v: str) -> str:
        """Validate setting code format"""
        return FieldValidators.validate_code(v, "setting_code")

    @field_validator("setting_type")
    @classmethod
    def validate_setting_type(cls, v: str) -> str:
        """Validate setting type"""
        allowed_types = ["STRING", "INTEGER", "DECIMAL", "BOOLEAN", "JSON"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "setting_type")

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: Optional[str]) -> Optional[str]:
        """Validate environment"""
        if v is None:
            return None
        allowed_environments = ["development", "staging", "production"]
        return FieldValidators.validate_enum(v.lower(), allowed_environments, "environment")


class SystemSetting(SystemSettingBase):
    """System setting schema (read)"""

    pass


class SystemSettingCreate(SystemSettingBase):
    """System setting creation schema"""

    setting_id: Optional[UUID] = Field(default=None, description="Setting ID (auto-generated if not provided)")

    @field_validator("setting_id", mode="before")
    @classmethod
    def generate_setting_id(cls, v: Optional[UUID]) -> UUID:
        """Generate setting ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "setting_id")


class SystemSettingUpdate(BaseEntityWithCompanyBrand):
    """System setting update schema"""

    setting_value: Optional[str] = Field(default=None, description="Setting value")
    is_active: Optional[bool] = Field(default=None, description="Is setting active")
    description: Optional[str] = Field(default=None, description="Setting description")

