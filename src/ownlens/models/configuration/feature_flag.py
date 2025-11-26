"""
OwnLens - Configuration Domain: Feature Flag Model

Feature flag validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class FeatureFlagBase(BaseEntity):
    """Base feature flag schema"""

    flag_id: UUID = Field(description="Flag ID")
    flag_name: str = Field(max_length=255, description="Flag name")
    flag_code: str = Field(max_length=100, description="Flag code")
    flag_type: str = Field(max_length=50, description="Flag type")
    is_enabled: bool = Field(default=False, description="Is flag enabled")
    enabled_percentage: float = Field(default=0.0, ge=0, le=1, description="Enabled percentage")
    enabled_user_ids: Optional[List[UUID]] = Field(default=None, description="Enabled user IDs")
    enabled_company_ids: Optional[List[UUID]] = Field(default=None, description="Enabled company IDs")
    enabled_brand_ids: Optional[List[UUID]] = Field(default=None, description="Enabled brand IDs")
    flag_value: Optional[Dict[str, Any]] = Field(default=None, description="Custom flag value")
    default_value: Optional[Dict[str, Any]] = Field(default=None, description="Default value")
    description: Optional[str] = Field(default=None, description="Flag description")
    category: Optional[str] = Field(default=None, max_length=100, description="Category")
    tags: Optional[List[str]] = Field(default=None, description="Tags")
    rollout_start_date: Optional[date] = Field(default=None, description="Rollout start date")
    rollout_end_date: Optional[date] = Field(default=None, description="Rollout end date")
    rollout_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Rollout percentage")
    is_active: bool = Field(default=True, description="Is flag active")
    is_experimental: bool = Field(default=False, description="Is experimental")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")
    updated_by: Optional[UUID] = Field(default=None, description="Updated by user ID")

    @field_validator("flag_code")
    @classmethod
    def validate_flag_code(cls, v: str) -> str:
        """Validate flag code format"""
        return FieldValidators.validate_code(v, "flag_code")

    @field_validator("flag_type")
    @classmethod
    def validate_flag_type(cls, v: str) -> str:
        """Validate flag type"""
        allowed_types = ["BOOLEAN", "PERCENTAGE", "USER_LIST", "CUSTOM"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "flag_type")

    @field_validator("enabled_percentage", "rollout_percentage")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class FeatureFlag(FeatureFlagBase):
    """Feature flag schema (read)"""

    pass


class FeatureFlagCreate(FeatureFlagBase):
    """Feature flag creation schema"""

    flag_id: Optional[UUID] = Field(default=None, description="Flag ID (auto-generated if not provided)")

    @field_validator("flag_id", mode="before")
    @classmethod
    def generate_flag_id(cls, v: Optional[UUID]) -> UUID:
        """Generate flag ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "flag_id")


class FeatureFlagUpdate(BaseEntity):
    """Feature flag update schema"""

    flag_name: Optional[str] = Field(default=None, max_length=255, description="Flag name")
    is_enabled: Optional[bool] = Field(default=None, description="Is flag enabled")
    enabled_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Enabled percentage")
    enabled_user_ids: Optional[List[UUID]] = Field(default=None, description="Enabled user IDs")
    is_active: Optional[bool] = Field(default=None, description="Is flag active")
    rollout_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Rollout percentage")

