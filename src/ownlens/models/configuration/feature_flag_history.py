"""
OwnLens - Configuration Domain: Feature Flag History Model

Feature flag history validation models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class FeatureFlagHistoryBase(BaseEntity):
    """Base feature flag history schema"""

    history_id: UUID = Field(description="History ID")
    flag_id: UUID = Field(description="Flag ID")
    change_type: str = Field(max_length=50, description="Change type")
    old_value: Optional[Dict[str, Any]] = Field(default=None, description="Old value")
    new_value: Optional[Dict[str, Any]] = Field(default=None, description="New value")
    changed_by: Optional[UUID] = Field(default=None, description="Changed by user ID")
    changed_at: datetime = Field(default_factory=datetime.utcnow, description="Changed at timestamp")
    change_reason: Optional[str] = Field(default=None, description="Change reason")

    @field_validator("change_type")
    @classmethod
    def validate_change_type(cls, v: str) -> str:
        """Validate change type"""
        allowed_types = ["CREATED", "ENABLED", "DISABLED", "UPDATED", "DELETED"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "change_type")


class FeatureFlagHistory(FeatureFlagHistoryBase):
    """Feature flag history schema (read)"""

    pass


class FeatureFlagHistoryCreate(FeatureFlagHistoryBase):
    """Feature flag history creation schema"""

    history_id: Optional[UUID] = Field(default=None, description="History ID (auto-generated if not provided)")

    @field_validator("history_id", mode="before")
    @classmethod
    def generate_history_id(cls, v: Optional[UUID]) -> UUID:
        """Generate history ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "history_id")


class FeatureFlagHistoryUpdate(BaseEntity):
    """Feature flag history update schema"""

    change_reason: Optional[str] = Field(default=None, description="Change reason")

