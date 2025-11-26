"""
OwnLens - Configuration Domain: System Setting History Model

System setting history validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class SystemSettingHistoryBase(BaseEntity):
    """Base system setting history schema"""

    history_id: UUID = Field(description="History ID")
    setting_id: UUID = Field(description="Setting ID")
    change_type: str = Field(max_length=50, description="Change type")
    old_value: Optional[str] = Field(default=None, description="Old value")
    new_value: Optional[str] = Field(default=None, description="New value")
    changed_by: Optional[UUID] = Field(default=None, description="Changed by user ID")
    changed_at: datetime = Field(default_factory=datetime.utcnow, description="Changed at timestamp")
    change_reason: Optional[str] = Field(default=None, description="Change reason")

    @field_validator("change_type")
    @classmethod
    def validate_change_type(cls, v: str) -> str:
        """Validate change type"""
        allowed_types = ["CREATED", "UPDATED", "DELETED"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "change_type")


class SystemSettingHistory(SystemSettingHistoryBase):
    """System setting history schema (read)"""

    pass


class SystemSettingHistoryCreate(SystemSettingHistoryBase):
    """System setting history creation schema"""

    history_id: Optional[UUID] = Field(default=None, description="History ID (auto-generated if not provided)")

    @field_validator("history_id", mode="before")
    @classmethod
    def generate_history_id(cls, v: Optional[UUID]) -> UUID:
        """Generate history ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "history_id")


class SystemSettingHistoryUpdate(BaseEntity):
    """System setting history update schema"""

    change_reason: Optional[str] = Field(default=None, description="Change reason")

