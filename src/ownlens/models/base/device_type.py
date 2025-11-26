"""
OwnLens - Base Domain: Device Type Model

Device type validation models.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class DeviceTypeBase(BaseEntity, TimestampMixin):
    """Base device type schema"""

    device_type_id: UUID = Field(description="Device type ID")
    device_type_code: str = Field(max_length=50, description="Device type code")
    device_type_name: str = Field(max_length=100, description="Device type name")
    description: Optional[str] = Field(default=None, description="Description")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("device_type_code")
    @classmethod
    def validate_device_type_code(cls, v: str) -> str:
        """Validate device type code format"""
        return FieldValidators.validate_code(v, "device_type_code")


class DeviceType(DeviceTypeBase):
    """Device type schema (read)"""

    pass


class DeviceTypeCreate(DeviceTypeBase):
    """Device type creation schema"""

    device_type_id: Optional[UUID] = Field(default=None, description="Device type ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_device_type_id(cls, data: Any) -> Any:
        """Ensure device_type_id is generated if missing or None"""
        if isinstance(data, dict):
            if "device_type_id" not in data or data.get("device_type_id") is None or data.get("device_type_id") == "":
                from uuid import uuid4
                data["device_type_id"] = uuid4()
        return data

    @field_validator("device_type_id", mode="before")
    @classmethod
    def validate_device_type_id(cls, v: Any) -> UUID:
        """Validate device type ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "device_type_id")


class DeviceTypeUpdate(BaseEntity, TimestampMixin):
    """Device type update schema"""

    device_type_name: Optional[str] = Field(default=None, max_length=100, description="Device type name")
    description: Optional[str] = Field(default=None, description="Description")
    is_active: Optional[bool] = Field(default=None, description="Is active")

