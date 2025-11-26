"""
OwnLens - Base Domain: Operating System Model

Operating system validation models.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class OperatingSystemBase(BaseEntity, TimestampMixin):
    """Base operating system schema"""

    os_id: UUID = Field(description="OS ID")
    os_code: str = Field(max_length=50, description="OS code")
    os_name: str = Field(max_length=100, description="OS name")
    os_family: Optional[str] = Field(default=None, max_length=50, description="OS family")
    version: Optional[str] = Field(default=None, max_length=50, description="Version")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("os_code")
    @classmethod
    def validate_os_code(cls, v: str) -> str:
        """Validate OS code format"""
        return FieldValidators.validate_code(v, "os_code")

    @field_validator("os_family")
    @classmethod
    def validate_os_family(cls, v: Optional[str]) -> Optional[str]:
        """Validate OS family"""
        if v is None:
            return None
        allowed_families = ["mobile", "desktop", "server"]
        return FieldValidators.validate_enum(v.lower(), allowed_families, "os_family")


class OperatingSystem(OperatingSystemBase):
    """Operating system schema (read)"""

    pass


class OperatingSystemCreate(OperatingSystemBase):
    """Operating system creation schema"""

    os_id: Optional[UUID] = Field(default=None, description="OS ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_os_id(cls, data: Any) -> Any:
        """Ensure os_id is generated if missing or None"""
        if isinstance(data, dict):
            if "os_id" not in data or data.get("os_id") is None or data.get("os_id") == "":
                from uuid import uuid4
                data["os_id"] = uuid4()
        return data

    @field_validator("os_id", mode="before")
    @classmethod
    def validate_os_id(cls, v: Any) -> UUID:
        """Validate OS ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "os_id")


class OperatingSystemUpdate(BaseEntity, TimestampMixin):
    """Operating system update schema"""

    os_name: Optional[str] = Field(default=None, max_length=100, description="OS name")
    os_family: Optional[str] = Field(default=None, max_length=50, description="OS family")
    version: Optional[str] = Field(default=None, max_length=50, description="Version")
    is_active: Optional[bool] = Field(default=None, description="Is active")

    @field_validator("os_family")
    @classmethod
    def validate_os_family(cls, v: Optional[str]) -> Optional[str]:
        """Validate OS family"""
        if v is None:
            return None
        allowed_families = ["mobile", "desktop", "server"]
        return FieldValidators.validate_enum(v.lower(), allowed_families, "os_family")

