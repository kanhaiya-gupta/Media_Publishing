"""
OwnLens - Security Domain: Role Model

Role validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class RoleBase(BaseEntity):
    """Base role schema"""

    role_id: UUID = Field(description="Role ID")
    role_name: str = Field(max_length=100, description="Role name")
    role_code: str = Field(max_length=50, description="Role code")
    description: Optional[str] = Field(default=None, description="Role description")
    is_system_role: bool = Field(default=False, description="Is system role (cannot be deleted)")
    is_active: bool = Field(default=True, description="Is role active")

    @field_validator("role_code")
    @classmethod
    def validate_role_code(cls, v: str) -> str:
        """Validate role code format"""
        return FieldValidators.validate_code(v, "role_code")


class Role(RoleBase):
    """Role schema (read)"""

    pass


class RoleCreate(RoleBase):
    """Role creation schema"""

    role_id: Optional[UUID] = Field(default=None, description="Role ID (auto-generated if not provided)")

    @field_validator("role_id", mode="before")
    @classmethod
    def generate_role_id(cls, v: Optional[UUID]) -> UUID:
        """Generate role ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "role_id")


class RoleUpdate(BaseEntity):
    """Role update schema"""

    role_name: Optional[str] = Field(default=None, max_length=100, description="Role name")
    description: Optional[str] = Field(default=None, description="Role description")
    is_active: Optional[bool] = Field(default=None, description="Is role active")

