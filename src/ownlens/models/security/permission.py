"""
OwnLens - Security Domain: Permission Model

Permission validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class PermissionBase(BaseEntity):
    """Base permission schema"""

    permission_id: UUID = Field(description="Permission ID")
    permission_name: str = Field(max_length=255, description="Permission name")
    permission_code: str = Field(max_length=100, description="Permission code")
    resource_type: str = Field(max_length=100, description="Resource type")
    action: str = Field(max_length=50, description="Action")
    description: Optional[str] = Field(default=None, description="Permission description")
    is_active: bool = Field(default=True, description="Is permission active")

    @field_validator("permission_code")
    @classmethod
    def validate_permission_code(cls, v: str) -> str:
        """Validate permission code format (allows colons for format like 'read:customer:data')"""
        import re
        if v is None or v == "":
            raise ValueError("permission_code: Cannot be empty")
        # Allow alphanumeric, underscores, hyphens, and colons
        if not re.match(r"^[a-zA-Z0-9_:-]+$", v):
            raise ValueError("permission_code: Must contain only alphanumeric characters, underscores, hyphens, and colons")
        return v.strip()

    @field_validator("resource_type")
    @classmethod
    def validate_resource_type(cls, v: str) -> str:
        """Validate resource type"""
        allowed_types = ["customer", "editorial", "company", "ml_models", "audit", "security", "compliance", "data_quality"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "resource_type")

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        """Validate action"""
        allowed_actions = ["read", "write", "delete", "execute", "admin"]
        return FieldValidators.validate_enum(v.lower(), allowed_actions, "action")


class Permission(PermissionBase):
    """Permission schema (read)"""

    pass


class PermissionCreate(PermissionBase):
    """Permission creation schema"""

    permission_id: Optional[UUID] = Field(default=None, description="Permission ID (auto-generated if not provided)")

    @field_validator("permission_id", mode="before")
    @classmethod
    def generate_permission_id(cls, v: Optional[UUID]) -> UUID:
        """Generate permission ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "permission_id")


class PermissionUpdate(BaseEntity):
    """Permission update schema"""

    permission_name: Optional[str] = Field(default=None, max_length=255, description="Permission name")
    description: Optional[str] = Field(default=None, description="Permission description")
    is_active: Optional[bool] = Field(default=None, description="Is permission active")

