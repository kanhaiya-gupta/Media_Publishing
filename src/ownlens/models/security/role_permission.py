"""
OwnLens - Security Domain: Role Permission Model

Role-permission relationship validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class RolePermissionBase(BaseEntity):
    """Base role-permission relationship schema"""

    role_permission_id: UUID = Field(description="Role permission ID")
    role_id: UUID = Field(description="Role ID")
    permission_id: UUID = Field(description="Permission ID")
    granted_at: datetime = Field(default_factory=datetime.utcnow, description="Granted at timestamp")
    granted_by: Optional[UUID] = Field(default=None, description="Granted by user ID")


class RolePermission(RolePermissionBase):
    """Role permission schema (read)"""

    pass


class RolePermissionCreate(RolePermissionBase):
    """Role permission creation schema"""

    role_permission_id: Optional[UUID] = Field(default=None, description="Role permission ID (auto-generated if not provided)")

    @field_validator("role_permission_id", mode="before")
    @classmethod
    def generate_role_permission_id(cls, v: Optional[UUID]) -> UUID:
        """Generate role permission ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "role_permission_id")


class RolePermissionUpdate(BaseEntity):
    """Role permission update schema"""

    granted_at: Optional[datetime] = Field(default=None, description="Granted at timestamp")
    granted_by: Optional[UUID] = Field(default=None, description="Granted by user ID")

