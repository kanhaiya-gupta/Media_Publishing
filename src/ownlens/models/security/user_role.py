"""
OwnLens - Security Domain: User Role Model

User-role relationship validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class UserRoleBase(BaseEntityWithCompanyBrand):
    """Base user-role relationship schema"""

    user_role_id: UUID = Field(description="User role ID")
    user_id: UUID = Field(description="User ID")
    role_id: UUID = Field(description="Role ID")
    granted_at: datetime = Field(default_factory=datetime.utcnow, description="Granted at timestamp")
    granted_by: Optional[UUID] = Field(default=None, description="Granted by user ID")
    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")
    is_active: bool = Field(default=True, description="Is role assignment active")


class UserRole(UserRoleBase):
    """User role schema (read)"""

    pass


class UserRoleCreate(UserRoleBase):
    """User role creation schema"""

    user_role_id: Optional[UUID] = Field(default=None, description="User role ID (auto-generated if not provided)")

    @field_validator("user_role_id", mode="before")
    @classmethod
    def generate_user_role_id(cls, v: Optional[UUID]) -> UUID:
        """Generate user role ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "user_role_id")


class UserRoleUpdate(BaseEntityWithCompanyBrand):
    """User role update schema"""

    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")
    is_active: Optional[bool] = Field(default=None, description="Is role assignment active")

