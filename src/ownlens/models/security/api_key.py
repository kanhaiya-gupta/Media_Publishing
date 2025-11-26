"""
OwnLens - Security Domain: API Key Model

API key validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ApiKeyBase(BaseEntityWithCompanyBrand):
    """Base API key schema"""

    api_key_id: UUID = Field(description="API key ID")
    api_key_hash: str = Field(max_length=255, description="Hashed API key")
    api_key_name: str = Field(max_length=255, description="API key name")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    permissions: List[str] = Field(default_factory=list, description="Permission codes")
    is_active: bool = Field(default=True, description="Is API key active")
    last_used_at: Optional[datetime] = Field(default=None, description="Last used timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")
    rate_limit_per_minute: int = Field(default=1000, ge=0, description="Rate limit per minute")
    rate_limit_per_hour: int = Field(default=10000, ge=0, description="Rate limit per hour")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")


class ApiKey(ApiKeyBase):
    """API key schema (read)"""

    pass


class ApiKeyCreate(ApiKeyBase):
    """API key creation schema"""

    api_key_id: Optional[UUID] = Field(default=None, description="API key ID (auto-generated if not provided)")

    @field_validator("api_key_id", mode="before")
    @classmethod
    def generate_api_key_id(cls, v: Optional[UUID]) -> UUID:
        """Generate API key ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "api_key_id")


class ApiKeyUpdate(BaseEntityWithCompanyBrand):
    """API key update schema"""

    api_key_name: Optional[str] = Field(default=None, max_length=255, description="API key name")
    permissions: Optional[List[str]] = Field(default=None, description="Permission codes")
    is_active: Optional[bool] = Field(default=None, description="Is API key active")
    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")
    rate_limit_per_minute: Optional[int] = Field(default=None, ge=0, description="Rate limit per minute")
    rate_limit_per_hour: Optional[int] = Field(default=None, ge=0, description="Rate limit per hour")

