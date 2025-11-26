"""
OwnLens - Security Domain: API Key Usage Model

API key usage tracking validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class ApiKeyUsageBase(BaseEntity):
    """Base API key usage schema"""

    usage_id: UUID = Field(description="Usage ID")
    api_key_id: UUID = Field(description="API key ID")
    endpoint: Optional[str] = Field(default=None, max_length=500, description="API endpoint")
    method: Optional[str] = Field(default=None, max_length=10, description="HTTP method")
    status_code: Optional[int] = Field(default=None, description="HTTP status code")
    response_time_ms: Optional[int] = Field(default=None, ge=0, description="Response time in milliseconds")
    request_size_bytes: Optional[int] = Field(default=None, ge=0, description="Request size in bytes")
    response_size_bytes: Optional[int] = Field(default=None, ge=0, description="Response size in bytes")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")

    @field_validator("method")
    @classmethod
    def validate_method(cls, v: Optional[str]) -> Optional[str]:
        """Validate HTTP method"""
        if v is None:
            return None
        allowed_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
        return FieldValidators.validate_enum(v.upper(), allowed_methods, "method")

    @field_validator("ip_address")
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        """Validate IP address"""
        return FieldValidators.validate_ip_address(v) if v else None


class ApiKeyUsage(ApiKeyUsageBase):
    """API key usage schema (read)"""

    usage_date: date = Field(description="Usage date (generated from created_at)")


class ApiKeyUsageCreate(ApiKeyUsageBase):
    """API key usage creation schema"""

    usage_id: Optional[UUID] = Field(default=None, description="Usage ID (auto-generated if not provided)")
    usage_date: date = Field(description="Usage date (required for partitioning)")

    @field_validator("usage_id", mode="before")
    @classmethod
    def generate_usage_id(cls, v: Optional[UUID]) -> UUID:
        """Generate usage ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "usage_id")


class ApiKeyUsageUpdate(BaseEntity):
    """API key usage update schema"""

    status_code: Optional[int] = Field(default=None, description="HTTP status code")
    response_time_ms: Optional[int] = Field(default=None, ge=0, description="Response time in milliseconds")

