"""
OwnLens - Audit Domain: Audit Log Model

Audit log validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class AuditLogBase(BaseEntityWithCompanyBrand):
    """Base audit log schema"""

    log_id: UUID = Field(description="Log ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    api_key_id: Optional[UUID] = Field(default=None, description="API key ID")
    session_id: Optional[UUID] = Field(default=None, description="Session ID")
    action: str = Field(max_length=50, description="Action")
    resource_type: str = Field(max_length=100, description="Resource type")
    resource_id: Optional[UUID] = Field(default=None, description="Resource ID")
    resource_identifier: Optional[str] = Field(default=None, max_length=255, description="Resource identifier")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")
    endpoint: Optional[str] = Field(default=None, max_length=500, description="API endpoint")
    method: Optional[str] = Field(default=None, max_length=10, description="HTTP method")
    event_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    success: bool = Field(description="Success status")
    status_code: Optional[int] = Field(default=None, description="HTTP status code")
    error_message: Optional[str] = Field(default=None, description="Error message")
    error_code: Optional[str] = Field(default=None, max_length=50, description="Error code")
    request_data: Optional[Dict[str, Any]] = Field(default=None, description="Request data")
    response_data: Optional[Dict[str, Any]] = Field(default=None, description="Response data")
    request_size_bytes: Optional[int] = Field(default=None, ge=0, description="Request size in bytes")
    response_size_bytes: Optional[int] = Field(default=None, ge=0, description="Response size in bytes")
    response_time_ms: Optional[int] = Field(default=None, ge=0, description="Response time in milliseconds")

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        """Validate action"""
        allowed_actions = ["READ", "WRITE", "DELETE", "UPDATE", "EXPORT", "EXECUTE", "LOGIN", "LOGOUT"]
        return FieldValidators.validate_enum(v.upper(), allowed_actions, "action")

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

    @field_validator("user_agent")
    @classmethod
    def validate_user_agent(cls, v: Optional[str]) -> Optional[str]:
        """Validate user agent"""
        return FieldValidators.validate_user_agent(v) if v else None


class AuditLog(AuditLogBase):
    """Audit log schema (read)"""

    event_date: date = Field(description="Event date (generated from event_timestamp)")


class AuditLogCreate(AuditLogBase):
    """Audit log creation schema"""

    log_id: Optional[UUID] = Field(default=None, description="Log ID (auto-generated if not provided)")
    event_date: date = Field(description="Event date (required for partitioning)")

    @field_validator("log_id", mode="before")
    @classmethod
    def generate_log_id(cls, v: Optional[UUID]) -> UUID:
        """Generate log ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "log_id")


class AuditLogUpdate(BaseEntityWithCompanyBrand):
    """Audit log update schema"""

    success: Optional[bool] = Field(default=None, description="Success status")
    status_code: Optional[int] = Field(default=None, description="HTTP status code")
    error_message: Optional[str] = Field(default=None, description="Error message")
    response_time_ms: Optional[int] = Field(default=None, ge=0, description="Response time in milliseconds")

