"""
OwnLens - Audit Domain: Security Event Model

Security event validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class SecurityEventBase(BaseEntityWithCompanyBrand):
    """Base security event schema"""

    event_id: UUID = Field(description="Event ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")
    event_type: str = Field(max_length=100, description="Event type")
    event_severity: str = Field(max_length=20, description="Event severity")
    description: Optional[str] = Field(default=None, description="Event description")
    success: bool = Field(description="Success status")
    failure_reason: Optional[str] = Field(default=None, description="Failure reason")
    event_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    session_id: Optional[UUID] = Field(default=None, description="Session ID")
    event_data: Optional[Dict[str, Any]] = Field(default=None, description="Event data")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type"""
        allowed_types = [
            "LOGIN_SUCCESS", "LOGIN_FAILED", "LOGOUT", "PASSWORD_CHANGE",
            "ROLE_CHANGE", "PERMISSION_DENIED", "SUSPICIOUS_ACTIVITY"
        ]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "event_type")

    @field_validator("event_severity")
    @classmethod
    def validate_event_severity(cls, v: str) -> str:
        """Validate event severity"""
        allowed_severities = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "event_severity")

    @field_validator("ip_address")
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        """Validate IP address"""
        return FieldValidators.validate_ip_address(v) if v else None


class SecurityEvent(SecurityEventBase):
    """Security event schema (read)"""

    event_date: date = Field(description="Event date (generated from event_timestamp)")


class SecurityEventCreate(SecurityEventBase):
    """Security event creation schema"""

    event_id: Optional[UUID] = Field(default=None, description="Event ID (auto-generated if not provided)")
    event_date: date = Field(description="Event date (required for partitioning)")

    @field_validator("event_id", mode="before")
    @classmethod
    def generate_event_id(cls, v: Optional[UUID]) -> UUID:
        """Generate event ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "event_id")


class SecurityEventUpdate(BaseEntityWithCompanyBrand):
    """Security event update schema"""

    event_severity: Optional[str] = Field(default=None, max_length=20, description="Event severity")
    description: Optional[str] = Field(default=None, description="Event description")
    success: Optional[bool] = Field(default=None, description="Success status")

    @field_validator("event_severity")
    @classmethod
    def validate_event_severity(cls, v: Optional[str]) -> Optional[str]:
        """Validate event severity"""
        if v is None:
            return None
        allowed_severities = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "event_severity")

