"""
OwnLens - Security Domain: User Session Model

User session validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class UserSessionBase(BaseEntity):
    """Base user session schema"""

    session_id: UUID = Field(description="Session ID")
    user_id: UUID = Field(description="User ID")
    session_token: str = Field(max_length=500, description="Session token (JWT or session token)")
    refresh_token: Optional[str] = Field(default=None, max_length=500, description="Refresh token")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")
    device_fingerprint: Optional[str] = Field(default=None, max_length=255, description="Device fingerprint")
    is_active: bool = Field(default=True, description="Is session active")
    expires_at: datetime = Field(description="Expiration timestamp")
    last_activity_at: datetime = Field(default_factory=datetime.utcnow, description="Last activity timestamp")

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


class UserSession(UserSessionBase):
    """User session schema (read)"""

    pass


class UserSessionCreate(UserSessionBase):
    """User session creation schema"""

    session_id: Optional[UUID] = Field(default=None, description="Session ID (auto-generated if not provided)")

    @field_validator("session_id", mode="before")
    @classmethod
    def generate_session_id(cls, v: Optional[UUID]) -> UUID:
        """Generate session ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "session_id")


class UserSessionUpdate(BaseEntity):
    """User session update schema"""

    is_active: Optional[bool] = Field(default=None, description="Is session active")
    last_activity_at: Optional[datetime] = Field(default=None, description="Last activity timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expiration timestamp")

