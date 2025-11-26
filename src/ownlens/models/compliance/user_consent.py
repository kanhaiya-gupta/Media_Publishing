"""
OwnLens - Compliance Domain: User Consent Model

User consent validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class UserConsentBase(BaseEntityWithCompanyBrand):
    """Base user consent schema"""

    consent_id: UUID = Field(description="Consent ID")
    user_id: UUID = Field(description="User ID")
    consent_type: str = Field(max_length=100, description="Consent type")
    consent_status: str = Field(description="Consent status")
    consent_method: Optional[str] = Field(default=None, max_length=50, description="Consent method")
    consent_text: Optional[str] = Field(default=None, description="Consent text shown to user")
    consent_version: Optional[str] = Field(default=None, max_length=50, description="Consent version")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")
    granted_at: Optional[datetime] = Field(default=None, description="Granted at timestamp")
    withdrawn_at: Optional[datetime] = Field(default=None, description="Withdrawn at timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expires at timestamp")

    @field_validator("consent_type")
    @classmethod
    def validate_consent_type(cls, v: str) -> str:
        """Validate consent type"""
        allowed_types = ["analytics", "marketing", "personalization", "cookies", "data_sharing"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "consent_type")

    @field_validator("consent_status")
    @classmethod
    def validate_consent_status(cls, v: str) -> str:
        """Validate consent status"""
        allowed_statuses = ["granted", "denied", "withdrawn", "expired"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "consent_status")

    @field_validator("consent_method")
    @classmethod
    def validate_consent_method(cls, v: Optional[str]) -> Optional[str]:
        """Validate consent method"""
        if v is None:
            return None
        allowed_methods = ["explicit", "implicit", "opt_in", "opt_out"]
        return FieldValidators.validate_enum(v.lower(), allowed_methods, "consent_method")

    @field_validator("ip_address")
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        """Validate IP address"""
        return FieldValidators.validate_ip_address(v) if v else None


class UserConsent(UserConsentBase):
    """User consent schema (read)"""

    pass


class UserConsentCreate(UserConsentBase):
    """User consent creation schema"""

    consent_id: Optional[UUID] = Field(default=None, description="Consent ID (auto-generated if not provided)")

    @field_validator("consent_id", mode="before")
    @classmethod
    def generate_consent_id(cls, v: Optional[UUID]) -> UUID:
        """Generate consent ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "consent_id")


class UserConsentUpdate(BaseEntityWithCompanyBrand):
    """User consent update schema"""

    consent_status: Optional[str] = Field(default=None, description="Consent status")
    withdrawn_at: Optional[datetime] = Field(default=None, description="Withdrawn at timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expires at timestamp")

    @field_validator("consent_status")
    @classmethod
    def validate_consent_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate consent status"""
        if v is None:
            return None
        allowed_statuses = ["granted", "denied", "withdrawn", "expired"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "consent_status")

