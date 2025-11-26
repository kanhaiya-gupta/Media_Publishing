"""
OwnLens - Compliance Domain: Data Subject Request Model

Data subject request validation models (GDPR).
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import EmailStr, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DataSubjectRequestBase(BaseEntityWithCompanyBrand):
    """Base data subject request schema"""

    request_id: UUID = Field(description="Request ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    external_user_id: Optional[str] = Field(default=None, max_length=255, description="External user ID")
    request_type: str = Field(description="Request type")
    request_status: str = Field(default="PENDING", description="Request status")
    request_priority: str = Field(default="NORMAL", description="Request priority")
    requestor_email: Optional[EmailStr] = Field(default=None, description="Requestor email")
    requestor_name: Optional[str] = Field(default=None, max_length=255, description="Requestor name")
    requestor_phone: Optional[str] = Field(default=None, max_length=50, description="Requestor phone")
    verification_method: Optional[str] = Field(default=None, max_length=50, description="Verification method")
    verification_status: str = Field(default="PENDING", description="Verification status")
    verification_code: Optional[str] = Field(default=None, max_length=100, description="Verification code")
    verification_expires_at: Optional[datetime] = Field(default=None, description="Verification expiration")
    request_description: Optional[str] = Field(default=None, description="Request description")
    data_scope: Optional[Dict[str, Any]] = Field(default=None, description="Data scope")
    requested_at: datetime = Field(default_factory=datetime.utcnow, description="Requested at timestamp")
    verified_at: Optional[datetime] = Field(default=None, description="Verified at timestamp")
    processing_started_at: Optional[datetime] = Field(default=None, description="Processing started at")
    completed_at: Optional[datetime] = Field(default=None, description="Completed at timestamp")
    due_date: Optional[datetime] = Field(default=None, description="Due date (GDPR: 30 days)")
    records_found: Optional[int] = Field(default=None, ge=0, description="Records found")
    records_processed: Optional[int] = Field(default=None, ge=0, description="Records processed")
    records_deleted: Optional[int] = Field(default=None, ge=0, description="Records deleted")
    data_export_url: Optional[str] = Field(default=None, max_length=1000, description="Data export URL")
    data_export_expires_at: Optional[datetime] = Field(default=None, description="Data export expiration")
    rejection_reason: Optional[str] = Field(default=None, description="Rejection reason")
    rejected_at: Optional[datetime] = Field(default=None, description="Rejected at timestamp")
    rejected_by: Optional[UUID] = Field(default=None, description="Rejected by user ID")

    @field_validator("request_type")
    @classmethod
    def validate_request_type(cls, v: str) -> str:
        """Validate request type"""
        allowed_types = ["ACCESS", "DELETION", "RECTIFICATION", "PORTABILITY", "RESTRICTION"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "request_type")

    @field_validator("request_status")
    @classmethod
    def validate_request_status(cls, v: str) -> str:
        """Validate request status"""
        allowed_statuses = ["PENDING", "VERIFIED", "PROCESSING", "COMPLETED", "REJECTED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "request_status")

    @field_validator("request_priority")
    @classmethod
    def validate_request_priority(cls, v: str) -> str:
        """Validate request priority"""
        allowed_priorities = ["LOW", "NORMAL", "HIGH", "URGENT"]
        return FieldValidators.validate_enum(v.upper(), allowed_priorities, "request_priority")

    @field_validator("verification_method")
    @classmethod
    def validate_verification_method(cls, v: Optional[str]) -> Optional[str]:
        """Validate verification method"""
        if v is None:
            return None
        allowed_methods = ["email", "phone", "id_document"]
        return FieldValidators.validate_enum(v.lower(), allowed_methods, "verification_method")

    @field_validator("verification_status")
    @classmethod
    def validate_verification_status(cls, v: str) -> str:
        """Validate verification status"""
        allowed_statuses = ["PENDING", "VERIFIED", "FAILED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "verification_status")

    @field_validator("requestor_email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format"""
        return FieldValidators.validate_email(v) if v else None

    @field_validator("requestor_phone")
    @classmethod
    def validate_phone(cls, v: Optional[str]) -> Optional[str]:
        """Validate phone number"""
        return FieldValidators.validate_phone_number(v) if v else None

    @field_validator("data_export_url")
    @classmethod
    def validate_data_export_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate data export URL"""
        return FieldValidators.validate_url(v) if v else None


class DataSubjectRequest(DataSubjectRequestBase):
    """Data subject request schema (read)"""

    pass


class DataSubjectRequestCreate(DataSubjectRequestBase):
    """Data subject request creation schema"""

    request_id: Optional[UUID] = Field(default=None, description="Request ID (auto-generated if not provided)")

    @field_validator("request_id", mode="before")
    @classmethod
    def generate_request_id(cls, v: Optional[UUID]) -> UUID:
        """Generate request ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "request_id")


class DataSubjectRequestUpdate(BaseEntityWithCompanyBrand):
    """Data subject request update schema"""

    request_status: Optional[str] = Field(default=None, description="Request status")
    verification_status: Optional[str] = Field(default=None, description="Verification status")
    verified_at: Optional[datetime] = Field(default=None, description="Verified at timestamp")
    processing_started_at: Optional[datetime] = Field(default=None, description="Processing started at")
    completed_at: Optional[datetime] = Field(default=None, description="Completed at timestamp")
    records_found: Optional[int] = Field(default=None, ge=0, description="Records found")
    records_processed: Optional[int] = Field(default=None, ge=0, description="Records processed")
    records_deleted: Optional[int] = Field(default=None, ge=0, description="Records deleted")
    data_export_url: Optional[str] = Field(default=None, max_length=1000, description="Data export URL")
    rejection_reason: Optional[str] = Field(default=None, description="Rejection reason")
    rejected_at: Optional[datetime] = Field(default=None, description="Rejected at timestamp")
    rejected_by: Optional[UUID] = Field(default=None, description="Rejected by user ID")

    @field_validator("request_status")
    @classmethod
    def validate_request_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate request status"""
        if v is None:
            return None
        allowed_statuses = ["PENDING", "VERIFIED", "PROCESSING", "COMPLETED", "REJECTED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "request_status")

    @field_validator("verification_status")
    @classmethod
    def validate_verification_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate verification status"""
        if v is None:
            return None
        allowed_statuses = ["PENDING", "VERIFIED", "FAILED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "verification_status")

