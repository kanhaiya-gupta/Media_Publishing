"""
OwnLens - Audit Domain: Compliance Event Model

Compliance event validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ComplianceEventBase(BaseEntityWithCompanyBrand):
    """Base compliance event schema"""

    event_id: UUID = Field(description="Event ID")
    compliance_type: str = Field(max_length=100, description="Compliance type")
    event_type: str = Field(max_length=100, description="Event type")
    description: Optional[str] = Field(default=None, description="Event description")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    requested_by: Optional[UUID] = Field(default=None, description="Requested by user ID")
    processed_by: Optional[UUID] = Field(default=None, description="Processed by user ID")
    resource_type: Optional[str] = Field(default=None, max_length=100, description="Resource type")
    resource_ids: Optional[List[UUID]] = Field(default=None, description="Resource IDs")
    data_scope: Optional[Dict[str, Any]] = Field(default=None, description="Data scope")
    status: str = Field(description="Event status")
    status_message: Optional[str] = Field(default=None, description="Status message")
    requested_at: Optional[datetime] = Field(default=None, description="Requested at timestamp")
    processed_at: Optional[datetime] = Field(default=None, description="Processed at timestamp")
    completed_at: Optional[datetime] = Field(default=None, description="Completed at timestamp")
    event_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    records_affected: Optional[int] = Field(default=None, ge=0, description="Records affected")
    data_export_url: Optional[str] = Field(default=None, max_length=1000, description="Data export URL")

    @field_validator("compliance_type")
    @classmethod
    def validate_compliance_type(cls, v: str) -> str:
        """Validate compliance type"""
        allowed_types = ["GDPR_ACCESS_REQUEST", "GDPR_DELETION_REQUEST", "DATA_RETENTION", "DATA_EXPORT", "CONSENT_UPDATE"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "compliance_type")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type"""
        allowed_types = ["REQUEST", "PROCESSED", "COMPLETED", "FAILED"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "event_type")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status"""
        allowed_statuses = ["PENDING", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "status")

    @field_validator("data_export_url")
    @classmethod
    def validate_data_export_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate data export URL"""
        return FieldValidators.validate_url(v) if v else None


class ComplianceEvent(ComplianceEventBase):
    """Compliance event schema (read)"""

    event_date: date = Field(description="Event date (generated from event_timestamp)")


class ComplianceEventCreate(ComplianceEventBase):
    """Compliance event creation schema"""

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


class ComplianceEventUpdate(BaseEntityWithCompanyBrand):
    """Compliance event update schema"""

    status: Optional[str] = Field(default=None, description="Event status")
    status_message: Optional[str] = Field(default=None, description="Status message")
    processed_at: Optional[datetime] = Field(default=None, description="Processed at timestamp")
    completed_at: Optional[datetime] = Field(default=None, description="Completed at timestamp")
    records_affected: Optional[int] = Field(default=None, ge=0, description="Records affected")
    data_export_url: Optional[str] = Field(default=None, max_length=1000, description="Data export URL")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate status"""
        if v is None:
            return None
        allowed_statuses = ["PENDING", "PROCESSING", "COMPLETED", "FAILED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "status")

