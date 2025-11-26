"""
OwnLens - Compliance Domain: Breach Incident Model

Data breach incident validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class BreachIncidentBase(BaseEntityWithCompanyBrand):
    """Base breach incident schema"""

    incident_id: UUID = Field(description="Incident ID")
    incident_type: str = Field(max_length=100, description="Incident type")
    incident_severity: str = Field(max_length=20, description="Incident severity")
    description: str = Field(description="Incident description")
    data_types_affected: Optional[List[str]] = Field(default=None, description="Data types affected")
    records_affected: Optional[int] = Field(default=None, ge=0, description="Records affected")
    users_affected: Optional[int] = Field(default=None, ge=0, description="Users affected")
    personal_data_affected: bool = Field(default=False, description="Personal data affected")
    discovered_at: datetime = Field(description="Discovered at timestamp")
    occurred_at: Optional[datetime] = Field(default=None, description="Occurred at timestamp")
    contained_at: Optional[datetime] = Field(default=None, description="Contained at timestamp")
    resolved_at: Optional[datetime] = Field(default=None, description="Resolved at timestamp")
    incident_status: str = Field(default="DISCOVERED", max_length=50, description="Incident status")
    response_actions: Optional[Dict[str, Any]] = Field(default=None, description="Response actions")
    notification_sent: bool = Field(default=False, description="Notification sent")
    notification_date: Optional[datetime] = Field(default=None, description="Notification date")
    regulatory_notification: bool = Field(default=False, description="Regulatory notification (GDPR Article 33)")
    regulatory_notification_date: Optional[datetime] = Field(default=None, description="Regulatory notification date")
    discovered_by: Optional[UUID] = Field(default=None, description="Discovered by user ID")
    investigated_by: Optional[UUID] = Field(default=None, description="Investigated by user ID")
    resolved_by: Optional[UUID] = Field(default=None, description="Resolved by user ID")

    @field_validator("incident_type")
    @classmethod
    def validate_incident_type(cls, v: str) -> str:
        """Validate incident type"""
        allowed_types = ["UNAUTHORIZED_ACCESS", "DATA_LEAK", "LOSS", "THEFT", "HACKING"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "incident_type")

    @field_validator("incident_severity")
    @classmethod
    def validate_incident_severity(cls, v: str) -> str:
        """Validate incident severity"""
        allowed_severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "incident_severity")

    @field_validator("incident_status")
    @classmethod
    def validate_incident_status(cls, v: str) -> str:
        """Validate incident status"""
        allowed_statuses = ["DISCOVERED", "INVESTIGATING", "CONTAINED", "RESOLVED", "CLOSED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "incident_status")


class BreachIncident(BreachIncidentBase):
    """Breach incident schema (read)"""

    pass


class BreachIncidentCreate(BreachIncidentBase):
    """Breach incident creation schema"""

    incident_id: Optional[UUID] = Field(default=None, description="Incident ID (auto-generated if not provided)")

    @field_validator("incident_id", mode="before")
    @classmethod
    def generate_incident_id(cls, v: Optional[UUID]) -> UUID:
        """Generate incident ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "incident_id")


class BreachIncidentUpdate(BaseEntityWithCompanyBrand):
    """Breach incident update schema"""

    incident_status: Optional[str] = Field(default=None, max_length=50, description="Incident status")
    incident_severity: Optional[str] = Field(default=None, max_length=20, description="Incident severity")
    description: Optional[str] = Field(default=None, description="Incident description")
    contained_at: Optional[datetime] = Field(default=None, description="Contained at timestamp")
    resolved_at: Optional[datetime] = Field(default=None, description="Resolved at timestamp")
    notification_sent: Optional[bool] = Field(default=None, description="Notification sent")
    regulatory_notification: Optional[bool] = Field(default=None, description="Regulatory notification")

    @field_validator("incident_status")
    @classmethod
    def validate_incident_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate incident status"""
        if v is None:
            return None
        allowed_statuses = ["DISCOVERED", "INVESTIGATING", "CONTAINED", "RESOLVED", "CLOSED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "incident_status")

    @field_validator("incident_severity")
    @classmethod
    def validate_incident_severity(cls, v: Optional[str]) -> Optional[str]:
        """Validate incident severity"""
        if v is None:
            return None
        allowed_severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "incident_severity")

