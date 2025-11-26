"""
OwnLens - Data Quality Domain: Quality Alert Model

Data quality alert validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class QualityAlertBase(BaseEntityWithCompanyBrand):
    """Base quality alert schema"""

    alert_id: UUID = Field(description="Alert ID")
    rule_id: Optional[UUID] = Field(default=None, description="Rule ID")
    check_id: Optional[UUID] = Field(default=None, description="Check ID")
    alert_type: str = Field(max_length=100, description="Alert type")
    alert_severity: str = Field(max_length=20, description="Alert severity")
    alert_title: str = Field(max_length=255, description="Alert title")
    alert_message: str = Field(description="Alert message")
    alert_status: str = Field(default="OPEN", max_length=50, description="Alert status")
    acknowledged_at: Optional[datetime] = Field(default=None, description="Acknowledged at timestamp")
    acknowledged_by: Optional[UUID] = Field(default=None, description="Acknowledged by user ID")
    resolved_at: Optional[datetime] = Field(default=None, description="Resolved at timestamp")
    resolved_by: Optional[UUID] = Field(default=None, description="Resolved by user ID")
    table_name: Optional[str] = Field(default=None, max_length=255, description="Table name")
    column_name: Optional[str] = Field(default=None, max_length=255, description="Column name")
    domain: Optional[str] = Field(default=None, max_length=50, description="Domain")
    quality_score: Optional[float] = Field(default=None, ge=0, le=1, description="Quality score")
    threshold: Optional[float] = Field(default=None, ge=0, le=1, description="Threshold")
    alert_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Alert timestamp")

    @field_validator("alert_type")
    @classmethod
    def validate_alert_type(cls, v: str) -> str:
        """Validate alert type"""
        allowed_types = ["QUALITY_DROP", "RULE_FAILURE", "THRESHOLD_BREACH"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "alert_type")

    @field_validator("alert_severity")
    @classmethod
    def validate_alert_severity(cls, v: str) -> str:
        """Validate alert severity"""
        allowed_severities = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "alert_severity")

    @field_validator("alert_status")
    @classmethod
    def validate_alert_status(cls, v: str) -> str:
        """Validate alert status"""
        allowed_statuses = ["OPEN", "ACKNOWLEDGED", "RESOLVED", "CLOSED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "alert_status")

    @field_validator("quality_score", "threshold")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class QualityAlert(QualityAlertBase):
    """Quality alert schema (read)"""

    alert_date: date = Field(description="Alert date (generated from alert_timestamp)")


class QualityAlertCreate(QualityAlertBase):
    """Quality alert creation schema"""

    alert_id: Optional[UUID] = Field(default=None, description="Alert ID (auto-generated if not provided)")
    alert_date: date = Field(description="Alert date (required for partitioning)")

    @field_validator("alert_id", mode="before")
    @classmethod
    def generate_alert_id(cls, v: Optional[UUID]) -> UUID:
        """Generate alert ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "alert_id")


class QualityAlertUpdate(BaseEntityWithCompanyBrand):
    """Quality alert update schema"""

    alert_status: Optional[str] = Field(default=None, max_length=50, description="Alert status")
    acknowledged_at: Optional[datetime] = Field(default=None, description="Acknowledged at timestamp")
    acknowledged_by: Optional[UUID] = Field(default=None, description="Acknowledged by user ID")
    resolved_at: Optional[datetime] = Field(default=None, description="Resolved at timestamp")
    resolved_by: Optional[UUID] = Field(default=None, description="Resolved by user ID")

    @field_validator("alert_status")
    @classmethod
    def validate_alert_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate alert status"""
        if v is None:
            return None
        allowed_statuses = ["OPEN", "ACKNOWLEDGED", "RESOLVED", "CLOSED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "alert_status")

