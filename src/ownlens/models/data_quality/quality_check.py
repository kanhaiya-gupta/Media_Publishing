"""
OwnLens - Data Quality Domain: Quality Check Model

Data quality check validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class QualityCheckBase(BaseEntityWithCompanyBrand):
    """Base quality check schema"""

    check_id: UUID = Field(description="Check ID")
    rule_id: UUID = Field(description="Rule ID")
    check_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Check timestamp")
    check_status: str = Field(description="Check status")
    records_checked: int = Field(default=0, ge=0, description="Records checked")
    records_passed: int = Field(default=0, ge=0, description="Records passed")
    records_failed: int = Field(default=0, ge=0, description="Records failed")
    pass_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Pass rate")
    quality_score: Optional[float] = Field(default=None, ge=0, le=1, description="Quality score")
    check_result: Optional[Dict[str, Any]] = Field(default=None, description="Check result")
    failed_records: Optional[List[Dict[str, Any]]] = Field(default=None, description="Failed records")
    error_message: Optional[str] = Field(default=None, description="Error message")
    execution_job_id: Optional[str] = Field(default=None, max_length=255, description="Execution job ID")
    execution_duration_sec: Optional[int] = Field(default=None, ge=0, description="Execution duration in seconds")

    @field_validator("check_status")
    @classmethod
    def validate_check_status(cls, v: str) -> str:
        """Validate check status"""
        allowed_statuses = ["PASSED", "FAILED", "WARNING", "ERROR"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "check_status")

    @field_validator("pass_rate", "quality_score")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class QualityCheck(QualityCheckBase):
    """Quality check schema (read)"""

    check_date: date = Field(description="Check date (generated from check_timestamp)")


class QualityCheckCreate(QualityCheckBase):
    """Quality check creation schema"""

    check_id: Optional[UUID] = Field(default=None, description="Check ID (auto-generated if not provided)")
    check_date: date = Field(description="Check date (required for partitioning)")

    @field_validator("check_id", mode="before")
    @classmethod
    def generate_check_id(cls, v: Optional[UUID]) -> UUID:
        """Generate check ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "check_id")


class QualityCheckUpdate(BaseEntityWithCompanyBrand):
    """Quality check update schema"""

    check_status: Optional[str] = Field(default=None, description="Check status")
    records_checked: Optional[int] = Field(default=None, ge=0, description="Records checked")
    records_passed: Optional[int] = Field(default=None, ge=0, description="Records passed")
    records_failed: Optional[int] = Field(default=None, ge=0, description="Records failed")
    pass_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Pass rate")
    quality_score: Optional[float] = Field(default=None, ge=0, le=1, description="Quality score")
    error_message: Optional[str] = Field(default=None, description="Error message")

    @field_validator("check_status")
    @classmethod
    def validate_check_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate check status"""
        if v is None:
            return None
        allowed_statuses = ["PASSED", "FAILED", "WARNING", "ERROR"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "check_status")

