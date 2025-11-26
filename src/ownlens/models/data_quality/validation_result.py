"""
OwnLens - Data Quality Domain: Validation Result Model

Data validation result validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ValidationResultBase(BaseEntityWithCompanyBrand):
    """Base validation result schema"""

    validation_id: UUID = Field(description="Validation ID")
    rule_id: UUID = Field(description="Rule ID")
    table_name: str = Field(max_length=255, description="Table name")
    record_id: UUID = Field(description="Record ID")
    column_name: Optional[str] = Field(default=None, max_length=255, description="Column name")
    validation_status: str = Field(description="Validation status")
    validation_message: Optional[str] = Field(default=None, description="Validation message")
    validation_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Validation timestamp")
    record_data: Optional[Dict[str, Any]] = Field(default=None, description="Record data snapshot")
    batch_id: Optional[UUID] = Field(default=None, description="Batch ID")

    @field_validator("validation_status")
    @classmethod
    def validate_validation_status(cls, v: str) -> str:
        """Validate validation status"""
        allowed_statuses = ["PASSED", "FAILED", "WARNING"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "validation_status")


class ValidationResult(ValidationResultBase):
    """Validation result schema (read)"""

    validation_date: date = Field(description="Validation date (generated from validation_timestamp)")


class ValidationResultCreate(ValidationResultBase):
    """Validation result creation schema"""

    validation_id: Optional[UUID] = Field(default=None, description="Validation ID (auto-generated if not provided)")
    validation_date: date = Field(description="Validation date (required for partitioning)")

    @field_validator("validation_id", mode="before")
    @classmethod
    def generate_validation_id(cls, v: Optional[UUID]) -> UUID:
        """Generate validation ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "validation_id")


class ValidationResultUpdate(BaseEntityWithCompanyBrand):
    """Validation result update schema"""

    validation_status: Optional[str] = Field(default=None, description="Validation status")
    validation_message: Optional[str] = Field(default=None, description="Validation message")

    @field_validator("validation_status")
    @classmethod
    def validate_validation_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate validation status"""
        if v is None:
            return None
        allowed_statuses = ["PASSED", "FAILED", "WARNING"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "validation_status")

