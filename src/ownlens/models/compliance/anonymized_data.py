"""
OwnLens - Compliance Domain: Anonymized Data Model

Anonymized data tracking validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class AnonymizedDataBase(BaseEntityWithCompanyBrand):
    """Base anonymized data schema"""

    anonymization_id: UUID = Field(description="Anonymization ID")
    original_table_name: str = Field(max_length=255, description="Original table name")
    original_record_id: UUID = Field(description="Original record ID")
    original_user_id: Optional[UUID] = Field(default=None, description="Original user ID")
    anonymization_method: Optional[str] = Field(default=None, max_length=100, description="Anonymization method")
    anonymization_date: datetime = Field(default_factory=datetime.utcnow, description="Anonymization date")
    anonymized_by: Optional[UUID] = Field(default=None, description="Anonymized by user ID")
    anonymization_reason: Optional[str] = Field(default=None, max_length=100, description="Anonymization reason")
    related_request_id: Optional[UUID] = Field(default=None, description="Related request ID")

    @field_validator("anonymization_method")
    @classmethod
    def validate_anonymization_method(cls, v: Optional[str]) -> Optional[str]:
        """Validate anonymization method"""
        if v is None:
            return None
        allowed_methods = ["HASH", "MASK", "RANDOMIZE", "DELETE"]
        return FieldValidators.validate_enum(v.upper(), allowed_methods, "anonymization_method")

    @field_validator("anonymization_reason")
    @classmethod
    def validate_anonymization_reason(cls, v: Optional[str]) -> Optional[str]:
        """Validate anonymization reason"""
        if v is None:
            return None
        allowed_reasons = ["GDPR_DELETION", "RETENTION_POLICY", "USER_REQUEST"]
        return FieldValidators.validate_enum(v.upper(), allowed_reasons, "anonymization_reason")


class AnonymizedData(AnonymizedDataBase):
    """Anonymized data schema (read)"""

    pass


class AnonymizedDataCreate(AnonymizedDataBase):
    """Anonymized data creation schema"""

    anonymization_id: Optional[UUID] = Field(default=None, description="Anonymization ID (auto-generated if not provided)")

    @field_validator("anonymization_id", mode="before")
    @classmethod
    def generate_anonymization_id(cls, v: Optional[UUID]) -> UUID:
        """Generate anonymization ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "anonymization_id")


class AnonymizedDataUpdate(BaseEntityWithCompanyBrand):
    """Anonymized data update schema"""

    anonymization_method: Optional[str] = Field(default=None, max_length=100, description="Anonymization method")
    anonymization_reason: Optional[str] = Field(default=None, max_length=100, description="Anonymization reason")

