"""
OwnLens - Audit Domain: Data Change Model

Data change validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DataChangeBase(BaseEntityWithCompanyBrand):
    """Base data change schema"""

    change_id: UUID = Field(description="Change ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    api_key_id: Optional[UUID] = Field(default=None, description="API key ID")
    table_name: str = Field(max_length=255, description="Table name")
    record_id: Optional[UUID] = Field(default=None, description="Record ID")
    change_type: str = Field(max_length=20, description="Change type")
    old_values: Optional[Dict[str, Any]] = Field(default=None, description="Old values")
    new_values: Optional[Dict[str, Any]] = Field(default=None, description="New values")
    changed_fields: Optional[List[str]] = Field(default=None, description="Changed field names")
    change_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Change timestamp")
    transaction_id: Optional[str] = Field(default=None, max_length=255, description="Transaction ID")
    batch_id: Optional[UUID] = Field(default=None, description="Batch ID")


class DataChange(DataChangeBase):
    """Data change schema (read)"""

    change_date: date = Field(description="Change date (generated from change_timestamp)")


class DataChangeCreate(DataChangeBase):
    """Data change creation schema"""

    change_id: Optional[UUID] = Field(default=None, description="Change ID (auto-generated if not provided)")
    change_date: date = Field(description="Change date (required for partitioning)")

    @field_validator("change_id", mode="before")
    @classmethod
    def generate_change_id(cls, v: Optional[UUID]) -> UUID:
        """Generate change ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "change_id")


class DataChangeUpdate(BaseEntityWithCompanyBrand):
    """Data change update schema"""

    old_values: Optional[Dict[str, Any]] = Field(default=None, description="Old values")
    new_values: Optional[Dict[str, Any]] = Field(default=None, description="New values")
    changed_fields: Optional[List[str]] = Field(default=None, description="Changed field names")

