"""
OwnLens - Audit Domain: Data Access Model

Data access validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DataAccessBase(BaseEntityWithCompanyBrand):
    """Base data access schema"""

    access_id: UUID = Field(description="Access ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    api_key_id: Optional[UUID] = Field(default=None, description="API key ID")
    resource_type: str = Field(max_length=100, description="Resource type")
    resource_id: Optional[UUID] = Field(default=None, description="Resource ID")
    resource_identifier: Optional[str] = Field(default=None, max_length=255, description="Resource identifier")
    access_type: str = Field(max_length=50, description="Access type")
    query_text: Optional[str] = Field(default=None, description="Query text")
    query_params: Optional[Dict[str, Any]] = Field(default=None, description="Query parameters")
    rows_returned: Optional[int] = Field(default=None, ge=0, description="Rows returned")
    execution_time_ms: Optional[int] = Field(default=None, ge=0, description="Execution time in milliseconds")
    access_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Access timestamp")
    ip_address: Optional[str] = Field(default=None, description="IP address")
    user_agent: Optional[str] = Field(default=None, description="User agent")
    endpoint: Optional[str] = Field(default=None, max_length=500, description="API endpoint")
    data_classification: Optional[str] = Field(default=None, max_length=50, description="Data classification")
    gdpr_relevant: bool = Field(default=False, description="Is GDPR relevant")

    @field_validator("access_type")
    @classmethod
    def validate_access_type(cls, v: str) -> str:
        """Validate access type"""
        allowed_types = ["QUERY", "EXPORT", "API_READ", "FILE_DOWNLOAD"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "access_type")

    @field_validator("data_classification")
    @classmethod
    def validate_data_classification(cls, v: Optional[str]) -> Optional[str]:
        """Validate data classification"""
        if v is None:
            return None
        allowed_classifications = ["public", "internal", "confidential", "restricted"]
        return FieldValidators.validate_enum(v.lower(), allowed_classifications, "data_classification")

    @field_validator("ip_address")
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        """Validate IP address"""
        return FieldValidators.validate_ip_address(v) if v else None


class DataAccess(DataAccessBase):
    """Data access schema (read)"""

    access_date: date = Field(description="Access date (generated from access_timestamp)")


class DataAccessCreate(DataAccessBase):
    """Data access creation schema"""

    access_id: Optional[UUID] = Field(default=None, description="Access ID (auto-generated if not provided)")
    access_date: date = Field(description="Access date (required for partitioning)")

    @field_validator("access_id", mode="before")
    @classmethod
    def generate_access_id(cls, v: Optional[UUID]) -> UUID:
        """Generate access ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "access_id")


class DataAccessUpdate(BaseEntityWithCompanyBrand):
    """Data access update schema"""

    rows_returned: Optional[int] = Field(default=None, ge=0, description="Rows returned")
    execution_time_ms: Optional[int] = Field(default=None, ge=0, description="Execution time in milliseconds")

