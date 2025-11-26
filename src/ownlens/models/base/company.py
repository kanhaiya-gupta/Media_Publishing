"""
OwnLens - Base Domain: Company Model

Company validation models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin, MetadataMixin
from ..field_validations import FieldValidators


class CompanyBase(BaseEntity, TimestampMixin, MetadataMixin):
    """Base company schema"""

    company_id: UUID = Field(description="Company ID")
    company_name: str = Field(max_length=255, description="Company name")
    company_code: str = Field(max_length=50, description="Company code")
    legal_name: Optional[str] = Field(default=None, max_length=255, description="Legal name")
    headquarters_country_code: Optional[str] = Field(default=None, max_length=2, description="Headquarters country code")
    headquarters_city: Optional[str] = Field(default=None, max_length=100, description="Headquarters city")
    website_url: Optional[str] = Field(default=None, max_length=500, description="Website URL")
    industry: Optional[str] = Field(default=None, max_length=100, description="Industry")
    founded_year: Optional[int] = Field(default=None, ge=1000, le=9999, description="Founded year")
    employee_count: Optional[int] = Field(default=None, ge=0, description="Employee count")
    is_active: bool = Field(default=True, description="Is active")
    created_by: Optional[str] = Field(default=None, max_length=100, description="Created by")
    updated_by: Optional[str] = Field(default=None, max_length=100, description="Updated by")

    @field_validator("company_code")
    @classmethod
    def validate_company_code(cls, v: str) -> str:
        """Validate company code format"""
        return FieldValidators.validate_code(v, "company_code")

    @field_validator("headquarters_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("website_url")
    @classmethod
    def validate_website_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate website URL"""
        return FieldValidators.validate_url(v)


class Company(CompanyBase):
    """Company schema (read)"""

    pass


class CompanyCreate(CompanyBase):
    """Company creation schema"""

    company_id: Optional[UUID] = Field(default=None, description="Company ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_company_id(cls, data: Any) -> Any:
        """Ensure company_id is generated if missing or None"""
        if isinstance(data, dict):
            if "company_id" not in data or data.get("company_id") is None or data.get("company_id") == "":
                from uuid import uuid4
                data["company_id"] = uuid4()
        return data

    @field_validator("company_id", mode="before")
    @classmethod
    def validate_company_id(cls, v: Any) -> UUID:
        """Validate company ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "company_id")


class CompanyUpdate(BaseEntity, TimestampMixin, MetadataMixin):
    """Company update schema"""

    company_name: Optional[str] = Field(default=None, max_length=255, description="Company name")
    legal_name: Optional[str] = Field(default=None, max_length=255, description="Legal name")
    headquarters_country_code: Optional[str] = Field(default=None, max_length=2, description="Headquarters country code")
    headquarters_city: Optional[str] = Field(default=None, max_length=100, description="Headquarters city")
    website_url: Optional[str] = Field(default=None, max_length=500, description="Website URL")
    industry: Optional[str] = Field(default=None, max_length=100, description="Industry")
    founded_year: Optional[int] = Field(default=None, ge=1000, le=9999, description="Founded year")
    employee_count: Optional[int] = Field(default=None, ge=0, description="Employee count")
    is_active: Optional[bool] = Field(default=None, description="Is active")
    updated_by: Optional[str] = Field(default=None, max_length=100, description="Updated by")

    @field_validator("headquarters_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("website_url")
    @classmethod
    def validate_website_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate website URL"""
        return FieldValidators.validate_url(v)

