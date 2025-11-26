"""
OwnLens - Base Domain: Brand Model

Brand validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin, MetadataMixin
from ..field_validations import FieldValidators


class BrandBase(BaseEntity, TimestampMixin, MetadataMixin):
    """Base brand schema"""

    brand_id: UUID = Field(description="Brand ID")
    company_id: UUID = Field(description="Company ID")
    brand_name: str = Field(max_length=255, description="Brand name")
    brand_code: str = Field(max_length=50, description="Brand code")
    brand_type: Optional[str] = Field(default=None, max_length=50, description="Brand type")
    description: Optional[str] = Field(default=None, description="Description")
    website_url: Optional[str] = Field(default=None, max_length=500, description="Website URL")
    logo_url: Optional[str] = Field(default=None, max_length=500, description="Logo URL")
    primary_language_code: Optional[str] = Field(default=None, max_length=5, description="Primary language code")
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    launch_date: Optional[date] = Field(default=None, description="Launch date")
    is_active: bool = Field(default=True, description="Is active")
    created_by: Optional[str] = Field(default=None, max_length=100, description="Created by")
    updated_by: Optional[str] = Field(default=None, max_length=100, description="Updated by")

    @field_validator("brand_code")
    @classmethod
    def validate_brand_code(cls, v: str) -> str:
        """Validate brand code format"""
        return FieldValidators.validate_code(v, "brand_code")

    @field_validator("brand_type")
    @classmethod
    def validate_brand_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate brand type"""
        if v is None:
            return None
        allowed_types = ["newspaper", "magazine", "digital", "tv", "radio"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "brand_type")

    @field_validator("primary_language_code")
    @classmethod
    def validate_language_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate language code"""
        return FieldValidators.validate_language_code(v)

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("website_url", "logo_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate URL format"""
        return FieldValidators.validate_url(v)


class Brand(BrandBase):
    """Brand schema (read)"""

    pass


class BrandCreate(BrandBase):
    """Brand creation schema"""

    brand_id: Optional[UUID] = Field(default=None, description="Brand ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_brand_id(cls, data: Any) -> Any:
        """Ensure brand_id is generated if missing or None"""
        if isinstance(data, dict):
            if "brand_id" not in data or data.get("brand_id") is None or data.get("brand_id") == "":
                from uuid import uuid4
                data["brand_id"] = uuid4()
        return data

    @field_validator("brand_id", mode="before")
    @classmethod
    def validate_brand_id(cls, v: Any) -> UUID:
        """Validate brand ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "brand_id")


class BrandUpdate(BaseEntity, TimestampMixin, MetadataMixin):
    """Brand update schema"""

    brand_name: Optional[str] = Field(default=None, max_length=255, description="Brand name")
    brand_type: Optional[str] = Field(default=None, max_length=50, description="Brand type")
    description: Optional[str] = Field(default=None, description="Description")
    website_url: Optional[str] = Field(default=None, max_length=500, description="Website URL")
    logo_url: Optional[str] = Field(default=None, max_length=500, description="Logo URL")
    primary_language_code: Optional[str] = Field(default=None, max_length=5, description="Primary language code")
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    launch_date: Optional[date] = Field(default=None, description="Launch date")
    is_active: Optional[bool] = Field(default=None, description="Is active")
    updated_by: Optional[str] = Field(default=None, max_length=100, description="Updated by")

    @field_validator("brand_type")
    @classmethod
    def validate_brand_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate brand type"""
        if v is None:
            return None
        allowed_types = ["newspaper", "magazine", "digital", "tv", "radio"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "brand_type")

    @field_validator("primary_language_code")
    @classmethod
    def validate_language_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate language code"""
        return FieldValidators.validate_language_code(v)

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("website_url", "logo_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate URL format"""
        return FieldValidators.validate_url(v)

