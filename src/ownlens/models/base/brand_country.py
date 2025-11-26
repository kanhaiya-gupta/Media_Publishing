"""
OwnLens - Base Domain: Brand Country Model

Brand country validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class BrandCountryBase(BaseEntity, TimestampMixin):
    """Base brand country schema"""

    brand_country_id: UUID = Field(description="Brand country ID")
    brand_id: UUID = Field(description="Brand ID")
    country_code: str = Field(max_length=2, description="Country code")
    is_primary: bool = Field(default=False, description="Is primary country")
    launch_date: Optional[date] = Field(default=None, description="Launch date")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)


class BrandCountry(BrandCountryBase):
    """Brand country schema (read)"""

    pass


class BrandCountryCreate(BrandCountryBase):
    """Brand country creation schema"""

    brand_country_id: Optional[UUID] = Field(default=None, description="Brand country ID (auto-generated if not provided)")

    @field_validator("brand_country_id", mode="before")
    @classmethod
    def generate_brand_country_id(cls, v: Optional[UUID]) -> UUID:
        """Generate brand country ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "brand_country_id")


class BrandCountryUpdate(BaseEntity, TimestampMixin):
    """Brand country update schema"""

    is_primary: Optional[bool] = Field(default=None, description="Is primary country")
    launch_date: Optional[date] = Field(default=None, description="Launch date")
    is_active: Optional[bool] = Field(default=None, description="Is active")

