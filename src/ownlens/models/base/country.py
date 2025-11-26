"""
OwnLens - Base Domain: Country Model

Country validation models.
"""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity, BaseSchema, TimestampMixin
from ..field_validations import FieldValidators


class CountryBase(BaseSchema, TimestampMixin):
    """Base country schema"""

    country_code: str = Field(max_length=2, description="Country code (ISO 3166-1 alpha-2)")
    country_name: str = Field(max_length=255, description="Country name")
    country_name_native: Optional[str] = Field(default=None, max_length=255, description="Country name (native)")
    continent_code: Optional[str] = Field(default=None, max_length=2, description="Continent code")
    region: Optional[str] = Field(default=None, max_length=100, description="Region")
    timezone: Optional[str] = Field(default=None, max_length=50, description="Timezone")
    currency_code: Optional[str] = Field(default=None, max_length=3, description="Currency code (ISO 4217)")
    language_codes: Optional[List[str]] = Field(default=None, max_length=5, description="Language codes (ISO 639-1)")
    population: Optional[int] = Field(default=None, ge=0, description="Population")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("language_codes")
    @classmethod
    def validate_language_codes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate language codes"""
        if v is None:
            return None
        return [FieldValidators.validate_language_code(code) for code in v]

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)


class Country(CountryBase):
    """Country schema (read)"""

    pass


class CountryCreate(CountryBase):
    """Country creation schema"""

    pass


class CountryUpdate(BaseSchema, TimestampMixin):
    """Country update schema"""

    country_name: Optional[str] = Field(default=None, max_length=255, description="Country name")
    country_name_native: Optional[str] = Field(default=None, max_length=255, description="Country name (native)")
    continent_code: Optional[str] = Field(default=None, max_length=2, description="Continent code")
    region: Optional[str] = Field(default=None, max_length=100, description="Region")
    timezone: Optional[str] = Field(default=None, max_length=50, description="Timezone")
    currency_code: Optional[str] = Field(default=None, max_length=3, description="Currency code")
    language_codes: Optional[List[str]] = Field(default=None, max_length=5, description="Language codes")
    population: Optional[int] = Field(default=None, ge=0, description="Population")
    is_active: Optional[bool] = Field(default=None, description="Is active")

    @field_validator("language_codes")
    @classmethod
    def validate_language_codes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate language codes"""
        if v is None:
            return None
        return [FieldValidators.validate_language_code(code) for code in v]

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)

