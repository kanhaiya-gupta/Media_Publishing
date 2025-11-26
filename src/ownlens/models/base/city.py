"""
OwnLens - Base Domain: City Model

City validation models.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class CityBase(BaseEntity, TimestampMixin):
    """Base city schema"""

    city_id: UUID = Field(description="City ID")
    city_name: str = Field(max_length=255, description="City name")
    country_code: str = Field(max_length=2, description="Country code")
    state_province: Optional[str] = Field(default=None, max_length=100, description="State/Province")
    latitude: Optional[float] = Field(default=None, ge=-90, le=90, description="Latitude")
    longitude: Optional[float] = Field(default=None, ge=-180, le=180, description="Longitude")
    population: Optional[int] = Field(default=None, ge=0, description="Population")
    timezone: Optional[str] = Field(default=None, max_length=50, description="Timezone")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: str) -> str:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)


class City(CityBase):
    """City schema (read)"""

    pass


class CityCreate(CityBase):
    """City creation schema"""

    city_id: Optional[UUID] = Field(default=None, description="City ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_city_id(cls, data: Any) -> Any:
        """Ensure city_id is generated if missing or None"""
        if isinstance(data, dict):
            # If city_id is missing or None/empty, generate it
            if "city_id" not in data or data.get("city_id") is None or data.get("city_id") == "":
                from uuid import uuid4
                data["city_id"] = uuid4()
        return data

    @field_validator("city_id", mode="before")
    @classmethod
    def validate_city_id(cls, v: Any) -> UUID:
        """Validate city ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "city_id")


class CityUpdate(BaseEntity, TimestampMixin):
    """City update schema"""

    city_name: Optional[str] = Field(default=None, max_length=255, description="City name")
    state_province: Optional[str] = Field(default=None, max_length=100, description="State/Province")
    latitude: Optional[float] = Field(default=None, ge=-90, le=90, description="Latitude")
    longitude: Optional[float] = Field(default=None, ge=-180, le=180, description="Longitude")
    population: Optional[int] = Field(default=None, ge=0, description="Population")
    timezone: Optional[str] = Field(default=None, max_length=50, description="Timezone")
    is_active: Optional[bool] = Field(default=None, description="Is active")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)

