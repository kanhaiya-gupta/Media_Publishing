"""
OwnLens - Editorial Domain: Content Event Model

Editorial content event validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ContentEventBase(BaseEntityWithCompanyBrand):
    """Base content event schema"""

    event_id: UUID = Field(description="Event ID")
    article_id: UUID = Field(description="Article ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    event_type: str = Field(max_length=50, description="Event type")
    event_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    category_id: Optional[UUID] = Field(default=None, description="Category ID")
    author_id: Optional[UUID] = Field(default=None, description="Author ID")
    country_code: Optional[str] = Field(default=None, max_length=2, description="Country code")
    city_id: Optional[UUID] = Field(default=None, description="City ID")
    device_type_id: Optional[UUID] = Field(default=None, description="Device type ID")
    os_id: Optional[UUID] = Field(default=None, description="OS ID")
    browser_id: Optional[UUID] = Field(default=None, description="Browser ID")
    referrer: Optional[str] = Field(default=None, max_length=255, description="Referrer")
    referrer_type: Optional[str] = Field(default=None, max_length=50, description="Referrer type")
    engagement_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Engagement metrics")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type"""
        allowed_types = ["article_view", "article_click", "share", "comment", "like", "bookmark"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "event_type")

    @field_validator("referrer_type")
    @classmethod
    def validate_referrer_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate referrer type"""
        if v is None:
            return None
        allowed_types = ["direct", "search", "social", "newsletter", "internal"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "referrer_type")

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)


class ContentEvent(ContentEventBase):
    """Content event schema (read)"""

    event_date: date = Field(description="Event date (generated from event_timestamp)")


class ContentEventCreate(ContentEventBase):
    """Content event creation schema"""

    event_id: Optional[UUID] = Field(default=None, description="Event ID (auto-generated if not provided)")
    event_date: Optional[date] = Field(default=None, description="Event date (auto-generated from event_timestamp if not provided)")

    @field_validator("event_id", mode="before")
    @classmethod
    def generate_event_id(cls, v: Optional[UUID]) -> UUID:
        """Generate event ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "event_id")

    @model_validator(mode="after")
    def set_event_date(self):
        """Set event_date from event_timestamp if not provided"""
        if self.event_date is None and self.event_timestamp is not None:
            self.event_date = self.event_timestamp.date()
        return self


class ContentEventUpdate(BaseEntityWithCompanyBrand):
    """Content event update schema"""

    event_type: Optional[str] = Field(default=None, max_length=50, description="Event type")
    engagement_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Engagement metrics")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate event type"""
        if v is None:
            return None
        allowed_types = ["article_view", "article_click", "share", "comment", "like", "bookmark"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "event_type")

