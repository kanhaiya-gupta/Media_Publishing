"""
OwnLens - Customer Domain: User Event Model

User event validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class UserEventBase(BaseEntityWithCompanyBrand):
    """Base user event schema"""

    event_id: UUID = Field(description="Event ID (UUID)")
    user_id: UUID = Field(description="User ID")
    session_id: UUID = Field(description="Session ID")
    event_type: str = Field(description="Event type")
    event_timestamp: datetime = Field(description="Event timestamp")

    # Content information
    article_id: Optional[str] = Field(default=None, description="Article ID")
    article_title: Optional[str] = Field(default=None, max_length=500, description="Article title")
    article_type: Optional[str] = Field(default=None, description="Article type")
    category_id: Optional[UUID] = Field(default=None, description="Category ID")
    page_url: Optional[str] = Field(default=None, max_length=1000, description="Page URL")

    # Geographic information
    country_code: Optional[str] = Field(default=None, max_length=2, description="Country code (ISO 3166-1 alpha-2)")
    city_id: Optional[UUID] = Field(default=None, description="City ID")
    timezone: Optional[str] = Field(default=None, description="Timezone")

    # Device information
    device_type_id: Optional[UUID] = Field(default=None, description="Device type ID")
    os_id: Optional[UUID] = Field(default=None, description="Operating system ID")
    browser_id: Optional[UUID] = Field(default=None, description="Browser ID")

    # Engagement metrics
    engagement_metrics: Dict[str, Any] = Field(default_factory=dict, description="Event-specific engagement metrics")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type"""
        allowed_types = [
            "article_view",
            "article_click",
            "video_play",
            "newsletter_signup",
            "subscription_prompt",
            "ad_click",
            "search",
            "navigation",
            "session_end",
            "comment",
            "share",
            "page_view",
            "scroll",
        ]
        return FieldValidators.validate_enum(v, allowed_types, "event_type")

    @field_validator("country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)

    @field_validator("page_url")
    @classmethod
    def validate_page_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate page URL"""
        return FieldValidators.validate_url(v)


class UserEvent(UserEventBase):
    """User event schema (read)"""

    event_date: date = Field(description="Event date (generated from event_timestamp)")


class UserEventCreate(UserEventBase):
    """User event creation schema"""

    event_id: Optional[UUID] = Field(default=None, description="Event ID (auto-generated if not provided)")
    event_timestamp: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Event timestamp")
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


class UserEventUpdate(BaseEntityWithCompanyBrand):
    """User event update schema"""

    event_type: Optional[str] = Field(default=None, description="Event type")
    event_timestamp: Optional[datetime] = Field(default=None, description="Event timestamp")
    article_id: Optional[str] = Field(default=None, description="Article ID")
    article_title: Optional[str] = Field(default=None, max_length=500, description="Article title")
    engagement_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Engagement metrics")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate event type"""
        if v is None:
            return None
        allowed_types = [
            "article_view",
            "article_click",
            "video_play",
            "newsletter_signup",
            "subscription_prompt",
            "ad_click",
            "search",
            "navigation",
            "session_end",
            "comment",
            "share",
            "page_view",
            "scroll",
        ]
        return FieldValidators.validate_enum(v, allowed_types, "event_type")

