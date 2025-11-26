"""
OwnLens - Company Domain: Content Event Model

Company content event validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, FieldValidationInfo, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ContentEventBase(BaseEntityWithCompanyBrand):
    """Base content event schema"""

    event_id: UUID = Field(description="Event ID")
    content_id: UUID = Field(description="Content ID")
    employee_id: Optional[UUID] = Field(default=None, description="Employee ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID")
    event_type: str = Field(max_length=50, description="Event type")
    event_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    department_id: Optional[UUID] = Field(default=None, description="Department ID")
    job_level: Optional[str] = Field(default=None, max_length=50, description="Job level")
    country_code: Optional[str] = Field(default=None, max_length=2, description="Country code")
    city_id: Optional[UUID] = Field(default=None, description="City ID")
    device_type_id: Optional[UUID] = Field(default=None, description="Device type ID")
    os_id: Optional[UUID] = Field(default=None, description="OS ID")
    browser_id: Optional[UUID] = Field(default=None, description="Browser ID")
    engagement_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Engagement metrics")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type"""
        allowed_types = ["content_view", "content_click", "share", "comment", "like", "download"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "event_type")

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
    event_date: Optional[date] = Field(default=None, description="Event date (generated from event_timestamp if not provided)")

    @field_validator("event_id", mode="before")
    @classmethod
    def generate_event_id(cls, v: Optional[UUID]) -> UUID:
        """Generate event ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "event_id")
    
    @field_validator("event_date", mode="before")
    @classmethod
    def generate_event_date(cls, v: Any, info: FieldValidationInfo) -> date:
        """Generate event_date from event_timestamp if not provided, or parse string date"""
        # If already a date, return it
        if isinstance(v, date):
            return v
        
        # If it's a string, try to parse it
        if isinstance(v, str):
            try:
                # Try parsing as date string (YYYY-MM-DD)
                return datetime.strptime(v, "%Y-%m-%d").date()
            except:
                try:
                    # Try parsing as ISO datetime string
                    return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
                except:
                    pass
        
        # If not provided, try to get from event_timestamp
        if "event_timestamp" in info.data:
            event_timestamp = info.data["event_timestamp"]
            if isinstance(event_timestamp, datetime):
                return event_timestamp.date()
            elif isinstance(event_timestamp, str):
                # Parse ISO format string
                try:
                    dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
                    return dt.date()
                except:
                    pass
        
        # Fallback to today's date
        return date.today()


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
        allowed_types = ["content_view", "content_click", "share", "comment", "like", "download"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "event_type")

