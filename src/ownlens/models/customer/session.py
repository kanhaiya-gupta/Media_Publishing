"""
OwnLens - Customer Domain: Session Model

User session validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, FieldValidationInfo, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class SessionBase(BaseEntityWithCompanyBrand):
    """Base session schema"""

    session_id: UUID = Field(description="Session ID (UUID)")
    user_id: UUID = Field(description="User ID")
    account_id: Optional[UUID] = Field(default=None, description="User account ID")

    # Geographic information
    country_code: Optional[str] = Field(default=None, max_length=2, description="Country code (ISO 3166-1 alpha-2)")
    city_id: Optional[UUID] = Field(default=None, description="City ID")
    timezone: Optional[str] = Field(default=None, description="Timezone")

    # Device information
    device_type_id: Optional[UUID] = Field(default=None, description="Device type ID")
    os_id: Optional[UUID] = Field(default=None, description="Operating system ID")
    browser_id: Optional[UUID] = Field(default=None, description="Browser ID")

    # Session metadata
    referrer: Optional[str] = Field(default=None, max_length=255, description="Referrer")
    user_segment: Optional[str] = Field(default=None, description="User segment")
    subscription_tier: Optional[str] = Field(default=None, description="Subscription tier")

    # Temporal metrics
    session_start: datetime = Field(description="Session start time")
    session_end: Optional[datetime] = Field(default=None, description="Session end time")
    session_duration_sec: Optional[int] = Field(default=None, ge=0, description="Session duration in seconds")

    # Event counts
    total_events: int = Field(default=0, ge=0, description="Total events in session")
    article_views: int = Field(default=0, ge=0, description="Article views")
    article_clicks: int = Field(default=0, ge=0, description="Article clicks")
    video_plays: int = Field(default=0, ge=0, description="Video plays")
    newsletter_signups: int = Field(default=0, ge=0, description="Newsletter signups")
    ad_clicks: int = Field(default=0, ge=0, description="Ad clicks")
    searches: int = Field(default=0, ge=0, description="Searches")

    # Page metrics
    pages_visited_count: int = Field(default=0, ge=0, description="Pages visited count")
    unique_pages_count: int = Field(default=0, ge=0, description="Unique pages count")
    unique_categories_count: int = Field(default=0, ge=0, description="Unique categories count")

    # JSON fields
    categories_visited: List[str] = Field(default_factory=list, description="Categories visited (JSON array)")
    article_ids_visited: List[str] = Field(default_factory=list, description="Article IDs visited (JSON array)")
    article_titles_visited: List[str] = Field(default_factory=list, description="Article titles visited (JSON array)")
    page_events: List[Dict[str, Any]] = Field(default_factory=list, description="Page events (JSON array)")

    # Engagement metrics
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    scroll_depth_avg: Optional[int] = Field(default=None, ge=0, le=100, description="Average scroll depth percentage")
    time_on_page_avg: Optional[int] = Field(default=None, ge=0, description="Average time on page in seconds")

    # Batch tracking
    batch_id: Optional[UUID] = Field(default=None, description="Batch processing ID")

    @field_validator("user_segment")
    @classmethod
    def validate_user_segment(cls, v: Optional[str]) -> Optional[str]:
        """Validate user segment"""
        if v is None:
            return None
        allowed_segments = [
            "power_user",
            "engaged",
            "casual",
            "new_visitor",
            "returning",
            "subscriber",
        ]
        return FieldValidators.validate_enum(v, allowed_segments, "user_segment")

    @field_validator("subscription_tier")
    @classmethod
    def validate_subscription_tier(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription tier"""
        if v is None:
            return None
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v, allowed_tiers, "subscription_tier")

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

    @field_validator("session_duration_sec", mode="after")
    @classmethod
    def calculate_duration(cls, v: Optional[int], info: FieldValidationInfo) -> Optional[int]:
        """Calculate session duration if not provided"""
        if v is None and "session_start" in info.data and "session_end" in info.data:
            if info.data["session_start"] and info.data["session_end"]:
                delta = info.data["session_end"] - info.data["session_start"]
                return int(delta.total_seconds())
        return v


class Session(SessionBase):
    """Session schema (read)"""

    pass


class SessionCreate(SessionBase):
    """Session creation schema"""

    session_id: Optional[UUID] = Field(default=None, description="Session ID (auto-generated if not provided)")
    session_start: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Session start time")

    @field_validator("session_id", mode="before")
    @classmethod
    def generate_session_id(cls, v: Optional[UUID]) -> UUID:
        """Generate session ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "session_id")


class SessionUpdate(BaseEntityWithCompanyBrand):
    """Session update schema"""

    session_end: Optional[datetime] = Field(default=None, description="Session end time")
    session_duration_sec: Optional[int] = Field(default=None, ge=0, description="Session duration in seconds")
    total_events: Optional[int] = Field(default=None, ge=0, description="Total events")
    article_views: Optional[int] = Field(default=None, ge=0, description="Article views")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")

