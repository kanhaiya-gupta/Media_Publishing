"""
OwnLens - Customer Domain: User Features Schema

ML-ready user features validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class UserFeaturesBase(BaseEntityWithCompanyBrand):
    """Base user features schema"""

    user_feature_id: UUID = Field(description="User feature ID")
    user_id: UUID = Field(description="User ID")
    feature_date: date = Field(description="Date when features were calculated")

    # Behavioral features
    total_sessions: int = Field(default=0, ge=0, description="Total sessions")
    avg_session_duration_sec: Optional[float] = Field(default=None, ge=0, description="Average session duration")
    min_session_duration_sec: Optional[int] = Field(default=None, ge=0, description="Min session duration")
    max_session_duration_sec: Optional[int] = Field(default=None, ge=0, description="Max session duration")
    std_session_duration_sec: Optional[float] = Field(default=None, ge=0, description="Std session duration")
    avg_events_per_session: Optional[float] = Field(default=None, ge=0, description="Average events per session")
    avg_pages_per_session: Optional[float] = Field(default=None, ge=0, description="Average pages per session")
    avg_categories_diversity: Optional[float] = Field(default=None, ge=0, description="Average categories diversity")

    # Engagement features
    avg_article_views: Optional[float] = Field(default=None, ge=0, description="Average article views")
    avg_article_clicks: Optional[float] = Field(default=None, ge=0, description="Average article clicks")
    avg_video_plays: Optional[float] = Field(default=None, ge=0, description="Average video plays")
    avg_searches: Optional[float] = Field(default=None, ge=0, description="Average searches")
    total_newsletter_signups: int = Field(default=0, ge=0, description="Total newsletter signups")
    total_ad_clicks: int = Field(default=0, ge=0, description="Total ad clicks")

    # Engagement score
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    max_engagement_score: Optional[float] = Field(default=None, ge=0, description="Max engagement score")
    min_engagement_score: Optional[float] = Field(default=None, ge=0, description="Min engagement score")

    # Conversion features
    has_newsletter: bool = Field(default=False, description="Has newsletter")
    newsletter_signup_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Newsletter signup rate")

    # Content features
    preferred_brand_id: Optional[UUID] = Field(default=None, description="Preferred brand ID")
    preferred_category_id: Optional[UUID] = Field(default=None, description="Preferred category ID")
    preferred_device_type_id: Optional[UUID] = Field(default=None, description="Preferred device type ID")
    preferred_os_id: Optional[UUID] = Field(default=None, description="Preferred OS ID")
    preferred_browser_id: Optional[UUID] = Field(default=None, description="Preferred browser ID")

    # Geographic features
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    primary_city_id: Optional[UUID] = Field(default=None, description="Primary city ID")

    # Subscription features
    current_subscription_tier: Optional[str] = Field(default=None, description="Current subscription tier")
    current_user_segment: Optional[str] = Field(default=None, description="Current user segment")

    # Temporal features
    first_session_date: Optional[date] = Field(default=None, description="First session date")
    last_session_date: Optional[date] = Field(default=None, description="Last session date")
    days_active: Optional[int] = Field(default=None, ge=0, description="Days active")

    # Recency features
    days_since_last_session: Optional[int] = Field(default=None, ge=0, description="Days since last session")
    active_today: bool = Field(default=False, description="Active today")
    active_last_7_days: bool = Field(default=False, description="Active last 7 days")
    active_last_30_days: bool = Field(default=False, description="Active last 30 days")

    # Churn prediction features
    churn_risk_score: Optional[float] = Field(default=None, ge=0, le=1, description="Churn risk score (0-1)")
    churn_probability: Optional[float] = Field(default=None, ge=0, le=1, description="Churn probability (0-1)")

    # Additional ML features
    ml_features: Dict[str, Any] = Field(default_factory=dict, description="Additional ML features (JSON)")

    @field_validator("newsletter_signup_rate", "churn_risk_score", "churn_probability")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("current_subscription_tier")
    @classmethod
    def validate_subscription_tier(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription tier"""
        if v is None:
            return None
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v, allowed_tiers, "subscription_tier")

    @field_validator("current_user_segment")
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


class UserFeatures(UserFeaturesBase):
    """User features schema (read)"""

    pass


class UserFeaturesCreate(UserFeaturesBase):
    """User features creation schema"""

    user_feature_id: Optional[UUID] = Field(default=None, description="User feature ID (auto-generated if not provided)")

    @field_validator("user_feature_id", mode="before")
    @classmethod
    def generate_user_feature_id(cls, v: Optional[UUID]) -> UUID:
        """Generate user feature ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "user_feature_id")


class UserFeaturesUpdate(BaseEntityWithCompanyBrand):
    """User features update schema"""

    total_sessions: Optional[int] = Field(default=None, ge=0, description="Total sessions")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    churn_risk_score: Optional[float] = Field(default=None, ge=0, le=1, description="Churn risk score")
    churn_probability: Optional[float] = Field(default=None, ge=0, le=1, description="Churn probability")
    ml_features: Optional[Dict[str, Any]] = Field(default=None, description="Additional ML features")

