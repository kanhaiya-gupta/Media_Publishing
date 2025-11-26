"""
OwnLens - Editorial Domain: Article Performance Schema

Article performance validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ArticlePerformanceBase(BaseEntityWithCompanyBrand):
    """Base article performance schema"""

    performance_id: UUID = Field(description="Performance ID")
    article_id: UUID = Field(description="Article ID")
    performance_date: date = Field(description="Performance date")

    # View metrics
    total_views: int = Field(default=0, ge=0, description="Total views")
    unique_views: int = Field(default=0, ge=0, description="Unique views")
    unique_readers: int = Field(default=0, ge=0, description="Unique readers")

    # Engagement metrics
    total_clicks: int = Field(default=0, ge=0, description="Total clicks")
    total_shares: int = Field(default=0, ge=0, description="Total shares")
    total_comments: int = Field(default=0, ge=0, description="Total comments")
    total_likes: int = Field(default=0, ge=0, description="Total likes")
    total_bookmarks: int = Field(default=0, ge=0, description="Total bookmarks")

    # Reading metrics
    avg_time_on_page_sec: Optional[int] = Field(default=None, ge=0, description="Average time on page (seconds)")
    avg_scroll_depth: Optional[int] = Field(default=None, ge=0, le=100, description="Average scroll depth (percentage)")
    completion_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Completion rate (0-1)")
    bounce_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Bounce rate (0-1)")

    # Video metrics (if applicable)
    video_plays: int = Field(default=0, ge=0, description="Video plays")
    video_completions: int = Field(default=0, ge=0, description="Video completions")
    avg_video_watch_time_sec: Optional[int] = Field(default=None, ge=0, description="Average video watch time (seconds)")

    # Referral metrics
    direct_views: int = Field(default=0, ge=0, description="Direct views")
    search_views: int = Field(default=0, ge=0, description="Search views")
    social_views: int = Field(default=0, ge=0, description="Social views")
    newsletter_views: int = Field(default=0, ge=0, description="Newsletter views")
    internal_views: int = Field(default=0, ge=0, description="Internal views")

    # Geographic metrics
    top_countries: Dict[str, Any] = Field(default_factory=dict, description="Top countries (JSON)")
    top_cities: Dict[str, Any] = Field(default_factory=dict, description="Top cities (JSON)")

    # Device metrics
    desktop_views: int = Field(default=0, ge=0, description="Desktop views")
    mobile_views: int = Field(default=0, ge=0, description="Mobile views")
    tablet_views: int = Field(default=0, ge=0, description="Tablet views")

    # Engagement score
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    quality_score: Optional[float] = Field(default=None, ge=0, description="Quality score (ML-predicted)")

    # Revenue metrics
    ad_revenue: float = Field(default=0, ge=0, description="Ad revenue")
    subscription_conversions: int = Field(default=0, ge=0, description="Subscription conversions")
    newsletter_signups: int = Field(default=0, ge=0, description="Newsletter signups")

    @field_validator("completion_rate", "bounce_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class ArticlePerformance(ArticlePerformanceBase):
    """Article performance schema (read)"""

    pass


class ArticlePerformanceCreate(ArticlePerformanceBase):
    """Article performance creation schema"""

    performance_id: Optional[UUID] = Field(default=None, description="Performance ID (auto-generated if not provided)")

    @field_validator("performance_id", mode="before")
    @classmethod
    def generate_performance_id(cls, v: Optional[UUID]) -> UUID:
        """Generate performance ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "performance_id")


class ArticlePerformanceUpdate(BaseEntityWithCompanyBrand):
    """Article performance update schema"""

    total_views: Optional[int] = Field(default=None, ge=0, description="Total views")
    unique_views: Optional[int] = Field(default=None, ge=0, description="Unique views")
    unique_readers: Optional[int] = Field(default=None, ge=0, description="Unique readers")
    total_clicks: Optional[int] = Field(default=None, ge=0, description="Total clicks")
    total_shares: Optional[int] = Field(default=None, ge=0, description="Total shares")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    quality_score: Optional[float] = Field(default=None, ge=0, description="Quality score")
