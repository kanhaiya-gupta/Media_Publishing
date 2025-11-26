"""
OwnLens - Company Domain: Content Performance Schema

Internal content performance validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ContentPerformanceBase(BaseEntityWithCompanyBrand):
    """Base content performance schema"""

    performance_id: UUID = Field(description="Performance ID")
    content_id: UUID = Field(description="Content ID")
    performance_date: date = Field(description="Performance date")

    # View metrics
    total_views: int = Field(default=0, ge=0, description="Total views")
    unique_views: int = Field(default=0, ge=0, description="Unique views")
    unique_employees: int = Field(default=0, ge=0, description="Unique employees who viewed")

    # Engagement metrics
    total_clicks: int = Field(default=0, ge=0, description="Total clicks")
    total_shares: int = Field(default=0, ge=0, description="Total shares")
    total_comments: int = Field(default=0, ge=0, description="Total comments")
    total_likes: int = Field(default=0, ge=0, description="Total likes")
    total_bookmarks: int = Field(default=0, ge=0, description="Total bookmarks")
    total_downloads: int = Field(default=0, ge=0, description="Total downloads")

    # Reading metrics
    avg_time_on_page_sec: Optional[int] = Field(default=None, ge=0, description="Average time on page (seconds)")
    avg_scroll_depth: Optional[int] = Field(default=None, ge=0, le=100, description="Average scroll depth (percentage)")
    completion_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Completion rate (0-1)")

    # Department breakdown
    views_by_department: Dict[str, Any] = Field(default_factory=dict, description="Views by department (JSON)")
    engagement_by_department: Dict[str, Any] = Field(default_factory=dict, description="Engagement by department (JSON)")

    # Geographic breakdown
    views_by_country: Dict[str, Any] = Field(default_factory=dict, description="Views by country (JSON)")
    views_by_city: Dict[str, Any] = Field(default_factory=dict, description="Views by city (JSON)")

    # Employee level breakdown
    views_by_level: Dict[str, Any] = Field(default_factory=dict, description="Views by level (JSON)")

    # Engagement score
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    reach_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Reach percentage (0-1)")

    @field_validator("completion_rate", "reach_percentage")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class ContentPerformance(ContentPerformanceBase):
    """Content performance schema (read)"""

    pass


class ContentPerformanceCreate(ContentPerformanceBase):
    """Content performance creation schema"""

    performance_id: Optional[UUID] = Field(default=None, description="Performance ID (auto-generated if not provided)")

    @field_validator("performance_id", mode="before")
    @classmethod
    def generate_performance_id(cls, v: Optional[UUID]) -> UUID:
        """Generate performance ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "performance_id")


class ContentPerformanceUpdate(BaseEntityWithCompanyBrand):
    """Content performance update schema"""

    total_views: Optional[int] = Field(default=None, ge=0, description="Total views")
    unique_views: Optional[int] = Field(default=None, ge=0, description="Unique views")
    unique_employees: Optional[int] = Field(default=None, ge=0, description="Unique employees")
    total_clicks: Optional[int] = Field(default=None, ge=0, description="Total clicks")
    total_shares: Optional[int] = Field(default=None, ge=0, description="Total shares")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    reach_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Reach percentage")
