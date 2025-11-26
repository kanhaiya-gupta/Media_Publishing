"""
OwnLens - Company Domain: Communications Analytics Schema

Communications analytics validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class CommunicationsAnalyticsBase(BaseEntityWithCompanyBrand):
    """Base communications analytics schema"""

    analytics_id: UUID = Field(description="Analytics ID")
    analytics_date: date = Field(description="Analytics date")

    # Content metrics
    total_content_published: int = Field(default=0, ge=0, description="Total content published")
    total_content_views: int = Field(default=0, ge=0, description="Total content views")
    unique_employees_reached: int = Field(default=0, ge=0, description="Unique employees reached")
    reach_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Reach percentage (0-1)")

    # Engagement metrics
    total_engagement: int = Field(default=0, ge=0, description="Total engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    engagement_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Engagement rate (0-1)")

    # Department metrics
    active_departments: int = Field(default=0, ge=0, description="Active departments")
    top_department_id: Optional[UUID] = Field(default=None, description="Top department ID")

    # Content type breakdown
    views_by_content_type: Dict[str, Any] = Field(default_factory=dict, description="Views by content type (JSON)")
    engagement_by_content_type: Dict[str, Any] = Field(default_factory=dict, description="Engagement by content type (JSON)")

    # Geographic breakdown
    views_by_country: Dict[str, Any] = Field(default_factory=dict, description="Views by country (JSON)")
    views_by_city: Dict[str, Any] = Field(default_factory=dict, description="Views by city (JSON)")

    # Employee level breakdown
    views_by_level: Dict[str, Any] = Field(default_factory=dict, description="Views by level (JSON)")
    engagement_by_level: Dict[str, Any] = Field(default_factory=dict, description="Engagement by level (JSON)")

    # Trends
    views_growth_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Views growth rate (0-1)")
    engagement_growth_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Engagement growth rate (0-1)")

    @field_validator("reach_percentage", "engagement_rate", "views_growth_rate", "engagement_growth_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class CommunicationsAnalytics(CommunicationsAnalyticsBase):
    """Communications analytics schema (read)"""

    pass


class CommunicationsAnalyticsCreate(CommunicationsAnalyticsBase):
    """Communications analytics creation schema"""

    analytics_id: Optional[UUID] = Field(default=None, description="Analytics ID (auto-generated if not provided)")

    @field_validator("analytics_id", mode="before")
    @classmethod
    def generate_analytics_id(cls, v: Optional[UUID]) -> UUID:
        """Generate analytics ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "analytics_id")


class CommunicationsAnalyticsUpdate(BaseEntityWithCompanyBrand):
    """Communications analytics update schema"""

    total_content_published: Optional[int] = Field(default=None, ge=0, description="Total content published")
    total_content_views: Optional[int] = Field(default=None, ge=0, description="Total content views")
    unique_employees_reached: Optional[int] = Field(default=None, ge=0, description="Unique employees reached")
    reach_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Reach percentage")
    total_engagement: Optional[int] = Field(default=None, ge=0, description="Total engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    engagement_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Engagement rate")
