"""
OwnLens - Editorial Domain: Category Performance Schema

Category performance validation models.
"""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class CategoryPerformanceBase(BaseEntityWithCompanyBrand):
    """Base category performance schema"""

    performance_id: UUID = Field(description="Performance ID")
    category_id: UUID = Field(description="Category ID")
    performance_date: date = Field(description="Performance date")

    # Publishing metrics
    articles_published: int = Field(default=0, ge=0, description="Articles published")
    articles_scheduled: int = Field(default=0, ge=0, description="Articles scheduled")

    # View metrics
    total_views: int = Field(default=0, ge=0, description="Total views")
    unique_readers: int = Field(default=0, ge=0, description="Unique readers")
    avg_views_per_article: Optional[float] = Field(default=None, ge=0, description="Average views per article")

    # Engagement metrics
    total_engagement: int = Field(default=0, ge=0, description="Total engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    engagement_growth_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Engagement growth rate (0-1)")

    # Trending status
    is_trending: bool = Field(default=False, description="Is trending")
    trending_score: Optional[float] = Field(default=None, ge=0, description="Trending score")
    trending_rank: Optional[int] = Field(default=None, ge=0, description="Trending rank")

    # Peak engagement times
    peak_hour: Optional[int] = Field(default=None, ge=0, le=23, description="Peak hour (0-23)")
    peak_day_of_week: Optional[int] = Field(default=None, ge=0, le=6, description="Peak day of week (0-6, Sunday=0)")

    # Best performing article
    best_article_id: Optional[UUID] = Field(default=None, description="Best article ID")
    best_article_views: Optional[int] = Field(default=None, ge=0, description="Best article views")

    # Top authors in category
    top_author_ids: Optional[List[UUID]] = Field(default=None, description="Top author IDs")

    @field_validator("engagement_growth_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class CategoryPerformance(CategoryPerformanceBase):
    """Category performance schema (read)"""

    pass


class CategoryPerformanceCreate(CategoryPerformanceBase):
    """Category performance creation schema"""

    performance_id: Optional[UUID] = Field(default=None, description="Performance ID (auto-generated if not provided)")

    @field_validator("performance_id", mode="before")
    @classmethod
    def generate_performance_id(cls, v: Optional[UUID]) -> UUID:
        """Generate performance ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "performance_id")


class CategoryPerformanceUpdate(BaseEntityWithCompanyBrand):
    """Category performance update schema"""

    articles_published: Optional[int] = Field(default=None, ge=0, description="Articles published")
    total_views: Optional[int] = Field(default=None, ge=0, description="Total views")
    unique_readers: Optional[int] = Field(default=None, ge=0, description="Unique readers")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    is_trending: Optional[bool] = Field(default=None, description="Is trending")
    trending_score: Optional[float] = Field(default=None, ge=0, description="Trending score")
    trending_rank: Optional[int] = Field(default=None, ge=0, description="Trending rank")
