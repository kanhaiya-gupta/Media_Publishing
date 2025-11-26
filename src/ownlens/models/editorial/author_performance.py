"""
OwnLens - Editorial Domain: Author Performance Schema

Author performance validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class AuthorPerformanceBase(BaseEntityWithCompanyBrand):
    """Base author performance schema"""

    performance_id: UUID = Field(description="Performance ID")
    author_id: UUID = Field(description="Author ID")
    performance_date: date = Field(description="Performance date")

    # Publishing metrics
    articles_published: int = Field(default=0, ge=0, description="Articles published")
    articles_scheduled: int = Field(default=0, ge=0, description="Articles scheduled")
    articles_draft: int = Field(default=0, ge=0, description="Articles draft")

    # View metrics
    total_views: int = Field(default=0, ge=0, description="Total views")
    unique_readers: int = Field(default=0, ge=0, description="Unique readers")
    avg_views_per_article: Optional[float] = Field(default=None, ge=0, description="Average views per article")

    # Engagement metrics
    total_engagement: int = Field(default=0, ge=0, description="Total engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    total_shares: int = Field(default=0, ge=0, description="Total shares")
    total_comments: int = Field(default=0, ge=0, description="Total comments")

    # Best performing article
    best_article_id: Optional[UUID] = Field(default=None, description="Best article ID")
    best_article_views: Optional[int] = Field(default=None, ge=0, description="Best article views")
    best_article_engagement: Optional[float] = Field(default=None, ge=0, description="Best article engagement")

    # Category performance
    top_category_id: Optional[UUID] = Field(default=None, description="Top category ID")
    top_category_views: Optional[int] = Field(default=None, ge=0, description="Top category views")

    # Ranking
    author_rank: Optional[int] = Field(default=None, ge=0, description="Author rank")
    engagement_rank: Optional[int] = Field(default=None, ge=0, description="Engagement rank")


class AuthorPerformance(AuthorPerformanceBase):
    """Author performance schema (read)"""

    pass


class AuthorPerformanceCreate(AuthorPerformanceBase):
    """Author performance creation schema"""

    performance_id: Optional[UUID] = Field(default=None, description="Performance ID (auto-generated if not provided)")

    @field_validator("performance_id", mode="before")
    @classmethod
    def generate_performance_id(cls, v: Optional[UUID]) -> UUID:
        """Generate performance ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "performance_id")


class AuthorPerformanceUpdate(BaseEntityWithCompanyBrand):
    """Author performance update schema"""

    articles_published: Optional[int] = Field(default=None, ge=0, description="Articles published")
    total_views: Optional[int] = Field(default=None, ge=0, description="Total views")
    unique_readers: Optional[int] = Field(default=None, ge=0, description="Unique readers")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    author_rank: Optional[int] = Field(default=None, ge=0, description="Author rank")
    engagement_rank: Optional[int] = Field(default=None, ge=0, description="Engagement rank")
