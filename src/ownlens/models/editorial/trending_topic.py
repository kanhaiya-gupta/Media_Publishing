"""
OwnLens - Editorial Domain: Trending Topic Model

Trending topic validation models.
"""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class TrendingTopicBase(BaseEntityWithCompanyBrand):
    """Base trending topic schema"""

    topic_id: UUID = Field(description="Topic ID")
    topic_name: str = Field(max_length=255, description="Topic name")
    topic_keywords: Optional[List[str]] = Field(default=None, description="Topic keywords")
    trending_score: float = Field(ge=0, description="Trending score")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    growth_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Growth rate")
    articles_count: int = Field(default=0, ge=0, description="Articles count")
    period_start: datetime = Field(description="Period start")
    period_end: datetime = Field(description="Period end")
    period_type: Optional[str] = Field(default=None, max_length=50, description="Period type")
    related_category_ids: Optional[List[UUID]] = Field(default=None, description="Related category IDs")
    top_article_ids: Optional[List[UUID]] = Field(default=None, description="Top article IDs")
    trend_rank: Optional[int] = Field(default=None, ge=0, description="Trend rank")

    @field_validator("period_type")
    @classmethod
    def validate_period_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate period type"""
        if v is None:
            return None
        allowed_types = ["hour", "day", "week", "month"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "period_type")

    @field_validator("growth_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class TrendingTopic(TrendingTopicBase):
    """Trending topic schema (read)"""

    pass


class TrendingTopicCreate(TrendingTopicBase):
    """Trending topic creation schema"""

    topic_id: Optional[UUID] = Field(default=None, description="Topic ID (auto-generated if not provided)")

    @field_validator("topic_id", mode="before")
    @classmethod
    def generate_topic_id(cls, v: Optional[UUID]) -> UUID:
        """Generate topic ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "topic_id")


class TrendingTopicUpdate(BaseEntityWithCompanyBrand):
    """Trending topic update schema"""

    trending_score: Optional[float] = Field(default=None, ge=0, description="Trending score")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")
    articles_count: Optional[int] = Field(default=None, ge=0, description="Articles count")
    trend_rank: Optional[int] = Field(default=None, ge=0, description="Trend rank")

