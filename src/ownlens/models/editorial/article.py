"""
OwnLens - Editorial Domain: Article Schema

Article validation models.
"""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ArticleBase(BaseEntityWithCompanyBrand):
    """Base article schema"""

    article_id: UUID = Field(description="Article ID")
    external_article_id: Optional[str] = Field(default=None, max_length=255, description="External system article ID")

    # Article information
    title: str = Field(max_length=500, description="Article title")
    headline: Optional[str] = Field(default=None, max_length=500, description="Headline (for A/B testing)")
    subtitle: Optional[str] = Field(default=None, description="Subtitle")
    summary: Optional[str] = Field(default=None, description="Article summary")
    content_url: str = Field(max_length=1000, description="Content URL")
    article_type: Optional[str] = Field(default=None, description="Article type")

    # Author information
    primary_author_id: Optional[UUID] = Field(default=None, description="Primary author ID")
    co_author_ids: Optional[List[UUID]] = Field(default=None, description="Co-author IDs")

    # Category information
    primary_category_id: Optional[UUID] = Field(default=None, description="Primary category ID")
    secondary_category_ids: Optional[List[UUID]] = Field(default=None, description="Secondary category IDs")
    tags: Optional[List[str]] = Field(default=None, max_length=100, description="Tags")

    # Content metrics
    word_count: Optional[int] = Field(default=None, ge=0, description="Word count")
    reading_time_minutes: Optional[int] = Field(default=None, ge=0, description="Estimated reading time")
    image_count: int = Field(default=0, ge=0, description="Image count")
    video_count: int = Field(default=0, ge=0, description="Video count")

    # Publishing information
    publish_time: datetime = Field(description="Publish time")
    last_updated_time: Optional[datetime] = Field(default=None, description="Last updated time")
    status: str = Field(default="draft", description="Article status")

    # SEO information
    meta_description: Optional[str] = Field(default=None, description="Meta description")
    meta_keywords: Optional[List[str]] = Field(default=None, description="Meta keywords")
    canonical_url: Optional[str] = Field(default=None, max_length=1000, description="Canonical URL")

    # Geographic relevance
    relevant_countries: Optional[List[str]] = Field(default=None, max_length=2, description="Relevant country codes")
    relevant_cities: Optional[List[UUID]] = Field(default=None, description="Relevant city IDs")

    @field_validator("article_type")
    @classmethod
    def validate_article_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate article type"""
        if v is None:
            return None
        allowed_types = ["news", "opinion", "analysis", "feature", "gallery", "video", "listicle"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "article_type")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate article status"""
        allowed_statuses = ["draft", "scheduled", "published", "archived", "deleted"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")

    @field_validator("content_url", "canonical_url")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate URL format"""
        return FieldValidators.validate_url(v)

    @field_validator("relevant_countries")
    @classmethod
    def validate_country_codes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate country codes"""
        if v is None:
            return None
        return [FieldValidators.validate_country_code(code) for code in v]


class Article(ArticleBase):
    """Article schema (read)"""

    publish_date: date = Field(description="Publish date")


class ArticleCreate(ArticleBase):
    """Article creation schema"""

    article_id: Optional[UUID] = Field(default=None, description="Article ID (auto-generated if not provided)")
    publish_time: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Publish time")

    @field_validator("article_id", mode="before")
    @classmethod
    def generate_article_id(cls, v: Optional[UUID]) -> UUID:
        """Generate article ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "article_id")


class ArticleUpdate(BaseEntityWithCompanyBrand):
    """Article update schema"""

    title: Optional[str] = Field(default=None, max_length=500, description="Article title")
    headline: Optional[str] = Field(default=None, max_length=500, description="Headline")
    summary: Optional[str] = Field(default=None, description="Article summary")
    status: Optional[str] = Field(default=None, description="Article status")
    last_updated_time: Optional[datetime] = Field(default=None, description="Last updated time")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate article status"""
        if v is None:
            return None
        allowed_statuses = ["draft", "scheduled", "published", "archived", "deleted"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")

