"""
OwnLens - Editorial Domain: Article Content Schema

Article content validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ArticleContentBase(BaseEntityWithCompanyBrand):
    """Base article content schema"""

    content_id: UUID = Field(description="Content ID")
    article_id: UUID = Field(description="Article ID")

    # Content details
    content_version: int = Field(ge=1, description="Content version number")
    content_format: str = Field(default="html", description="Content format")
    content_body: str = Field(description="Full article content")
    content_body_json: Optional[Dict[str, Any]] = Field(default=None, description="Structured content (JSON)")

    # Content structure
    content_blocks: List[Dict[str, Any]] = Field(default_factory=list, description="Content blocks (JSON array)")
    content_sections: List[Dict[str, Any]] = Field(default_factory=list, description="Content sections (JSON array)")

    # Content metadata
    word_count: Optional[int] = Field(default=None, ge=0, description="Word count")
    character_count: Optional[int] = Field(default=None, ge=0, description="Character count")
    reading_time_minutes: Optional[int] = Field(default=None, ge=0, description="Estimated reading time")
    content_language_code: Optional[str] = Field(default=None, max_length=5, description="Content language code")

    # Full-text search
    search_keywords: Optional[List[str]] = Field(default=None, description="Extracted keywords for search")

    # Content status
    content_status: str = Field(default="draft", description="Content status")
    is_current_version: bool = Field(default=True, description="Is current version")
    is_published_version: bool = Field(default=False, description="Is published version")

    # Versioning
    previous_version_id: Optional[UUID] = Field(default=None, description="Previous version ID")
    version_notes: Optional[str] = Field(default=None, description="Version notes")
    version_created_by: Optional[UUID] = Field(default=None, description="Version created by")

    # Timestamps
    published_at: Optional[datetime] = Field(default=None, description="When this version was published")

    @field_validator("content_format")
    @classmethod
    def validate_content_format(cls, v: str) -> str:
        """Validate content format"""
        allowed_formats = ["html", "markdown", "json", "plain_text"]
        return FieldValidators.validate_enum(v.lower(), allowed_formats, "content_format")

    @field_validator("content_status")
    @classmethod
    def validate_content_status(cls, v: str) -> str:
        """Validate content status"""
        allowed_statuses = ["draft", "review", "approved", "published", "archived"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "content_status")

    @field_validator("content_language_code")
    @classmethod
    def validate_language_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate language code"""
        return FieldValidators.validate_language_code(v)

    @field_validator("content_body")
    @classmethod
    def validate_content_body(cls, v: str) -> str:
        """Validate content body is not empty"""
        if not v or not v.strip():
            raise ValueError("Content body cannot be empty")
        return v


class ArticleContent(ArticleContentBase):
    """Article content schema (read)"""

    search_vector: Optional[str] = Field(default=None, description="Full-text search vector (tsvector)")


class ArticleContentCreate(ArticleContentBase):
    """Article content creation schema"""

    content_id: Optional[UUID] = Field(default=None, description="Content ID (auto-generated if not provided)")

    @field_validator("content_id", mode="before")
    @classmethod
    def generate_content_id(cls, v: Optional[UUID]) -> UUID:
        """Generate content ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "content_id")


class ArticleContentUpdate(BaseEntityWithCompanyBrand):
    """Article content update schema"""

    content_body: Optional[str] = Field(default=None, description="Full article content")
    content_body_json: Optional[Dict[str, Any]] = Field(default=None, description="Structured content")
    content_blocks: Optional[List[Dict[str, Any]]] = Field(default=None, description="Content blocks")
    content_status: Optional[str] = Field(default=None, description="Content status")
    version_notes: Optional[str] = Field(default=None, description="Version notes")

    @field_validator("content_status")
    @classmethod
    def validate_content_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate content status"""
        if v is None:
            return None
        allowed_statuses = ["draft", "review", "approved", "published", "archived"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "content_status")

