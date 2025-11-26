"""
OwnLens - Editorial Domain: Media Usage Model

Media usage tracking validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class MediaUsageBase(BaseEntityWithCompanyBrand):
    """Base media usage schema"""

    usage_id: UUID = Field(description="Usage ID")
    media_id: UUID = Field(description="Media ID")
    article_id: Optional[UUID] = Field(default=None, description="Article ID")
    usage_date: date = Field(description="Usage date")
    view_count: int = Field(default=0, ge=0, description="View count")
    click_count: int = Field(default=0, ge=0, description="Click count")
    download_count: int = Field(default=0, ge=0, description="Download count")
    share_count: int = Field(default=0, ge=0, description="Share count")
    avg_view_duration_sec: Optional[int] = Field(default=None, ge=0, description="Average view duration (seconds)")
    completion_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Completion rate")

    @field_validator("completion_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class MediaUsage(MediaUsageBase):
    """Media usage schema (read)"""

    pass


class MediaUsageCreate(MediaUsageBase):
    """Media usage creation schema"""

    usage_id: Optional[UUID] = Field(default=None, description="Usage ID (auto-generated if not provided)")

    @field_validator("usage_id", mode="before")
    @classmethod
    def generate_usage_id(cls, v: Optional[UUID]) -> UUID:
        """Generate usage ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "usage_id")


class MediaUsageUpdate(BaseEntityWithCompanyBrand):
    """Media usage update schema"""

    view_count: Optional[int] = Field(default=None, ge=0, description="View count")
    click_count: Optional[int] = Field(default=None, ge=0, description="Click count")
    download_count: Optional[int] = Field(default=None, ge=0, description="Download count")
    share_count: Optional[int] = Field(default=None, ge=0, description="Share count")
    avg_view_duration_sec: Optional[int] = Field(default=None, ge=0, description="Average view duration (seconds)")
    completion_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Completion rate")

