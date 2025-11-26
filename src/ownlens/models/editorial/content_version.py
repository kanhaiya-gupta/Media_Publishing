"""
OwnLens - Editorial Domain: Content Version Schema

Content version validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ContentVersionBase(BaseEntityWithCompanyBrand):
    """Base content version schema"""

    version_id: UUID = Field(description="Version ID")
    article_id: UUID = Field(description="Article ID")
    content_id: UUID = Field(description="Content ID")

    # Version details
    version_number: int = Field(ge=1, description="Version number")
    version_type: str = Field(description="Version type")
    version_status: str = Field(default="draft", description="Version status")

    # Version metadata
    version_name: Optional[str] = Field(default=None, max_length=255, description="Version name")
    version_description: Optional[str] = Field(default=None, description="Version description")
    change_summary: Optional[str] = Field(default=None, description="Change summary")

    # Changes tracking
    changed_fields: Optional[List[str]] = Field(default=None, description="Changed field names")
    changes_json: Dict[str, Any] = Field(default_factory=dict, description="Detailed changes")

    # Who and when
    created_by: Optional[UUID] = Field(default=None, description="Created by")
    reviewed_by: Optional[UUID] = Field(default=None, description="Reviewed by")
    approved_by: Optional[UUID] = Field(default=None, description="Approved by")
    published_by: Optional[UUID] = Field(default=None, description="Published by")

    # Timestamps
    reviewed_at: Optional[datetime] = Field(default=None, description="Reviewed at")
    approved_at: Optional[datetime] = Field(default=None, description="Approved at")
    published_at: Optional[datetime] = Field(default=None, description="Published at")

    # Version comparison
    previous_version_id: Optional[UUID] = Field(default=None, description="Previous version ID")
    diff_summary: Optional[str] = Field(default=None, description="Text diff summary")

    @field_validator("version_type")
    @classmethod
    def validate_version_type(cls, v: str) -> str:
        """Validate version type"""
        allowed_types = ["draft", "revision", "correction", "update", "republish"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "version_type")

    @field_validator("version_status")
    @classmethod
    def validate_version_status(cls, v: str) -> str:
        """Validate version status"""
        allowed_statuses = ["draft", "review", "approved", "published", "rejected"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "version_status")


class ContentVersion(ContentVersionBase):
    """Content version schema (read)"""

    pass


class ContentVersionCreate(ContentVersionBase):
    """Content version creation schema"""

    version_id: Optional[UUID] = Field(default=None, description="Version ID (auto-generated if not provided)")

    @field_validator("version_id", mode="before")
    @classmethod
    def generate_version_id(cls, v: Optional[UUID]) -> UUID:
        """Generate version ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "version_id")


class ContentVersionUpdate(BaseEntityWithCompanyBrand):
    """Content version update schema"""

    version_status: Optional[str] = Field(default=None, description="Version status")
    version_description: Optional[str] = Field(default=None, description="Version description")
    change_summary: Optional[str] = Field(default=None, description="Change summary")
    reviewed_at: Optional[datetime] = Field(default=None, description="Reviewed at")
    approved_at: Optional[datetime] = Field(default=None, description="Approved at")
    published_at: Optional[datetime] = Field(default=None, description="Published at")

    @field_validator("version_status")
    @classmethod
    def validate_version_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate version status"""
        if v is None:
            return None
        allowed_statuses = ["draft", "review", "approved", "published", "rejected"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "version_status")

