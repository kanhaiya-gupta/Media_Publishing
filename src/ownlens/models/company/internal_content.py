"""
OwnLens - Company Domain: Internal Content Schema

Internal content validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class InternalContentBase(BaseEntityWithCompanyBrand):
    """Base internal content schema"""

    content_id: UUID = Field(description="Content ID")

    # Content information
    title: str = Field(max_length=500, description="Content title")
    content_type: str = Field(description="Content type")
    summary: Optional[str] = Field(default=None, description="Content summary")
    content_body: Optional[str] = Field(default=None, description="Content body (optional, can be in object storage)")
    content_url: Optional[str] = Field(default=None, max_length=1000, description="Content URL (if stored externally)")

    # Author information
    author_employee_id: Optional[UUID] = Field(default=None, description="Author employee ID")
    author_department_id: Optional[UUID] = Field(default=None, description="Author department ID")
    department_id: Optional[UUID] = Field(default=None, description="Department ID")

    # Category and tags
    category_id: Optional[UUID] = Field(default=None, description="Category ID")
    tags: Optional[List[str]] = Field(default=None, max_length=100, description="Tags")

    # Publishing information
    publish_time: datetime = Field(description="Publish time (required)")
    expiry_date: Optional[date] = Field(default=None, description="Content expiration date")
    status: str = Field(default="draft", description="Content status")

    # Target audience
    target_departments: Optional[List[UUID]] = Field(default=None, description="Target department IDs")
    target_employee_levels: Optional[List[str]] = Field(default=None, description="Target employee levels")
    target_countries: Optional[List[str]] = Field(default=None, max_length=2, description="Target country codes")
    is_company_wide: bool = Field(default=False, description="Is company-wide content")

    # Priority
    priority_level: str = Field(default="normal", description="Priority level")
    is_important: bool = Field(default=False, description="Is important content")

    @field_validator("content_type")
    @classmethod
    def validate_content_type(cls, v: str) -> str:
        """Validate content type"""
        allowed_types = ["announcement", "newsletter", "internal_article", "policy", "update", "event"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "content_type")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate content status"""
        allowed_statuses = ["draft", "scheduled", "published", "archived", "deleted"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")

    @field_validator("priority_level")
    @classmethod
    def validate_priority_level(cls, v: str) -> str:
        """Validate priority level"""
        allowed_levels = ["low", "normal", "high", "urgent"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "priority_level")

    @field_validator("target_countries")
    @classmethod
    def validate_target_countries(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate target country codes"""
        if v is None:
            return None
        return [FieldValidators.validate_country_code(code) for code in v]

    @field_validator("content_url")
    @classmethod
    def validate_content_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate content URL"""
        return FieldValidators.validate_url(v)


class InternalContent(InternalContentBase):
    """Internal content schema (read)"""

    publish_date: date = Field(description="Publish date (generated from publish_time)")


class InternalContentCreate(InternalContentBase):
    """Internal content creation schema"""

    content_id: Optional[UUID] = Field(default=None, description="Content ID (auto-generated if not provided)")
    publish_time: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Publish time")

    @field_validator("content_id", mode="before")
    @classmethod
    def generate_content_id(cls, v: Optional[UUID]) -> UUID:
        """Generate content ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "content_id")


class InternalContentUpdate(BaseEntityWithCompanyBrand):
    """Internal content update schema"""

    title: Optional[str] = Field(default=None, max_length=500, description="Content title")
    summary: Optional[str] = Field(default=None, description="Content summary")
    content_body: Optional[str] = Field(default=None, description="Content body")
    status: Optional[str] = Field(default=None, description="Content status")
    expiry_date: Optional[date] = Field(default=None, description="Content expiration date")
    priority_level: Optional[str] = Field(default=None, description="Priority level")
    is_important: Optional[bool] = Field(default=None, description="Is important content")
    is_company_wide: Optional[bool] = Field(default=None, description="Is company-wide content")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate content status"""
        if v is None:
            return None
        allowed_statuses = ["draft", "scheduled", "published", "archived", "deleted"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")

    @field_validator("priority_level")
    @classmethod
    def validate_priority_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate priority level"""
        if v is None:
            return None
        allowed_levels = ["low", "normal", "high", "urgent"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "priority_level")

