"""
OwnLens - Company Domain: Employee Engagement Model

Employee engagement validation models.
"""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class EmployeeEngagementBase(BaseEntityWithCompanyBrand):
    """Base employee engagement schema"""

    engagement_id: UUID = Field(description="Engagement ID")
    employee_id: UUID = Field(description="Employee ID")
    department_id: Optional[UUID] = Field(default=None, description="Department ID")
    engagement_date: date = Field(description="Engagement date")

    # Engagement metrics
    content_views: int = Field(default=0, ge=0, description="Content views")
    content_interactions: int = Field(default=0, ge=0, description="Content interactions")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    time_spent_sec: int = Field(default=0, ge=0, description="Time spent (seconds)")

    # Content preferences
    preferred_content_types: Optional[List[str]] = Field(default=None, description="Preferred content types")
    preferred_categories: Optional[List[UUID]] = Field(default=None, description="Preferred category IDs")

    # Activity metrics
    active_days: int = Field(default=0, ge=0, description="Active days")
    first_interaction_time: Optional[datetime] = Field(default=None, description="First interaction time")
    last_interaction_time: Optional[datetime] = Field(default=None, description="Last interaction time")

    # Engagement level
    engagement_level: Optional[str] = Field(default=None, max_length=20, description="Engagement level")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")

    @field_validator("engagement_level")
    @classmethod
    def validate_engagement_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate engagement level"""
        if v is None:
            return None
        allowed_levels = ["low", "medium", "high", "very_high"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "engagement_level")


class EmployeeEngagement(EmployeeEngagementBase):
    """Employee engagement schema (read)"""

    pass


class EmployeeEngagementCreate(EmployeeEngagementBase):
    """Employee engagement creation schema"""

    engagement_id: Optional[UUID] = Field(default=None, description="Engagement ID (auto-generated if not provided)")

    @field_validator("engagement_id", mode="before")
    @classmethod
    def generate_engagement_id(cls, v: Optional[UUID]) -> UUID:
        """Generate engagement ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "engagement_id")


class EmployeeEngagementUpdate(BaseEntityWithCompanyBrand):
    """Employee engagement update schema"""

    content_views: Optional[int] = Field(default=None, ge=0, description="Content views")
    content_interactions: Optional[int] = Field(default=None, ge=0, description="Content interactions")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    time_spent_sec: Optional[int] = Field(default=None, ge=0, description="Time spent (seconds)")
    engagement_level: Optional[str] = Field(default=None, max_length=20, description="Engagement level")
    engagement_score: Optional[float] = Field(default=None, ge=0, description="Engagement score")

    @field_validator("engagement_level")
    @classmethod
    def validate_engagement_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate engagement level"""
        if v is None:
            return None
        allowed_levels = ["low", "medium", "high", "very_high"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "engagement_level")
