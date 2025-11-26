"""
OwnLens - Company Domain: Department Performance Schema

Department performance validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DepartmentPerformanceBase(BaseEntityWithCompanyBrand):
    """Base department performance schema"""

    performance_id: UUID = Field(description="Performance ID")
    department_id: UUID = Field(description="Department ID")
    performance_date: date = Field(description="Performance date")

    # Content metrics
    content_published: int = Field(default=0, ge=0, description="Content published")
    content_views: int = Field(default=0, ge=0, description="Content views")
    content_engagement: int = Field(default=0, ge=0, description="Content engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")

    # Employee engagement
    active_employees: int = Field(default=0, ge=0, description="Active employees")
    employee_engagement_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Employee engagement rate (0-1)")
    avg_views_per_employee: Optional[float] = Field(default=None, ge=0, description="Average views per employee")

    # Top content
    top_content_id: Optional[UUID] = Field(default=None, description="Top content ID")
    top_content_views: Optional[int] = Field(default=None, ge=0, description="Top content views")
    top_content_engagement: Optional[float] = Field(default=None, ge=0, description="Top content engagement")

    # Geographic metrics
    views_by_country: Dict[str, Any] = Field(default_factory=dict, description="Views by country (JSON)")
    views_by_city: Dict[str, Any] = Field(default_factory=dict, description="Views by city (JSON)")

    # Ranking
    department_rank: Optional[int] = Field(default=None, ge=0, description="Department rank")
    engagement_rank: Optional[int] = Field(default=None, ge=0, description="Engagement rank")

    @field_validator("employee_engagement_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class DepartmentPerformance(DepartmentPerformanceBase):
    """Department performance schema (read)"""

    pass


class DepartmentPerformanceCreate(DepartmentPerformanceBase):
    """Department performance creation schema"""

    performance_id: Optional[UUID] = Field(default=None, description="Performance ID (auto-generated if not provided)")

    @field_validator("performance_id", mode="before")
    @classmethod
    def generate_performance_id(cls, v: Optional[UUID]) -> UUID:
        """Generate performance ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "performance_id")


class DepartmentPerformanceUpdate(BaseEntityWithCompanyBrand):
    """Department performance update schema"""

    content_published: Optional[int] = Field(default=None, ge=0, description="Content published")
    content_views: Optional[int] = Field(default=None, ge=0, description="Content views")
    content_engagement: Optional[int] = Field(default=None, ge=0, description="Content engagement")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    active_employees: Optional[int] = Field(default=None, ge=0, description="Active employees")
    employee_engagement_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Employee engagement rate")
    department_rank: Optional[int] = Field(default=None, ge=0, description="Department rank")
    engagement_rank: Optional[int] = Field(default=None, ge=0, description="Engagement rank")
