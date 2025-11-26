"""
OwnLens - Customer Domain: User Segment Schema

User segmentation validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class UserSegmentBase(BaseEntityWithCompanyBrand):
    """Base user segment schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    segment_id: UUID = Field(description="Segment ID")
    segment_name: str = Field(max_length=100, description="Segment name")
    segment_code: str = Field(max_length=50, description="Segment code")
    cluster_number: Optional[int] = Field(default=None, ge=0, description="K-means cluster number")
    description: Optional[str] = Field(default=None, description="Segment description")

    # Segment metrics
    user_count: int = Field(default=0, ge=0, description="Number of users in segment")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    avg_sessions: Optional[float] = Field(default=None, ge=0, description="Average sessions")
    avg_duration_sec: Optional[float] = Field(default=None, ge=0, description="Average duration in seconds")
    newsletter_signup_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Newsletter signup rate")

    # Model information
    model_version: Optional[str] = Field(default=None, max_length=50, description="ML model version used")

    # Status
    is_active: bool = Field(default=True, description="Is segment active")

    @field_validator("segment_code")
    @classmethod
    def validate_segment_code(cls, v: str) -> str:
        """Validate segment code format"""
        return FieldValidators.validate_code(v, "segment_code")

    @field_validator("newsletter_signup_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class UserSegment(UserSegmentBase):
    """User segment schema (read)"""

    pass


class UserSegmentCreate(UserSegmentBase):
    """User segment creation schema"""

    segment_id: Optional[UUID] = Field(default=None, description="Segment ID (auto-generated if not provided)")

    @field_validator("segment_id", mode="before")
    @classmethod
    def generate_segment_id(cls, v: Optional[UUID]) -> UUID:
        """Generate segment ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "segment_id")


class UserSegmentUpdate(BaseEntityWithCompanyBrand):
    """User segment update schema"""

    segment_name: Optional[str] = Field(default=None, max_length=100, description="Segment name")
    description: Optional[str] = Field(default=None, description="Segment description")
    user_count: Optional[int] = Field(default=None, ge=0, description="Number of users in segment")
    is_active: Optional[bool] = Field(default=None, description="Is segment active")


class UserSegmentAssignmentBase(BaseEntityWithCompanyBrand):
    """Base user segment assignment schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    assignment_id: UUID = Field(description="Assignment ID")
    user_id: UUID = Field(description="User ID")
    segment_id: UUID = Field(description="Segment ID")
    assignment_date: date = Field(description="Assignment date")
    confidence_score: Optional[float] = Field(default=None, ge=0, le=1, description="Model confidence score")
    model_version: Optional[str] = Field(default=None, max_length=50, description="Model version")
    is_current: bool = Field(default=True, description="Is current assignment")

    @field_validator("confidence_score")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)


class UserSegmentAssignment(UserSegmentAssignmentBase):
    """User segment assignment schema (read)"""

    pass


class UserSegmentAssignmentCreate(UserSegmentAssignmentBase):
    """User segment assignment creation schema"""

    assignment_id: Optional[UUID] = Field(default=None, description="Assignment ID (auto-generated if not provided)")

    @field_validator("assignment_id", mode="before")
    @classmethod
    def generate_assignment_id(cls, v: Optional[UUID]) -> UUID:
        """Generate assignment ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "assignment_id")

