"""
OwnLens - Editorial Domain: Content Recommendation Model

Content strategy recommendation validation models.
"""

from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ContentRecommendationBase(BaseEntityWithCompanyBrand):
    """Base content recommendation schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    recommendation_id: UUID = Field(description="Recommendation ID")
    recommendation_type: str = Field(max_length=50, description="Recommendation type")
    recommended_topic: Optional[str] = Field(default=None, max_length=255, description="Recommended topic")
    recommended_category_id: Optional[UUID] = Field(default=None, description="Recommended category ID")
    recommended_author_id: Optional[UUID] = Field(default=None, description="Recommended author ID")
    recommended_publish_time: Optional[datetime] = Field(default=None, description="Recommended publish time")
    recommendation_score: float = Field(ge=0, le=1, description="Recommendation score (0-1)")
    estimated_engagement: Optional[float] = Field(default=None, ge=0, description="Estimated engagement")
    estimated_views: Optional[int] = Field(default=None, ge=0, description="Estimated views")
    reasoning: Optional[str] = Field(default=None, description="Reasoning for recommendation")
    similar_successful_content: List[Any] = Field(default_factory=list, description="Similar successful content (JSON array)")
    model_version: Optional[str] = Field(default=None, max_length=50, description="Model version")
    algorithm: Optional[str] = Field(default=None, max_length=100, description="Algorithm used")
    status: str = Field(default="pending", max_length=50, description="Status")
    implemented_at: Optional[datetime] = Field(default=None, description="Implemented at timestamp")
    expires_at: Optional[datetime] = Field(default=None, description="Expires at timestamp")

    @field_validator("recommendation_type")
    @classmethod
    def validate_recommendation_type(cls, v: str) -> str:
        """Validate recommendation type"""
        allowed_types = ["topic", "category", "author", "publish_time"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "recommendation_type")

    @field_validator("recommendation_score")
    @classmethod
    def validate_percentage(cls, v: float) -> float:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status"""
        allowed_statuses = ["pending", "accepted", "rejected", "implemented"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")


class ContentRecommendation(ContentRecommendationBase):
    """Content recommendation schema (read)"""

    pass


class ContentRecommendationCreate(ContentRecommendationBase):
    """Content recommendation creation schema"""

    recommendation_id: Optional[UUID] = Field(default=None, description="Recommendation ID (auto-generated if not provided)")

    @field_validator("recommendation_id", mode="before")
    @classmethod
    def generate_recommendation_id(cls, v: Optional[UUID]) -> UUID:
        """Generate recommendation ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "recommendation_id")


class ContentRecommendationUpdate(BaseEntityWithCompanyBrand):
    """Content recommendation update schema"""

    status: Optional[str] = Field(default=None, max_length=50, description="Status")
    implemented_at: Optional[datetime] = Field(default=None, description="Implemented at timestamp")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate status"""
        if v is None:
            return None
        allowed_statuses = ["pending", "accepted", "rejected", "implemented"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "status")
