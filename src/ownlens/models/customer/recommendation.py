"""
OwnLens - Customer Domain: Recommendation Schema

Content recommendation validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class RecommendationBase(BaseEntityWithCompanyBrand):
    """Base recommendation schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    recommendation_id: UUID = Field(description="Recommendation ID")
    user_id: UUID = Field(description="User ID")
    article_id: str = Field(max_length=255, description="Recommended article ID")
    category_id: Optional[UUID] = Field(default=None, description="Category ID")

    # Recommendation details
    recommendation_type: Optional[str] = Field(default=None, description="Recommendation type")
    recommendation_score: float = Field(ge=0, le=1, description="Recommendation strength (0-1)")
    rank_position: Optional[int] = Field(default=None, ge=1, description="Position in recommendation list")

    # Model information
    model_version: Optional[str] = Field(default=None, max_length=50, description="Model version")
    algorithm: Optional[str] = Field(default=None, max_length=100, description="Algorithm used")

    # User interaction
    was_shown: bool = Field(default=False, description="Was recommendation shown to user")
    was_clicked: bool = Field(default=False, description="Was recommendation clicked")
    was_viewed: bool = Field(default=False, description="Was recommendation viewed")
    clicked_at: Optional[datetime] = Field(default=None, description="When recommendation was clicked")
    viewed_at: Optional[datetime] = Field(default=None, description="When recommendation was viewed")

    # Expiration
    expires_at: Optional[datetime] = Field(default=None, description="Recommendation expiration")

    @field_validator("recommendation_score")
    @classmethod
    def validate_percentage(cls, v: float) -> float:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)

    @field_validator("recommendation_type")
    @classmethod
    def validate_recommendation_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate recommendation type"""
        if v is None:
            return None
        allowed_types = ["collaborative", "content_based", "hybrid"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "recommendation_type")


class Recommendation(RecommendationBase):
    """Recommendation schema (read)"""

    pass


class RecommendationCreate(RecommendationBase):
    """Recommendation creation schema"""

    recommendation_id: Optional[UUID] = Field(default=None, description="Recommendation ID (auto-generated if not provided)")

    @field_validator("recommendation_id", mode="before")
    @classmethod
    def generate_recommendation_id(cls, v: Optional[UUID]) -> UUID:
        """Generate recommendation ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "recommendation_id")


class RecommendationUpdate(BaseEntityWithCompanyBrand):
    """Recommendation update schema"""

    was_shown: Optional[bool] = Field(default=None, description="Was recommendation shown")
    was_clicked: Optional[bool] = Field(default=None, description="Was recommendation clicked")
    was_viewed: Optional[bool] = Field(default=None, description="Was recommendation viewed")
    clicked_at: Optional[datetime] = Field(default=None, description="When recommendation was clicked")
    viewed_at: Optional[datetime] = Field(default=None, description="When recommendation was viewed")

