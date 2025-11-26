"""
OwnLens - Customer Domain: Churn Prediction Schema

Churn prediction validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ChurnPredictionBase(BaseEntityWithCompanyBrand):
    """Base churn prediction schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    prediction_id: UUID = Field(description="Prediction ID")
    user_id: UUID = Field(description="User ID")
    prediction_date: date = Field(description="Prediction date")

    # Prediction results
    churn_probability: float = Field(ge=0, le=1, description="Churn probability (0-1)")
    churn_risk_level: Optional[str] = Field(default=None, description="Churn risk level")
    is_churned: bool = Field(default=False, description="Actual churn status")
    churned_date: Optional[date] = Field(default=None, description="Actual churn date")

    # Model information
    model_version: str = Field(max_length=50, description="Model version")
    model_confidence: Optional[float] = Field(default=None, ge=0, le=1, description="Model confidence score")
    feature_importance: Dict[str, Any] = Field(default_factory=dict, description="Top contributing features")

    # Key features used
    days_since_last_session: Optional[int] = Field(default=None, ge=0, description="Days since last session")
    total_sessions: Optional[int] = Field(default=None, ge=0, description="Total sessions")
    avg_engagement_score: Optional[float] = Field(default=None, ge=0, description="Average engagement score")
    subscription_tier: Optional[str] = Field(default=None, description="Subscription tier")

    @field_validator("churn_probability", "model_confidence")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)

    @field_validator("churn_risk_level")
    @classmethod
    def validate_risk_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate churn risk level"""
        if v is None:
            return None
        allowed_levels = ["low", "medium", "high", "critical"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "churn_risk_level")

    @field_validator("subscription_tier")
    @classmethod
    def validate_subscription_tier(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription tier"""
        if v is None:
            return None
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v, allowed_tiers, "subscription_tier")


class ChurnPrediction(ChurnPredictionBase):
    """Churn prediction schema (read)"""

    pass


class ChurnPredictionCreate(ChurnPredictionBase):
    """Churn prediction creation schema"""

    prediction_id: Optional[UUID] = Field(default=None, description="Prediction ID (auto-generated if not provided)")

    @field_validator("prediction_id", mode="before")
    @classmethod
    def generate_prediction_id(cls, v: Optional[UUID]) -> UUID:
        """Generate prediction ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "prediction_id")


class ChurnPredictionUpdate(BaseEntityWithCompanyBrand):
    """Churn prediction update schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    churn_probability: Optional[float] = Field(default=None, ge=0, le=1, description="Churn probability")
    churn_risk_level: Optional[str] = Field(default=None, description="Churn risk level")
    is_churned: Optional[bool] = Field(default=None, description="Actual churn status")
    churned_date: Optional[date] = Field(default=None, description="Actual churn date")
    model_confidence: Optional[float] = Field(default=None, ge=0, le=1, description="Model confidence score")

