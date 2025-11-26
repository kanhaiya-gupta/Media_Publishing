"""
OwnLens - Customer Domain: Conversion Prediction Schema

Conversion prediction validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ConversionPredictionBase(BaseEntityWithCompanyBrand):
    """Base conversion prediction schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    prediction_id: UUID = Field(description="Prediction ID")
    user_id: UUID = Field(description="User ID")
    prediction_date: date = Field(description="Prediction date")

    # Prediction results
    conversion_probability: float = Field(ge=0, le=1, description="Conversion probability (0-1)")
    conversion_tier: Optional[str] = Field(default=None, description="Predicted subscription tier")
    conversion_value: Optional[float] = Field(default=None, ge=0, description="Predicted lifetime value")

    # Model information
    model_version: str = Field(max_length=50, description="Model version")
    model_confidence: Optional[float] = Field(default=None, ge=0, le=1, description="Model confidence score")
    feature_importance: Dict[str, Any] = Field(default_factory=dict, description="Top contributing features")

    # Actual conversion (for validation)
    did_convert: bool = Field(default=False, description="Did user actually convert")
    converted_at: Optional[datetime] = Field(default=None, description="When user converted")
    actual_tier: Optional[str] = Field(default=None, description="Actual subscription tier")

    @field_validator("conversion_probability", "model_confidence")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0)

    @field_validator("conversion_tier", "actual_tier")
    @classmethod
    def validate_subscription_tier(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription tier"""
        if v is None:
            return None
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v, allowed_tiers, "subscription_tier")


class ConversionPrediction(ConversionPredictionBase):
    """Conversion prediction schema (read)"""

    pass


class ConversionPredictionCreate(ConversionPredictionBase):
    """Conversion prediction creation schema"""

    prediction_id: Optional[UUID] = Field(default=None, description="Prediction ID (auto-generated if not provided)")

    @field_validator("prediction_id", mode="before")
    @classmethod
    def generate_prediction_id(cls, v: Optional[UUID]) -> UUID:
        """Generate prediction ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "prediction_id")


class ConversionPredictionUpdate(BaseEntityWithCompanyBrand):
    """Conversion prediction update schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    conversion_probability: Optional[float] = Field(default=None, ge=0, le=1, description="Conversion probability")
    did_convert: Optional[bool] = Field(default=None, description="Did user actually convert")
    converted_at: Optional[datetime] = Field(default=None, description="When user converted")
    actual_tier: Optional[str] = Field(default=None, description="Actual subscription tier")

