"""
OwnLens - ML Models Domain: Model Prediction Model

Model prediction validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ModelPredictionBase(BaseEntityWithCompanyBrand):
    """Base model prediction schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    prediction_id: UUID = Field(description="Prediction ID")
    model_id: UUID = Field(description="Model ID")
    prediction_type: str = Field(max_length=100, description="Prediction type")
    entity_id: Optional[UUID] = Field(default=None, description="Entity ID")
    entity_type: Optional[str] = Field(default=None, max_length=100, description="Entity type")
    prediction_value: Optional[float] = Field(default=None, description="Predicted value")
    prediction_probability: Optional[float] = Field(default=None, ge=0, le=1, description="Prediction probability")
    prediction_class: Optional[str] = Field(default=None, max_length=100, description="Predicted class")
    prediction_confidence: Optional[float] = Field(default=None, ge=0, le=1, description="Prediction confidence")
    input_features: Optional[Dict[str, Any]] = Field(default=None, description="Input features")
    actual_value: Optional[float] = Field(default=None, description="Actual value")
    actual_class: Optional[str] = Field(default=None, max_length=100, description="Actual class")
    is_correct: Optional[bool] = Field(default=None, description="Is prediction correct")
    predicted_at: datetime = Field(default_factory=datetime.utcnow, description="Predicted at timestamp")
    actual_observed_at: Optional[datetime] = Field(default=None, description="Actual observed at timestamp")
    batch_id: Optional[UUID] = Field(default=None, description="Batch ID")

    @field_validator("prediction_probability", "prediction_confidence")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class ModelPrediction(ModelPredictionBase):
    """Model prediction schema (read)"""

    prediction_date: date = Field(description="Prediction date (generated from predicted_at)")


class ModelPredictionCreate(ModelPredictionBase):
    """Model prediction creation schema"""

    prediction_id: Optional[UUID] = Field(default=None, description="Prediction ID (auto-generated if not provided)")
    prediction_date: date = Field(description="Prediction date (required for partitioning)")

    @field_validator("prediction_id", mode="before")
    @classmethod
    def generate_prediction_id(cls, v: Optional[UUID]) -> UUID:
        """Generate prediction ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "prediction_id")


class ModelPredictionUpdate(BaseEntityWithCompanyBrand):
    """Model prediction update schema"""

    actual_value: Optional[float] = Field(default=None, description="Actual value")
    actual_class: Optional[str] = Field(default=None, max_length=100, description="Actual class")
    is_correct: Optional[bool] = Field(default=None, description="Is prediction correct")
    actual_observed_at: Optional[datetime] = Field(default=None, description="Actual observed at timestamp")

