"""
OwnLens - ML Models Domain: Model Feature Model

Model feature validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class ModelFeatureBase(BaseEntity):
    """Base model feature schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    feature_id: UUID = Field(description="Feature ID")
    model_id: UUID = Field(description="Model ID")
    feature_name: str = Field(max_length=255, description="Feature name")
    feature_code: str = Field(max_length=100, description="Feature code")
    feature_type: str = Field(max_length=50, description="Feature type")
    feature_data_type: Optional[str] = Field(default=None, max_length=50, description="Feature data type")
    description: Optional[str] = Field(default=None, description="Feature description")
    is_required: bool = Field(default=True, description="Is feature required")
    default_value: Optional[str] = Field(default=None, max_length=255, description="Default value")
    feature_importance: Optional[float] = Field(default=None, ge=0, description="Feature importance")
    feature_rank: Optional[int] = Field(default=None, ge=0, description="Feature rank")
    min_value: Optional[float] = Field(default=None, description="Min value")
    max_value: Optional[float] = Field(default=None, description="Max value")
    mean_value: Optional[float] = Field(default=None, description="Mean value")
    std_value: Optional[float] = Field(default=None, ge=0, description="Std value")
    null_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Null percentage")

    @field_validator("feature_code")
    @classmethod
    def validate_feature_code(cls, v: str) -> str:
        """Validate feature code format"""
        return FieldValidators.validate_code(v, "feature_code")

    @field_validator("feature_type")
    @classmethod
    def validate_feature_type(cls, v: str) -> str:
        """Validate feature type"""
        allowed_types = ["numerical", "categorical", "boolean", "datetime", "text"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "feature_type")

    @field_validator("null_percentage")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class ModelFeature(ModelFeatureBase):
    """Model feature schema (read)"""

    pass


class ModelFeatureCreate(ModelFeatureBase):
    """Model feature creation schema"""

    feature_id: Optional[UUID] = Field(default=None, description="Feature ID (auto-generated if not provided)")

    @field_validator("feature_id", mode="before")
    @classmethod
    def generate_feature_id(cls, v: Optional[UUID]) -> UUID:
        """Generate feature ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "feature_id")


class ModelFeatureUpdate(BaseEntity):
    """Model feature update schema"""

    feature_name: Optional[str] = Field(default=None, max_length=255, description="Feature name")
    description: Optional[str] = Field(default=None, description="Feature description")
    is_required: Optional[bool] = Field(default=None, description="Is feature required")
    feature_importance: Optional[float] = Field(default=None, ge=0, description="Feature importance")
    feature_rank: Optional[int] = Field(default=None, ge=0, description="Feature rank")

