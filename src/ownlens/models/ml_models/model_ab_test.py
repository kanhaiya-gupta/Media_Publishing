"""
OwnLens - ML Models Domain: Model A/B Test Model

Model A/B test validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ModelABTestBase(BaseEntityWithCompanyBrand):
    """Base model A/B test schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    test_id: UUID = Field(description="Test ID")
    test_name: str = Field(max_length=255, description="Test name")
    test_code: str = Field(max_length=100, description="Test code")
    model_a_id: UUID = Field(description="Model A ID")
    model_b_id: UUID = Field(description="Model B ID")
    traffic_split_a: float = Field(default=0.5, ge=0, le=1, description="Traffic split for model A")
    traffic_split_b: float = Field(default=0.5, ge=0, le=1, description="Traffic split for model B")
    test_start_date: date = Field(description="Test start date")
    test_end_date: Optional[date] = Field(default=None, description="Test end date")
    min_sample_size: Optional[int] = Field(default=None, ge=0, description="Minimum sample size")
    test_status: str = Field(default="PLANNED", max_length=50, description="Test status")
    model_a_predictions: int = Field(default=0, ge=0, description="Model A predictions")
    model_b_predictions: int = Field(default=0, ge=0, description="Model B predictions")
    model_a_accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Model A accuracy")
    model_b_accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Model B accuracy")
    model_a_metric: Optional[float] = Field(default=None, description="Model A primary metric")
    model_b_metric: Optional[float] = Field(default=None, description="Model B primary metric")
    statistical_significance: Optional[float] = Field(default=None, ge=0, le=1, description="Statistical significance (p-value)")
    confidence_level: Optional[float] = Field(default=None, ge=0, le=1, description="Confidence level")
    winning_model_id: Optional[UUID] = Field(default=None, description="Winning model ID")
    winner_determined_at: Optional[datetime] = Field(default=None, description="Winner determined at timestamp")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")

    @field_validator("test_code")
    @classmethod
    def validate_test_code(cls, v: str) -> str:
        """Validate test code format"""
        return FieldValidators.validate_code(v, "test_code")

    @field_validator("test_status")
    @classmethod
    def validate_test_status(cls, v: str) -> str:
        """Validate test status"""
        allowed_statuses = ["PLANNED", "RUNNING", "COMPLETED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "test_status")

    @field_validator("traffic_split_a", "traffic_split_b", "model_a_accuracy", "model_b_accuracy", "statistical_significance", "confidence_level")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class ModelABTest(ModelABTestBase):
    """Model A/B test schema (read)"""

    pass


class ModelABTestCreate(ModelABTestBase):
    """Model A/B test creation schema"""

    test_id: Optional[UUID] = Field(default=None, description="Test ID (auto-generated if not provided)")

    @field_validator("test_id", mode="before")
    @classmethod
    def generate_test_id(cls, v: Optional[UUID]) -> UUID:
        """Generate test ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "test_id")


class ModelABTestUpdate(BaseEntityWithCompanyBrand):
    """Model A/B test update schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    test_status: Optional[str] = Field(default=None, max_length=50, description="Test status")
    test_end_date: Optional[date] = Field(default=None, description="Test end date")
    model_a_predictions: Optional[int] = Field(default=None, ge=0, description="Model A predictions")
    model_b_predictions: Optional[int] = Field(default=None, ge=0, description="Model B predictions")
    model_a_accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Model A accuracy")
    model_b_accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Model B accuracy")
    winning_model_id: Optional[UUID] = Field(default=None, description="Winning model ID")
    winner_determined_at: Optional[datetime] = Field(default=None, description="Winner determined at timestamp")

    @field_validator("test_status")
    @classmethod
    def validate_test_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate test status"""
        if v is None:
            return None
        allowed_statuses = ["PLANNED", "RUNNING", "COMPLETED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "test_status")

