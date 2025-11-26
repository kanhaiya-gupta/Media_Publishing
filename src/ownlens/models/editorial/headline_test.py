"""
OwnLens - Editorial Domain: Headline Test Model

Headline A/B test validation models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class HeadlineTestBase(BaseEntityWithCompanyBrand):
    """Base headline test schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    test_id: UUID = Field(description="Test ID")
    article_id: UUID = Field(description="Article ID")
    headline_variant_a: str = Field(max_length=500, description="Headline variant A")
    headline_variant_b: Optional[str] = Field(default=None, max_length=500, description="Headline variant B")
    headline_variant_c: Optional[str] = Field(default=None, max_length=500, description="Headline variant C")
    test_start_time: datetime = Field(description="Test start time")
    test_end_time: Optional[datetime] = Field(default=None, description="Test end time")
    traffic_split: Optional[Dict[str, Any]] = Field(default=None, description="Traffic distribution")
    variant_a_views: int = Field(default=0, ge=0, description="Variant A views")
    variant_b_views: int = Field(default=0, ge=0, description="Variant B views")
    variant_c_views: int = Field(default=0, ge=0, description="Variant C views")
    variant_a_clicks: int = Field(default=0, ge=0, description="Variant A clicks")
    variant_b_clicks: int = Field(default=0, ge=0, description="Variant B clicks")
    variant_c_clicks: int = Field(default=0, ge=0, description="Variant C clicks")
    variant_a_ctr: Optional[float] = Field(default=None, ge=0, le=1, description="Variant A CTR")
    variant_b_ctr: Optional[float] = Field(default=None, ge=0, le=1, description="Variant B CTR")
    variant_c_ctr: Optional[float] = Field(default=None, ge=0, le=1, description="Variant C CTR")
    winning_variant: Optional[str] = Field(default=None, max_length=1, description="Winning variant")
    statistical_significance: Optional[float] = Field(default=None, ge=0, le=1, description="Statistical significance")
    predicted_ctr_a: Optional[float] = Field(default=None, ge=0, le=1, description="Predicted CTR A")
    predicted_ctr_b: Optional[float] = Field(default=None, ge=0, le=1, description="Predicted CTR B")
    predicted_ctr_c: Optional[float] = Field(default=None, ge=0, le=1, description="Predicted CTR C")
    model_version: Optional[str] = Field(default=None, max_length=50, description="Model version")
    test_status: str = Field(default="active", max_length=50, description="Test status")

    @field_validator("test_status")
    @classmethod
    def validate_test_status(cls, v: str) -> str:
        """Validate test status"""
        allowed_statuses = ["active", "completed", "cancelled"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "test_status")

    @field_validator("winning_variant")
    @classmethod
    def validate_winning_variant(cls, v: Optional[str]) -> Optional[str]:
        """Validate winning variant"""
        if v is None:
            return None
        allowed_variants = ["a", "b", "c"]
        return FieldValidators.validate_enum(v.lower(), allowed_variants, "winning_variant")

    @field_validator("variant_a_ctr", "variant_b_ctr", "variant_c_ctr", "statistical_significance", "predicted_ctr_a", "predicted_ctr_b", "predicted_ctr_c")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class HeadlineTest(HeadlineTestBase):
    """Headline test schema (read)"""

    pass


class HeadlineTestCreate(HeadlineTestBase):
    """Headline test creation schema"""

    test_id: Optional[UUID] = Field(default=None, description="Test ID (auto-generated if not provided)")

    @field_validator("test_id", mode="before")
    @classmethod
    def generate_test_id(cls, v: Optional[UUID]) -> UUID:
        """Generate test ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "test_id")


class HeadlineTestUpdate(BaseEntityWithCompanyBrand):
    """Headline test update schema"""

    test_status: Optional[str] = Field(default=None, max_length=50, description="Test status")
    test_end_time: Optional[datetime] = Field(default=None, description="Test end time")
    variant_a_views: Optional[int] = Field(default=None, ge=0, description="Variant A views")
    variant_b_views: Optional[int] = Field(default=None, ge=0, description="Variant B views")
    variant_a_clicks: Optional[int] = Field(default=None, ge=0, description="Variant A clicks")
    variant_b_clicks: Optional[int] = Field(default=None, ge=0, description="Variant B clicks")
    variant_a_ctr: Optional[float] = Field(default=None, ge=0, le=1, description="Variant A CTR")
    variant_b_ctr: Optional[float] = Field(default=None, ge=0, le=1, description="Variant B CTR")
    winning_variant: Optional[str] = Field(default=None, max_length=1, description="Winning variant")

    @field_validator("test_status")
    @classmethod
    def validate_test_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate test status"""
        if v is None:
            return None
        allowed_statuses = ["active", "completed", "cancelled"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "test_status")

