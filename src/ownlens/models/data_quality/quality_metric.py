"""
OwnLens - Data Quality Domain: Quality Metric Model

Data quality metric validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class QualityMetricBase(BaseEntityWithCompanyBrand):
    """Base quality metric schema"""

    metric_id: UUID = Field(description="Metric ID")
    table_name: str = Field(max_length=255, description="Table name")
    column_name: Optional[str] = Field(default=None, max_length=255, description="Column name")
    domain: Optional[str] = Field(default=None, max_length=50, description="Domain")
    metric_date: date = Field(description="Metric date")
    total_records: int = Field(default=0, ge=0, description="Total records")
    null_count: int = Field(default=0, ge=0, description="Null count")
    null_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Null percentage")
    completeness_score: Optional[float] = Field(default=None, ge=0, le=1, description="Completeness score")
    accuracy_score: Optional[float] = Field(default=None, ge=0, le=1, description="Accuracy score")
    invalid_count: int = Field(default=0, ge=0, description="Invalid count")
    invalid_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Invalid percentage")
    consistency_score: Optional[float] = Field(default=None, ge=0, le=1, description="Consistency score")
    duplicate_count: int = Field(default=0, ge=0, description="Duplicate count")
    duplicate_percentage: Optional[float] = Field(default=None, ge=0, le=1, description="Duplicate percentage")
    validity_score: Optional[float] = Field(default=None, ge=0, le=1, description="Validity score")
    out_of_range_count: int = Field(default=0, ge=0, description="Out of range count")
    format_errors: int = Field(default=0, ge=0, description="Format errors")
    timeliness_score: Optional[float] = Field(default=None, ge=0, le=1, description="Timeliness score")
    stale_records: int = Field(default=0, ge=0, description="Stale records")
    avg_data_age_hours: Optional[float] = Field(default=None, ge=0, description="Average data age in hours")
    uniqueness_score: Optional[float] = Field(default=None, ge=0, le=1, description="Uniqueness score")
    unique_count: int = Field(default=0, ge=0, description="Unique count")
    overall_quality_score: Optional[float] = Field(default=None, ge=0, le=1, description="Overall quality score")
    quality_grade: Optional[str] = Field(default=None, max_length=10, description="Quality grade")

    @field_validator("quality_grade")
    @classmethod
    def validate_quality_grade(cls, v: Optional[str]) -> Optional[str]:
        """Validate quality grade"""
        if v is None:
            return None
        allowed_grades = ["A", "B", "C", "D", "F"]
        return FieldValidators.validate_enum(v.upper(), allowed_grades, "quality_grade")

    @field_validator("null_percentage", "invalid_percentage", "duplicate_percentage", "completeness_score", "accuracy_score", "consistency_score", "validity_score", "timeliness_score", "uniqueness_score", "overall_quality_score")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class QualityMetric(QualityMetricBase):
    """Quality metric schema (read)"""

    pass


class QualityMetricCreate(QualityMetricBase):
    """Quality metric creation schema"""

    metric_id: Optional[UUID] = Field(default=None, description="Metric ID (auto-generated if not provided)")

    @field_validator("metric_id", mode="before")
    @classmethod
    def generate_metric_id(cls, v: Optional[UUID]) -> UUID:
        """Generate metric ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "metric_id")


class QualityMetricUpdate(BaseEntityWithCompanyBrand):
    """Quality metric update schema"""

    total_records: Optional[int] = Field(default=None, ge=0, description="Total records")
    null_count: Optional[int] = Field(default=None, ge=0, description="Null count")
    completeness_score: Optional[float] = Field(default=None, ge=0, le=1, description="Completeness score")
    overall_quality_score: Optional[float] = Field(default=None, ge=0, le=1, description="Overall quality score")
    quality_grade: Optional[str] = Field(default=None, max_length=10, description="Quality grade")

