"""
OwnLens - Data Quality Domain: Quality Rule Model

Data quality rule validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class QualityRuleBase(BaseEntity):
    """Base quality rule schema"""

    rule_id: UUID = Field(description="Rule ID")
    rule_name: str = Field(max_length=255, description="Rule name")
    rule_code: str = Field(max_length=100, description="Rule code")
    rule_type: str = Field(max_length=100, description="Rule type")
    table_name: str = Field(max_length=255, description="Table name")
    column_name: Optional[str] = Field(default=None, max_length=255, description="Column name")
    domain: Optional[str] = Field(default=None, max_length=50, description="Domain")
    rule_expression: str = Field(description="Rule expression (SQL or validation logic)")
    rule_threshold: Optional[float] = Field(default=None, ge=0, le=1, description="Rule threshold")
    rule_severity: str = Field(default="WARNING", max_length=20, description="Rule severity")
    is_active: bool = Field(default=True, description="Is rule active")
    is_enforced: bool = Field(default=False, description="Is rule enforced")
    description: Optional[str] = Field(default=None, description="Rule description")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")

    @field_validator("rule_code")
    @classmethod
    def validate_rule_code(cls, v: str) -> str:
        """Validate rule code format"""
        return FieldValidators.validate_code(v, "rule_code")

    @field_validator("rule_type")
    @classmethod
    def validate_rule_type(cls, v: str) -> str:
        """Validate rule type"""
        allowed_types = ["completeness", "accuracy", "consistency", "validity", "timeliness", "uniqueness"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "rule_type")

    @field_validator("rule_severity")
    @classmethod
    def validate_rule_severity(cls, v: str) -> str:
        """Validate rule severity"""
        allowed_severities = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "rule_severity")

    @field_validator("rule_threshold")
    @classmethod
    def validate_threshold(cls, v: Optional[float]) -> Optional[float]:
        """Validate threshold (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class QualityRule(QualityRuleBase):
    """Quality rule schema (read)"""

    pass


class QualityRuleCreate(QualityRuleBase):
    """Quality rule creation schema"""

    rule_id: Optional[UUID] = Field(default=None, description="Rule ID (auto-generated if not provided)")

    @field_validator("rule_id", mode="before")
    @classmethod
    def generate_rule_id(cls, v: Optional[UUID]) -> UUID:
        """Generate rule ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "rule_id")


class QualityRuleUpdate(BaseEntity):
    """Quality rule update schema"""

    rule_name: Optional[str] = Field(default=None, max_length=255, description="Rule name")
    rule_expression: Optional[str] = Field(default=None, description="Rule expression")
    rule_threshold: Optional[float] = Field(default=None, ge=0, le=1, description="Rule threshold")
    rule_severity: Optional[str] = Field(default=None, max_length=20, description="Rule severity")
    is_active: Optional[bool] = Field(default=None, description="Is rule active")
    is_enforced: Optional[bool] = Field(default=None, description="Is rule enforced")
    description: Optional[str] = Field(default=None, description="Rule description")

    @field_validator("rule_severity")
    @classmethod
    def validate_rule_severity(cls, v: Optional[str]) -> Optional[str]:
        """Validate rule severity"""
        if v is None:
            return None
        allowed_severities = ["INFO", "WARNING", "ERROR", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_severities, "rule_severity")

