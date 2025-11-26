"""
OwnLens - Compliance Domain: Retention Policy Model

Data retention policy validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class RetentionPolicyBase(BaseEntityWithCompanyBrand):
    """Base retention policy schema"""

    policy_id: UUID = Field(description="Policy ID")
    policy_name: str = Field(max_length=255, description="Policy name")
    policy_type: str = Field(max_length=100, description="Policy type")
    table_name: Optional[str] = Field(default=None, max_length=255, description="Table name")
    retention_period_days: int = Field(ge=0, description="Retention period in days")
    retention_period_months: Optional[int] = Field(default=None, ge=0, description="Retention period in months")
    retention_period_years: Optional[int] = Field(default=None, ge=0, description="Retention period in years")
    archive_before_delete: bool = Field(default=False, description="Archive before delete")
    archive_location: Optional[str] = Field(default=None, max_length=500, description="Archive location")
    delete_after_retention: bool = Field(default=True, description="Delete after retention")
    deletion_method: Optional[str] = Field(default=None, max_length=50, description="Deletion method")
    is_active: bool = Field(default=True, description="Is policy active")
    last_executed_at: Optional[datetime] = Field(default=None, description="Last executed at")
    next_execution_at: Optional[datetime] = Field(default=None, description="Next execution at")
    description: Optional[str] = Field(default=None, description="Policy description")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")

    @field_validator("policy_type")
    @classmethod
    def validate_policy_type(cls, v: str) -> str:
        """Validate policy type"""
        allowed_types = ["customer", "editorial", "company", "audit", "ml_models"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "policy_type")

    @field_validator("deletion_method")
    @classmethod
    def validate_deletion_method(cls, v: Optional[str]) -> Optional[str]:
        """Validate deletion method"""
        if v is None:
            return None
        allowed_methods = ["SOFT_DELETE", "HARD_DELETE", "ANONYMIZE"]
        return FieldValidators.validate_enum(v.upper(), allowed_methods, "deletion_method")


class RetentionPolicy(RetentionPolicyBase):
    """Retention policy schema (read)"""

    pass


class RetentionPolicyCreate(RetentionPolicyBase):
    """Retention policy creation schema"""

    policy_id: Optional[UUID] = Field(default=None, description="Policy ID (auto-generated if not provided)")

    @field_validator("policy_id", mode="before")
    @classmethod
    def generate_policy_id(cls, v: Optional[UUID]) -> UUID:
        """Generate policy ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "policy_id")


class RetentionPolicyUpdate(BaseEntityWithCompanyBrand):
    """Retention policy update schema"""

    policy_name: Optional[str] = Field(default=None, max_length=255, description="Policy name")
    retention_period_days: Optional[int] = Field(default=None, ge=0, description="Retention period in days")
    is_active: Optional[bool] = Field(default=None, description="Is policy active")
    next_execution_at: Optional[datetime] = Field(default=None, description="Next execution at")
    description: Optional[str] = Field(default=None, description="Policy description")

