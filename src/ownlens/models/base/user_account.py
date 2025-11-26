"""
OwnLens - Base Domain: User Account Model

User account validation models.
"""

from datetime import date
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity, BaseEntityWithCompanyBrand, TimestampMixin, MetadataMixin
from ..field_validations import FieldValidators


class UserAccountBase(BaseEntityWithCompanyBrand, TimestampMixin, MetadataMixin):
    """Base user account schema"""

    account_id: UUID = Field(description="Account ID")
    user_id: UUID = Field(description="User ID")
    account_type: str = Field(max_length=50, description="Account type")
    subscription_tier: str = Field(default="free", max_length=50, description="Subscription tier")
    subscription_status: str = Field(default="active", max_length=50, description="Subscription status")
    subscription_start_date: Optional[date] = Field(default=None, description="Subscription start date")
    subscription_end_date: Optional[date] = Field(default=None, description="Subscription end date")
    trial_end_date: Optional[date] = Field(default=None, description="Trial end date")
    payment_method: Optional[str] = Field(default=None, max_length=50, description="Payment method")
    billing_country_code: Optional[str] = Field(default=None, max_length=2, description="Billing country code")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("account_type")
    @classmethod
    def validate_account_type(cls, v: str) -> str:
        """Validate account type"""
        allowed_types = ["customer", "editorial", "company"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "account_type")

    @field_validator("subscription_tier")
    @classmethod
    def validate_subscription_tier(cls, v: str) -> str:
        """Validate subscription tier"""
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v.lower(), allowed_tiers, "subscription_tier")

    @field_validator("subscription_status")
    @classmethod
    def validate_subscription_status(cls, v: str) -> str:
        """Validate subscription status"""
        allowed_statuses = ["active", "cancelled", "expired", "trial"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "subscription_status")

    @field_validator("billing_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)


class UserAccount(UserAccountBase):
    """User account schema (read)"""

    pass


class UserAccountCreate(UserAccountBase):
    """User account creation schema"""

    account_id: Optional[UUID] = Field(default=None, description="Account ID (auto-generated if not provided)")

    @field_validator("account_id", mode="before")
    @classmethod
    def generate_account_id(cls, v: Optional[UUID]) -> UUID:
        """Generate account ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "account_id")


class UserAccountUpdate(BaseEntity, TimestampMixin, MetadataMixin):
    """User account update schema"""

    subscription_tier: Optional[str] = Field(default=None, max_length=50, description="Subscription tier")
    subscription_status: Optional[str] = Field(default=None, max_length=50, description="Subscription status")
    subscription_start_date: Optional[date] = Field(default=None, description="Subscription start date")
    subscription_end_date: Optional[date] = Field(default=None, description="Subscription end date")
    trial_end_date: Optional[date] = Field(default=None, description="Trial end date")
    payment_method: Optional[str] = Field(default=None, max_length=50, description="Payment method")
    billing_country_code: Optional[str] = Field(default=None, max_length=2, description="Billing country code")
    is_active: Optional[bool] = Field(default=None, description="Is active")

    @field_validator("subscription_tier")
    @classmethod
    def validate_subscription_tier(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription tier"""
        if v is None:
            return None
        allowed_tiers = ["free", "premium", "pro", "enterprise"]
        return FieldValidators.validate_enum(v.lower(), allowed_tiers, "subscription_tier")

    @field_validator("subscription_status")
    @classmethod
    def validate_subscription_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate subscription status"""
        if v is None:
            return None
        allowed_statuses = ["active", "cancelled", "expired", "trial"]
        return FieldValidators.validate_enum(v.lower(), allowed_statuses, "subscription_status")

    @field_validator("billing_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

