"""
OwnLens - Customer Domain: User Schema

User validation models.
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import EmailStr, Field, field_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class UserBase(BaseEntity, TimestampMixin):
    """Base user schema"""

    user_id: UUID = Field(description="User ID")
    user_external_id: Optional[str] = Field(default=None, max_length=255, description="External system user ID")
    email: Optional[EmailStr] = Field(default=None, description="User email")
    username: Optional[str] = Field(default=None, max_length=100, description="Username")
    first_name: Optional[str] = Field(default=None, max_length=100, description="First name")
    last_name: Optional[str] = Field(default=None, max_length=100, description="Last name")
    display_name: Optional[str] = Field(default=None, max_length=255, description="Display name")
    phone_number: Optional[str] = Field(default=None, max_length=50, description="Phone number")
    date_of_birth: Optional[date] = Field(default=None, description="Date of birth")
    gender: Optional[str] = Field(default=None, max_length=20, description="Gender")
    profile_image_url: Optional[str] = Field(default=None, max_length=500, description="Profile image URL")

    # Geographic information
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    primary_language_code: Optional[str] = Field(default=None, max_length=5, description="Primary language code")
    timezone: Optional[str] = Field(default=None, description="Timezone")

    # Status
    is_active: bool = Field(default=True, description="Is user active")
    is_verified: bool = Field(default=False, description="Is user verified")
    last_login_at: Optional[datetime] = Field(default=None, description="Last login timestamp")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format"""
        return FieldValidators.validate_email(v)

    @field_validator("phone_number")
    @classmethod
    def validate_phone_number(cls, v: Optional[str]) -> Optional[str]:
        """Validate phone number"""
        return FieldValidators.validate_phone_number(v)

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("primary_language_code")
    @classmethod
    def validate_language_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate language code"""
        return FieldValidators.validate_language_code(v)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: Optional[str]) -> Optional[str]:
        """Validate timezone"""
        return FieldValidators.validate_timezone(v)

    @field_validator("profile_image_url")
    @classmethod
    def validate_profile_image_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate profile image URL"""
        return FieldValidators.validate_url(v)

    @field_validator("gender")
    @classmethod
    def validate_gender(cls, v: Optional[str]) -> Optional[str]:
        """Validate gender"""
        if v is None:
            return None
        allowed_values = ["male", "female", "other", "prefer_not_to_say"]
        return FieldValidators.validate_enum(v.lower(), allowed_values, "gender")


class User(UserBase):
    """User schema (read)"""

    deleted_at: Optional[datetime] = Field(default=None, description="Soft delete timestamp")


class UserCreate(UserBase):
    """User creation schema"""

    user_id: Optional[UUID] = Field(default=None, description="User ID (auto-generated if not provided)")

    @field_validator("user_id", mode="before")
    @classmethod
    def generate_user_id(cls, v: Optional[UUID]) -> UUID:
        """Generate user ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "user_id")


class UserUpdate(UserBase):
    """User update schema"""

    email: Optional[EmailStr] = Field(default=None, description="User email")
    first_name: Optional[str] = Field(default=None, max_length=100, description="First name")
    last_name: Optional[str] = Field(default=None, max_length=100, description="Last name")
    phone_number: Optional[str] = Field(default=None, max_length=50, description="Phone number")
    is_active: Optional[bool] = Field(default=None, description="Is user active")
    is_verified: Optional[bool] = Field(default=None, description="Is user verified")

