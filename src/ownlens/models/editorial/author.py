"""
OwnLens - Editorial Domain: Author Schema

Author validation models.
"""

from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from pydantic import EmailStr, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class AuthorBase(BaseEntityWithCompanyBrand):
    """Base author schema"""

    author_id: UUID = Field(description="Author ID")
    user_id: Optional[UUID] = Field(default=None, description="User ID (if author has account)")

    # Author information
    author_name: str = Field(max_length=255, description="Author name")
    author_code: str = Field(max_length=100, description="Author code (unique within brand)")
    email: Optional[EmailStr] = Field(default=None, description="Author email")
    bio: Optional[str] = Field(default=None, description="Author bio")
    profile_image_url: Optional[str] = Field(default=None, max_length=500, description="Profile image URL")
    department: Optional[str] = Field(default=None, max_length=100, description="Department")
    role: Optional[str] = Field(default=None, max_length=100, description="Role")
    expertise_areas: Optional[List[str]] = Field(default=None, description="Expertise areas")

    # Geographic information
    primary_country_code: Optional[str] = Field(default=None, max_length=2, description="Primary country code")
    primary_city_id: Optional[UUID] = Field(default=None, description="Primary city ID")

    # Status
    is_active: bool = Field(default=True, description="Is author active")
    hire_date: Optional[date] = Field(default=None, description="Hire date")
    last_article_date: Optional[date] = Field(default=None, description="Last article date")

    @field_validator("author_code")
    @classmethod
    def validate_author_code(cls, v: str) -> str:
        """Validate author code format"""
        return FieldValidators.validate_code(v, "author_code")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        """Validate email format"""
        return FieldValidators.validate_email(v)

    @field_validator("profile_image_url")
    @classmethod
    def validate_profile_image_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate profile image URL"""
        return FieldValidators.validate_url(v)

    @field_validator("primary_country_code")
    @classmethod
    def validate_country_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate country code"""
        return FieldValidators.validate_country_code(v)

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: Optional[str]) -> Optional[str]:
        """Validate author role"""
        if v is None:
            return None
        allowed_roles = [
            "journalist", "editor", "correspondent", "columnist", "photographer", "videographer",
            "reporter", "senior_editor", "deputy_editor", "chief_editor"
        ]
        return FieldValidators.validate_enum(v.lower(), allowed_roles, "role")


class Author(AuthorBase):
    """Author schema (read)"""

    pass


class AuthorCreate(AuthorBase):
    """Author creation schema"""

    author_id: Optional[UUID] = Field(default=None, description="Author ID (auto-generated if not provided)")

    @field_validator("author_id", mode="before")
    @classmethod
    def generate_author_id(cls, v: Optional[UUID]) -> UUID:
        """Generate author ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "author_id")


class AuthorUpdate(BaseEntityWithCompanyBrand):
    """Author update schema"""

    author_name: Optional[str] = Field(default=None, max_length=255, description="Author name")
    email: Optional[EmailStr] = Field(default=None, description="Author email")
    bio: Optional[str] = Field(default=None, description="Author bio")
    is_active: Optional[bool] = Field(default=None, description="Is author active")
    last_article_date: Optional[date] = Field(default=None, description="Last article date")

