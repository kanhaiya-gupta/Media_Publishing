"""
OwnLens - Base Domain: Category Model

Category validation models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin, MetadataMixin
from ..field_validations import FieldValidators


class CategoryBase(BaseEntity, TimestampMixin, MetadataMixin):
    """Base category schema"""

    category_id: UUID = Field(description="Category ID")
    brand_id: Optional[UUID] = Field(default=None, description="Brand ID")
    category_name: str = Field(max_length=255, description="Category name")
    category_code: str = Field(max_length=100, description="Category code")
    parent_category_id: Optional[UUID] = Field(default=None, description="Parent category ID")
    category_type: str = Field(max_length=50, description="Category type")
    description: Optional[str] = Field(default=None, description="Description")
    display_order: int = Field(default=0, ge=0, description="Display order")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("category_code")
    @classmethod
    def validate_category_code(cls, v: str) -> str:
        """Validate category code format"""
        return FieldValidators.validate_code(v, "category_code")

    @field_validator("category_type")
    @classmethod
    def validate_category_type(cls, v: str) -> str:
        """Validate category type"""
        allowed_types = ["editorial", "company", "both"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "category_type")


class Category(CategoryBase):
    """Category schema (read)"""

    pass


class CategoryCreate(CategoryBase):
    """Category creation schema"""

    category_id: Optional[UUID] = Field(default=None, description="Category ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_category_id(cls, data: Any) -> Any:
        """Ensure category_id is generated if missing or None"""
        if isinstance(data, dict):
            if "category_id" not in data or data.get("category_id") is None or data.get("category_id") == "":
                from uuid import uuid4
                data["category_id"] = uuid4()
        return data

    @field_validator("category_id", mode="before")
    @classmethod
    def validate_category_id(cls, v: Any) -> UUID:
        """Validate category ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "category_id")


class CategoryUpdate(BaseEntity, TimestampMixin, MetadataMixin):
    """Category update schema"""

    category_name: Optional[str] = Field(default=None, max_length=255, description="Category name")
    description: Optional[str] = Field(default=None, description="Description")
    display_order: Optional[int] = Field(default=None, ge=0, description="Display order")
    is_active: Optional[bool] = Field(default=None, description="Is active")
    category_type: Optional[str] = Field(default=None, max_length=50, description="Category type")

    @field_validator("category_type")
    @classmethod
    def validate_category_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate category type"""
        if v is None:
            return None
        allowed_types = ["editorial", "company", "both"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "category_type")

