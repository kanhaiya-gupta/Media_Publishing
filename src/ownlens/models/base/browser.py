"""
OwnLens - Base Domain: Browser Model

Browser validation models.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..base import BaseEntity, TimestampMixin
from ..field_validations import FieldValidators


class BrowserBase(BaseEntity, TimestampMixin):
    """Base browser schema"""

    browser_id: UUID = Field(description="Browser ID")
    browser_code: str = Field(max_length=50, description="Browser code")
    browser_name: str = Field(max_length=100, description="Browser name")
    vendor: Optional[str] = Field(default=None, max_length=100, description="Vendor")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("browser_code")
    @classmethod
    def validate_browser_code(cls, v: str) -> str:
        """Validate browser code format"""
        return FieldValidators.validate_code(v, "browser_code")


class Browser(BrowserBase):
    """Browser schema (read)"""

    pass


class BrowserCreate(BrowserBase):
    """Browser creation schema"""

    browser_id: Optional[UUID] = Field(default=None, description="Browser ID (auto-generated if not provided)")

    @model_validator(mode="before")
    @classmethod
    def ensure_browser_id(cls, data: Any) -> Any:
        """Ensure browser_id is generated if missing or None"""
        if isinstance(data, dict):
            if "browser_id" not in data or data.get("browser_id") is None or data.get("browser_id") == "":
                from uuid import uuid4
                data["browser_id"] = uuid4()
        return data

    @field_validator("browser_id", mode="before")
    @classmethod
    def validate_browser_id(cls, v: Any) -> UUID:
        """Validate browser ID"""
        if v is None or v == "":
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "browser_id")


class BrowserUpdate(BaseEntity, TimestampMixin):
    """Browser update schema"""

    browser_name: Optional[str] = Field(default=None, max_length=100, description="Browser name")
    vendor: Optional[str] = Field(default=None, max_length=100, description="Vendor")
    is_active: Optional[bool] = Field(default=None, description="Is active")

