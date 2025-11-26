"""
OwnLens - Editorial Domain: Media Variant Model

Media variant validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class MediaVariantBase(BaseEntity):
    """Base media variant schema"""

    variant_id: UUID = Field(description="Variant ID")
    media_id: UUID = Field(description="Media ID")
    variant_type: str = Field(max_length=50, description="Variant type")
    variant_size: Optional[str] = Field(default=None, max_length=50, description="Variant size")
    variant_format: Optional[str] = Field(default=None, max_length=20, description="Variant format")
    file_size_bytes: Optional[int] = Field(default=None, ge=0, description="File size in bytes")
    file_hash: Optional[str] = Field(default=None, max_length=255, description="File hash")
    storage_path: str = Field(max_length=1000, description="Storage path")
    storage_url: Optional[str] = Field(default=None, max_length=1000, description="Storage URL")
    width: Optional[int] = Field(default=None, ge=0, description="Width")
    height: Optional[int] = Field(default=None, ge=0, description="Height")
    aspect_ratio: Optional[float] = Field(default=None, ge=0, description="Aspect ratio")
    quality_level: Optional[str] = Field(default=None, max_length=20, description="Quality level")
    compression_ratio: Optional[float] = Field(default=None, ge=0, le=1, description="Compression ratio")
    is_active: bool = Field(default=True, description="Is active")

    @field_validator("variant_type")
    @classmethod
    def validate_variant_type(cls, v: str) -> str:
        """Validate variant type"""
        allowed_types = ["thumbnail", "small", "medium", "large", "original", "webp", "avif", "preview"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "variant_type")

    @field_validator("quality_level")
    @classmethod
    def validate_quality_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate quality level"""
        if v is None:
            return None
        allowed_levels = ["low", "medium", "high", "original"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "quality_level")

    @field_validator("compression_ratio")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class MediaVariant(MediaVariantBase):
    """Media variant schema (read)"""

    pass


class MediaVariantCreate(MediaVariantBase):
    """Media variant creation schema"""

    variant_id: Optional[UUID] = Field(default=None, description="Variant ID (auto-generated if not provided)")

    @field_validator("variant_id", mode="before")
    @classmethod
    def generate_variant_id(cls, v: Optional[UUID]) -> UUID:
        """Generate variant ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "variant_id")


class MediaVariantUpdate(BaseEntity):
    """Media variant update schema"""

    variant_size: Optional[str] = Field(default=None, max_length=50, description="Variant size")
    quality_level: Optional[str] = Field(default=None, max_length=20, description="Quality level")
    is_active: Optional[bool] = Field(default=None, description="Is active")

    @field_validator("quality_level")
    @classmethod
    def validate_quality_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate quality level"""
        if v is None:
            return None
        allowed_levels = ["low", "medium", "high", "original"]
        return FieldValidators.validate_enum(v.lower(), allowed_levels, "quality_level")
