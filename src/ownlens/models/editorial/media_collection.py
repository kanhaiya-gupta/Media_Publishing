"""
OwnLens - Editorial Domain: Media Collection Model

Media collection validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class MediaCollectionBase(BaseEntityWithCompanyBrand):
    """Base media collection schema"""

    collection_id: UUID = Field(description="Collection ID")
    collection_name: str = Field(max_length=255, description="Collection name")
    collection_type: str = Field(max_length=50, description="Collection type")
    description: Optional[str] = Field(default=None, description="Collection description")
    media_count: int = Field(default=0, ge=0, description="Number of media in collection")
    cover_media_id: Optional[UUID] = Field(default=None, description="Cover media ID")
    is_active: bool = Field(default=True, description="Is collection active")
    is_public: bool = Field(default=True, description="Is collection public")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")

    @field_validator("collection_type")
    @classmethod
    def validate_collection_type(cls, v: str) -> str:
        """Validate collection type"""
        allowed_types = ["gallery", "playlist", "album", "folder"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "collection_type")


class MediaCollection(MediaCollectionBase):
    """Media collection schema (read)"""

    pass


class MediaCollectionCreate(MediaCollectionBase):
    """Media collection creation schema"""

    collection_id: Optional[UUID] = Field(default=None, description="Collection ID (auto-generated if not provided)")

    @field_validator("collection_id", mode="before")
    @classmethod
    def generate_collection_id(cls, v: Optional[UUID]) -> UUID:
        """Generate collection ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "collection_id")


class MediaCollectionUpdate(BaseEntityWithCompanyBrand):
    """Media collection update schema"""

    collection_name: Optional[str] = Field(default=None, max_length=255, description="Collection name")
    description: Optional[str] = Field(default=None, description="Collection description")
    media_count: Optional[int] = Field(default=None, ge=0, description="Number of media in collection")
    cover_media_id: Optional[UUID] = Field(default=None, description="Cover media ID")
    is_active: Optional[bool] = Field(default=None, description="Is collection active")
    is_public: Optional[bool] = Field(default=None, description="Is collection public")

