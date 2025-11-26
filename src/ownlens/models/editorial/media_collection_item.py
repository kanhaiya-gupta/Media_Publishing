"""
OwnLens - Editorial Domain: Media Collection Item Model

Media collection item validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class MediaCollectionItemBase(BaseEntity):
    """Base media collection item schema"""

    item_id: UUID = Field(description="Item ID")
    collection_id: UUID = Field(description="Collection ID")
    media_id: UUID = Field(description="Media ID")
    position_in_collection: Optional[int] = Field(default=None, ge=0, description="Position in collection")
    item_title: Optional[str] = Field(default=None, max_length=255, description="Item title")
    item_description: Optional[str] = Field(default=None, description="Item description")
    added_at: datetime = Field(default_factory=datetime.utcnow, description="Added at timestamp")


class MediaCollectionItem(MediaCollectionItemBase):
    """Media collection item schema (read)"""

    pass


class MediaCollectionItemCreate(MediaCollectionItemBase):
    """Media collection item creation schema"""

    item_id: Optional[UUID] = Field(default=None, description="Item ID (auto-generated if not provided)")

    @field_validator("item_id", mode="before")
    @classmethod
    def generate_item_id(cls, v: Optional[UUID]) -> UUID:
        """Generate item ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "item_id")


class MediaCollectionItemUpdate(BaseEntity):
    """Media collection item update schema"""

    position_in_collection: Optional[int] = Field(default=None, ge=0, description="Position in collection")
    item_title: Optional[str] = Field(default=None, max_length=255, description="Item title")
    item_description: Optional[str] = Field(default=None, description="Item description")

