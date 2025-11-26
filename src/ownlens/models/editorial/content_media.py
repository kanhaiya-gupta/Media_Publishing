"""
OwnLens - Editorial Domain: Content Media Model

Content-media relationship validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class ContentMediaBase(BaseEntity):
    """Base content-media relationship schema"""

    relationship_id: UUID = Field(description="Relationship ID")
    article_id: UUID = Field(description="Article ID")
    media_id: UUID = Field(description="Media ID")
    content_id: Optional[UUID] = Field(default=None, description="Content ID")
    relationship_type: str = Field(max_length=50, description="Relationship type")
    position_in_content: Optional[int] = Field(default=None, ge=0, description="Position in content")
    position_in_section: Optional[str] = Field(default=None, max_length=100, description="Position in section")
    media_context: Optional[str] = Field(default=None, description="Media context")
    media_caption: Optional[str] = Field(default=None, description="Media caption")
    media_alt_text: Optional[str] = Field(default=None, description="Media alt text")
    is_featured: bool = Field(default=False, description="Is featured")
    is_required: bool = Field(default=False, description="Is required")
    display_size: Optional[str] = Field(default=None, max_length=50, description="Display size")
    alignment: Optional[str] = Field(default=None, max_length=20, description="Alignment")
    wrap_text: bool = Field(default=False, description="Wrap text")
    linked_at: datetime = Field(default_factory=datetime.utcnow, description="Linked at")

    @field_validator("relationship_type")
    @classmethod
    def validate_relationship_type(cls, v: str) -> str:
        """Validate relationship type"""
        allowed_types = ["featured_image", "inline_image", "gallery_image", "video", "document", "thumbnail"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "relationship_type")

    @field_validator("display_size")
    @classmethod
    def validate_display_size(cls, v: Optional[str]) -> Optional[str]:
        """Validate display size"""
        if v is None:
            return None
        allowed_sizes = ["small", "medium", "large", "full_width"]
        return FieldValidators.validate_enum(v.lower(), allowed_sizes, "display_size")

    @field_validator("alignment")
    @classmethod
    def validate_alignment(cls, v: Optional[str]) -> Optional[str]:
        """Validate alignment"""
        if v is None:
            return None
        allowed_alignments = ["left", "center", "right", "full_width"]
        return FieldValidators.validate_enum(v.lower(), allowed_alignments, "alignment")


class ContentMedia(ContentMediaBase):
    """Content-media relationship schema (read)"""

    pass


class ContentMediaCreate(ContentMediaBase):
    """Content-media relationship creation schema"""

    relationship_id: Optional[UUID] = Field(default=None, description="Relationship ID (auto-generated if not provided)")

    @field_validator("relationship_id", mode="before")
    @classmethod
    def generate_relationship_id(cls, v: Optional[UUID]) -> UUID:
        """Generate relationship ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "relationship_id")


class ContentMediaUpdate(BaseEntity):
    """Content-media relationship update schema"""

    relationship_type: Optional[str] = Field(default=None, max_length=50, description="Relationship type")
    position_in_content: Optional[int] = Field(default=None, ge=0, description="Position in content")
    media_caption: Optional[str] = Field(default=None, description="Media caption")
    media_alt_text: Optional[str] = Field(default=None, description="Media alt text")
    is_featured: Optional[bool] = Field(default=None, description="Is featured")
    display_size: Optional[str] = Field(default=None, max_length=50, description="Display size")
    alignment: Optional[str] = Field(default=None, max_length=20, description="Alignment")

    @field_validator("relationship_type")
    @classmethod
    def validate_relationship_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate relationship type"""
        if v is None:
            return None
        allowed_types = ["featured_image", "inline_image", "gallery_image", "video", "document", "thumbnail"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "relationship_type")

    @field_validator("display_size")
    @classmethod
    def validate_display_size(cls, v: Optional[str]) -> Optional[str]:
        """Validate display size"""
        if v is None:
            return None
        allowed_sizes = ["small", "medium", "large", "full_width"]
        return FieldValidators.validate_enum(v.lower(), allowed_sizes, "display_size")

    @field_validator("alignment")
    @classmethod
    def validate_alignment(cls, v: Optional[str]) -> Optional[str]:
        """Validate alignment"""
        if v is None:
            return None
        allowed_alignments = ["left", "center", "right", "full_width"]
        return FieldValidators.validate_enum(v.lower(), allowed_alignments, "alignment")
