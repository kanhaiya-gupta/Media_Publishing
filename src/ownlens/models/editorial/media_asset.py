"""
OwnLens - Editorial Domain: Media Asset Model

Media asset validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class MediaAssetBase(BaseEntityWithCompanyBrand):
    """Base media asset schema"""

    media_id: UUID = Field(description="Media ID")
    media_name: str = Field(max_length=255, description="Media name")
    media_code: Optional[str] = Field(default=None, max_length=100, description="Unique media code")
    external_media_id: Optional[str] = Field(default=None, max_length=255, description="External system media ID")
    media_type: str = Field(max_length=50, description="Media type")
    media_category: Optional[str] = Field(default=None, max_length=100, description="Media category")
    original_filename: str = Field(max_length=500, description="Original filename")
    file_extension: Optional[str] = Field(default=None, max_length=20, description="File extension")
    mime_type: Optional[str] = Field(default=None, max_length=100, description="MIME type")
    file_size_bytes: Optional[int] = Field(default=None, ge=0, description="File size in bytes")
    file_hash: Optional[str] = Field(default=None, max_length=255, description="File hash (SHA-256)")
    storage_type: str = Field(default="s3", max_length=50, description="Storage type")
    storage_bucket: str = Field(default="ownlens-media", max_length=255, description="Storage bucket")
    storage_path: str = Field(max_length=1000, description="Storage path")
    storage_url: Optional[str] = Field(default=None, max_length=1000, description="Storage URL")
    image_width: Optional[int] = Field(default=None, ge=0, description="Image width (pixels)")
    image_height: Optional[int] = Field(default=None, ge=0, description="Image height (pixels)")
    image_aspect_ratio: Optional[float] = Field(default=None, ge=0, description="Image aspect ratio")
    image_color_space: Optional[str] = Field(default=None, max_length=50, description="Image color space")
    image_dpi: Optional[int] = Field(default=None, ge=0, description="Image DPI")
    image_orientation: Optional[str] = Field(default=None, max_length=20, description="Image orientation")
    video_duration_sec: Optional[int] = Field(default=None, ge=0, description="Video duration (seconds)")
    video_duration_formatted: Optional[str] = Field(default=None, max_length=20, description="Video duration (formatted)")
    video_width: Optional[int] = Field(default=None, ge=0, description="Video width (pixels)")
    video_height: Optional[int] = Field(default=None, ge=0, description="Video height (pixels)")
    video_frame_rate: Optional[float] = Field(default=None, ge=0, description="Video frame rate")
    video_codec: Optional[str] = Field(default=None, max_length=50, description="Video codec")
    video_bitrate: Optional[int] = Field(default=None, ge=0, description="Video bitrate")
    video_has_audio: bool = Field(default=False, description="Video has audio")
    video_audio_codec: Optional[str] = Field(default=None, max_length=50, description="Video audio codec")
    document_page_count: Optional[int] = Field(default=None, ge=0, description="Document page count")
    document_word_count: Optional[int] = Field(default=None, ge=0, description="Document word count")
    document_language_code: Optional[str] = Field(default=None, max_length=5, description="Document language code")
    title: Optional[str] = Field(default=None, max_length=500, description="Media title")
    description: Optional[str] = Field(default=None, description="Media description")
    alt_text: Optional[str] = Field(default=None, description="Alt text")
    caption: Optional[str] = Field(default=None, description="Caption")
    credit: Optional[str] = Field(default=None, max_length=255, description="Credit")
    copyright: Optional[str] = Field(default=None, max_length=255, description="Copyright")
    license_type: Optional[str] = Field(default=None, max_length=100, description="License type")
    keywords: Optional[List[str]] = Field(default=None, description="Keywords")
    tags: Optional[List[str]] = Field(default=None, max_length=100, description="Tags")
    uploaded_by: Optional[UUID] = Field(default=None, description="Uploaded by user ID")
    author_id: Optional[UUID] = Field(default=None, description="Author ID")
    category_id: Optional[UUID] = Field(default=None, description="Category ID")
    is_active: bool = Field(default=True, description="Is active")
    is_public: bool = Field(default=True, description="Is public")
    is_featured: bool = Field(default=False, description="Is featured")
    uploaded_at: datetime = Field(default_factory=datetime.utcnow, description="Uploaded at")
    last_used_at: Optional[datetime] = Field(default=None, description="Last used at")
    usage_count: int = Field(default=0, ge=0, description="Usage count")
    view_count: int = Field(default=0, ge=0, description="View count")
    download_count: int = Field(default=0, ge=0, description="Download count")

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, v: str) -> str:
        """Validate media type"""
        allowed_types = ["image", "video", "document", "audio", "other"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "media_type")

    @field_validator("storage_type")
    @classmethod
    def validate_storage_type(cls, v: str) -> str:
        """Validate storage type"""
        allowed_types = ["s3", "minio", "local", "cdn"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "storage_type")

    @field_validator("image_orientation")
    @classmethod
    def validate_image_orientation(cls, v: Optional[str]) -> Optional[str]:
        """Validate image orientation"""
        if v is None:
            return None
        allowed_orientations = ["landscape", "portrait", "square"]
        return FieldValidators.validate_enum(v.lower(), allowed_orientations, "image_orientation")

    @field_validator("image_color_space")
    @classmethod
    def validate_image_color_space(cls, v: Optional[str]) -> Optional[str]:
        """Validate image color space"""
        if v is None:
            return None
        allowed_spaces = ["RGB", "CMYK", "Grayscale"]
        # Case-insensitive matching: normalize input and map to correct case
        v_normalized = v.strip()
        v_upper = v_normalized.upper()
        if v_upper == "GRAYSCALE":
            return "Grayscale"
        elif v_upper == "RGB":
            return "RGB"
        elif v_upper == "CMYK":
            return "CMYK"
        else:
            raise ValueError(f"image_color_space: Must be one of {allowed_spaces}, got {v}")


class MediaAsset(MediaAssetBase):
    """Media asset schema (read)"""

    pass


class MediaAssetCreate(MediaAssetBase):
    """Media asset creation schema"""

    media_id: Optional[UUID] = Field(default=None, description="Media ID (auto-generated if not provided)")

    @field_validator("media_id", mode="before")
    @classmethod
    def generate_media_id(cls, v: Optional[UUID]) -> UUID:
        """Generate media ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "media_id")


class MediaAssetUpdate(BaseEntityWithCompanyBrand):
    """Media asset update schema"""

    media_name: Optional[str] = Field(default=None, max_length=255, description="Media name")
    title: Optional[str] = Field(default=None, max_length=500, description="Media title")
    description: Optional[str] = Field(default=None, description="Media description")
    alt_text: Optional[str] = Field(default=None, description="Alt text")
    caption: Optional[str] = Field(default=None, description="Caption")
    is_active: Optional[bool] = Field(default=None, description="Is active")
    is_public: Optional[bool] = Field(default=None, description="Is public")
    is_featured: Optional[bool] = Field(default=None, description="Is featured")
    last_used_at: Optional[datetime] = Field(default=None, description="Last used at")
    view_count: Optional[int] = Field(default=None, ge=0, description="View count")
    download_count: Optional[int] = Field(default=None, ge=0, description="Download count")
