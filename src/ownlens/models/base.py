"""
OwnLens - Base Model Classes

Base classes and common validators for all domain models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import ConfigDict


class BaseSchema(BaseModel):
    """Base schema class with common configuration"""

    model_config = ConfigDict(
        # Use enum values instead of names
        use_enum_values=True,
        # Validate assignment
        validate_assignment=True,
        # Allow population by field name
        populate_by_name=True,
        # Serialize by alias
        by_alias=True,
        # JSON encoders
        json_encoders={
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v),
        },
        # Extra fields handling
        extra="forbid",  # Forbid extra fields
    )


class TimestampMixin(BaseModel):
    """Mixin for timestamp fields"""

    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Last update timestamp")

    @model_validator(mode="before")
    @classmethod
    def set_updated_at(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Set updated_at to current time if not provided"""
        if isinstance(values, dict) and "updated_at" not in values:
            values["updated_at"] = datetime.utcnow()
        return values


class MetadataMixin(BaseModel):
    """Mixin for metadata fields"""

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class CompanyBrandMixin(BaseModel):
    """Mixin for company and brand scoping"""

    company_id: UUID = Field(description="Company ID")
    brand_id: UUID = Field(description="Brand ID")

    @field_validator("company_id", "brand_id", mode="before")
    @classmethod
    def validate_uuid(cls, v: Any) -> UUID:
        """Validate UUID format"""
        if v is None:
            raise ValueError("company_id and brand_id are required")
        if isinstance(v, str):
            try:
                return UUID(v)
            except ValueError:
                raise ValueError(f"Invalid UUID format: {v}")
        if isinstance(v, UUID):
            return v
        raise ValueError(f"Invalid UUID type: {type(v)}")


class BaseEntity(BaseSchema, TimestampMixin, MetadataMixin):
    """Base entity with common fields"""

    id: Optional[UUID] = Field(default=None, description="Entity ID")

    @field_validator("id", mode="before")
    @classmethod
    def validate_id(cls, v: Any) -> Optional[UUID]:
        """Validate ID as UUID"""
        if v is None:
            return None
        if isinstance(v, str):
            try:
                return UUID(v)
            except ValueError:
                raise ValueError(f"Invalid UUID format: {v}")
        if isinstance(v, UUID):
            return v
        raise ValueError(f"Invalid UUID type: {type(v)}")


class BaseEntityWithCompanyBrand(BaseEntity, CompanyBrandMixin):
    """Base entity with company and brand scoping"""

    pass


class PaginationParams(BaseModel):
    """Pagination parameters"""

    page: int = Field(default=1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(default=20, ge=1, le=100, description="Items per page")
    offset: Optional[int] = Field(default=None, description="Offset for pagination")

    @model_validator(mode="after")
    def calculate_offset(self) -> "PaginationParams":
        """Calculate offset from page and page_size"""
        if self.offset is None:
            self.offset = (self.page - 1) * self.page_size
        return self


class SortParams(BaseModel):
    """Sorting parameters"""

    sort_by: Optional[str] = Field(default=None, description="Field to sort by")
    sort_order: str = Field(default="asc", pattern="^(asc|desc)$", description="Sort order (asc/desc)")

    @field_validator("sort_order")
    @classmethod
    def validate_sort_order(cls, v: str) -> str:
        """Validate sort order"""
        if v.lower() not in ["asc", "desc"]:
            raise ValueError("sort_order must be 'asc' or 'desc'")
        return v.lower()


class FilterParams(BaseModel):
    """Filtering parameters"""

    filters: Dict[str, Any] = Field(default_factory=dict, description="Filter criteria")


class QueryParams(PaginationParams, SortParams, FilterParams):
    """Combined query parameters"""

    pass


class ResponseMetadata(BaseModel):
    """Response metadata"""

    total: int = Field(description="Total number of items")
    page: int = Field(description="Current page number")
    page_size: int = Field(description="Items per page")
    total_pages: int = Field(description="Total number of pages")

    @model_validator(mode="after")
    def calculate_total_pages(self) -> "ResponseMetadata":
        """Calculate total pages"""
        if self.total_pages == 0:
            self.total_pages = (self.total + self.page_size - 1) // self.page_size
        return self


class BaseResponse(BaseModel):
    """Base response model"""

    success: bool = Field(default=True, description="Success status")
    message: Optional[str] = Field(default=None, description="Response message")
    metadata: Optional[ResponseMetadata] = Field(default=None, description="Response metadata")


class ErrorResponse(BaseModel):
    """Error response model"""

    success: bool = Field(default=False, description="Success status")
    error: str = Field(description="Error message")
    error_code: Optional[str] = Field(default=None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Error details")

