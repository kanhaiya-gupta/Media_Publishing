"""
OwnLens - Audit Domain: Data Lineage Model

Data lineage validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class DataLineageBase(BaseEntityWithCompanyBrand):
    """Base data lineage schema"""

    lineage_id: UUID = Field(description="Lineage ID")
    source_type: str = Field(max_length=100, description="Source type")
    source_identifier: str = Field(max_length=500, description="Source identifier")
    source_record_id: Optional[UUID] = Field(default=None, description="Source record ID")
    destination_type: str = Field(max_length=100, description="Destination type")
    destination_identifier: str = Field(max_length=500, description="Destination identifier")
    destination_record_id: Optional[UUID] = Field(default=None, description="Destination record ID")
    transformation_type: Optional[str] = Field(default=None, max_length=100, description="Transformation type")
    transformation_details: Optional[Dict[str, Any]] = Field(default=None, description="Transformation details")
    transformation_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Transformation timestamp")
    pipeline_id: Optional[str] = Field(default=None, max_length=255, description="Pipeline ID")
    batch_id: Optional[UUID] = Field(default=None, description="Batch ID")

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        """Validate source type"""
        allowed_types = ["kafka", "api", "database", "file", "ml_model"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "source_type")

    @field_validator("destination_type")
    @classmethod
    def validate_destination_type(cls, v: str) -> str:
        """Validate destination type"""
        allowed_types = ["database", "ml_model", "api", "file"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "destination_type")

    @field_validator("transformation_type")
    @classmethod
    def validate_transformation_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate transformation type"""
        if v is None:
            return None
        allowed_types = ["aggregation", "filter", "join", "ml_prediction", "enrichment"]
        return FieldValidators.validate_enum(v.lower(), allowed_types, "transformation_type")


class DataLineage(DataLineageBase):
    """Data lineage schema (read)"""

    transformation_date: date = Field(description="Transformation date (generated from transformation_timestamp)")


class DataLineageCreate(DataLineageBase):
    """Data lineage creation schema"""

    lineage_id: Optional[UUID] = Field(default=None, description="Lineage ID (auto-generated if not provided)")
    transformation_date: date = Field(description="Transformation date (required for partitioning)")

    @field_validator("lineage_id", mode="before")
    @classmethod
    def generate_lineage_id(cls, v: Optional[UUID]) -> UUID:
        """Generate lineage ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "lineage_id")


class DataLineageUpdate(BaseEntityWithCompanyBrand):
    """Data lineage update schema"""

    transformation_type: Optional[str] = Field(default=None, max_length=100, description="Transformation type")
    transformation_details: Optional[Dict[str, Any]] = Field(default=None, description="Transformation details")

