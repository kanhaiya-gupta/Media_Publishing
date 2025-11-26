"""
OwnLens - Compliance Domain: Privacy Assessment Model

Privacy Impact Assessment validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class PrivacyAssessmentBase(BaseEntityWithCompanyBrand):
    """Base privacy assessment schema"""

    assessment_id: UUID = Field(description="Assessment ID")
    assessment_name: str = Field(max_length=255, description="Assessment name")
    assessment_type: str = Field(max_length=50, description="Assessment type")
    description: Optional[str] = Field(default=None, description="Assessment description")
    data_types_processed: Optional[List[str]] = Field(default=None, description="Data types processed")
    processing_purposes: Optional[List[str]] = Field(default=None, description="Processing purposes")
    data_subjects_affected: Optional[int] = Field(default=None, ge=0, description="Data subjects affected")
    risk_level: Optional[str] = Field(default=None, max_length=20, description="Risk level")
    risk_factors: Optional[Dict[str, Any]] = Field(default=None, description="Risk factors")
    mitigation_measures: Optional[Dict[str, Any]] = Field(default=None, description="Mitigation measures")
    assessment_status: str = Field(default="DRAFT", max_length=50, description="Assessment status")
    assessed_by: Optional[UUID] = Field(default=None, description="Assessed by user ID")
    reviewed_by: Optional[UUID] = Field(default=None, description="Reviewed by user ID")
    approved_by: Optional[UUID] = Field(default=None, description="Approved by user ID")
    assessment_date: Optional[date] = Field(default=None, description="Assessment date")
    review_date: Optional[date] = Field(default=None, description="Review date")
    approval_date: Optional[date] = Field(default=None, description="Approval date")
    next_review_date: Optional[date] = Field(default=None, description="Next review date")

    @field_validator("assessment_type")
    @classmethod
    def validate_assessment_type(cls, v: str) -> str:
        """Validate assessment type"""
        allowed_types = ["PIA", "DPIA", "RISK_ASSESSMENT"]
        return FieldValidators.validate_enum(v.upper(), allowed_types, "assessment_type")

    @field_validator("risk_level")
    @classmethod
    def validate_risk_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate risk level"""
        if v is None:
            return None
        allowed_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_levels, "risk_level")

    @field_validator("assessment_status")
    @classmethod
    def validate_assessment_status(cls, v: str) -> str:
        """Validate assessment status"""
        allowed_statuses = ["DRAFT", "REVIEW", "APPROVED", "REJECTED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "assessment_status")


class PrivacyAssessment(PrivacyAssessmentBase):
    """Privacy assessment schema (read)"""

    pass


class PrivacyAssessmentCreate(PrivacyAssessmentBase):
    """Privacy assessment creation schema"""

    assessment_id: Optional[UUID] = Field(default=None, description="Assessment ID (auto-generated if not provided)")

    @field_validator("assessment_id", mode="before")
    @classmethod
    def generate_assessment_id(cls, v: Optional[UUID]) -> UUID:
        """Generate assessment ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "assessment_id")


class PrivacyAssessmentUpdate(BaseEntityWithCompanyBrand):
    """Privacy assessment update schema"""

    assessment_name: Optional[str] = Field(default=None, max_length=255, description="Assessment name")
    description: Optional[str] = Field(default=None, description="Assessment description")
    risk_level: Optional[str] = Field(default=None, max_length=20, description="Risk level")
    assessment_status: Optional[str] = Field(default=None, max_length=50, description="Assessment status")
    review_date: Optional[date] = Field(default=None, description="Review date")
    approval_date: Optional[date] = Field(default=None, description="Approval date")
    next_review_date: Optional[date] = Field(default=None, description="Next review date")

    @field_validator("risk_level")
    @classmethod
    def validate_risk_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate risk level"""
        if v is None:
            return None
        allowed_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_levels, "risk_level")

    @field_validator("assessment_status")
    @classmethod
    def validate_assessment_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate assessment status"""
        if v is None:
            return None
        allowed_statuses = ["DRAFT", "REVIEW", "APPROVED", "REJECTED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "assessment_status")

