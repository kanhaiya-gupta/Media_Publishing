"""
OwnLens - Compliance Domain: Retention Execution Model

Retention execution validation models.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class RetentionExecutionBase(BaseEntity):
    """Base retention execution schema"""

    execution_id: UUID = Field(description="Execution ID")
    policy_id: UUID = Field(description="Policy ID")
    execution_status: str = Field(description="Execution status")
    execution_started_at: datetime = Field(description="Execution started at")
    execution_completed_at: Optional[datetime] = Field(default=None, description="Execution completed at")
    execution_duration_sec: Optional[int] = Field(default=None, ge=0, description="Execution duration in seconds")
    records_processed: int = Field(default=0, ge=0, description="Records processed")
    records_archived: int = Field(default=0, ge=0, description="Records archived")
    records_deleted: int = Field(default=0, ge=0, description="Records deleted")
    records_anonymized: int = Field(default=0, ge=0, description="Records anonymized")
    error_message: Optional[str] = Field(default=None, description="Error message")
    error_count: int = Field(default=0, ge=0, description="Error count")

    @field_validator("execution_status")
    @classmethod
    def validate_execution_status(cls, v: str) -> str:
        """Validate execution status"""
        allowed_statuses = ["SUCCESS", "FAILED", "PARTIAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "execution_status")


class RetentionExecution(RetentionExecutionBase):
    """Retention execution schema (read)"""

    pass


class RetentionExecutionCreate(RetentionExecutionBase):
    """Retention execution creation schema"""

    execution_id: Optional[UUID] = Field(default=None, description="Execution ID (auto-generated if not provided)")

    @field_validator("execution_id", mode="before")
    @classmethod
    def generate_execution_id(cls, v: Optional[UUID]) -> UUID:
        """Generate execution ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "execution_id")


class RetentionExecutionUpdate(BaseEntity):
    """Retention execution update schema"""

    execution_status: Optional[str] = Field(default=None, description="Execution status")
    execution_completed_at: Optional[datetime] = Field(default=None, description="Execution completed at")
    execution_duration_sec: Optional[int] = Field(default=None, ge=0, description="Execution duration")
    records_processed: Optional[int] = Field(default=None, ge=0, description="Records processed")
    error_message: Optional[str] = Field(default=None, description="Error message")

    @field_validator("execution_status")
    @classmethod
    def validate_execution_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate execution status"""
        if v is None:
            return None
        allowed_statuses = ["SUCCESS", "FAILED", "PARTIAL"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "execution_status")

