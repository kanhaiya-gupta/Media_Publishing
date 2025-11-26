"""
OwnLens - ML Models Domain: Training Run Model

Training run validation models.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntity
from ..field_validations import FieldValidators


class TrainingRunBase(BaseEntity):
    """Base training run schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    run_id: UUID = Field(description="Run ID")
    model_id: UUID = Field(description="Model ID")
    run_name: Optional[str] = Field(default=None, max_length=255, description="Run name")
    run_code: Optional[str] = Field(default=None, max_length=100, description="Run code")
    experiment_name: Optional[str] = Field(default=None, max_length=255, description="Experiment name")
    training_started_at: datetime = Field(description="Training started at")
    training_completed_at: Optional[datetime] = Field(default=None, description="Training completed at")
    training_duration_sec: Optional[int] = Field(default=None, ge=0, description="Training duration in seconds")
    training_status: str = Field(default="RUNNING", max_length=50, description="Training status")
    hyperparameters: Optional[Dict[str, Any]] = Field(default=None, description="Hyperparameters")
    training_config: Optional[Dict[str, Any]] = Field(default=None, description="Training configuration")
    dataset_config: Optional[Dict[str, Any]] = Field(default=None, description="Dataset configuration")
    training_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Training metrics")
    validation_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Validation metrics")
    test_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Test metrics")
    model_artifact_path: Optional[str] = Field(default=None, max_length=1000, description="Model artifact path")
    training_logs_path: Optional[str] = Field(default=None, max_length=1000, description="Training logs path")
    training_plots_path: Optional[str] = Field(default=None, max_length=1000, description="Training plots path")
    error_message: Optional[str] = Field(default=None, description="Error message")
    error_traceback: Optional[str] = Field(default=None, description="Error traceback")
    trained_by: Optional[UUID] = Field(default=None, description="Trained by user ID")
    training_environment: Optional[str] = Field(default=None, max_length=255, description="Training environment")
    training_job_id: Optional[str] = Field(default=None, max_length=255, description="Training job ID")

    @field_validator("training_status")
    @classmethod
    def validate_training_status(cls, v: str) -> str:
        """Validate training status"""
        allowed_statuses = ["RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "training_status")


class TrainingRun(TrainingRunBase):
    """Training run schema (read)"""

    pass


class TrainingRunCreate(TrainingRunBase):
    """Training run creation schema"""

    run_id: Optional[UUID] = Field(default=None, description="Run ID (auto-generated if not provided)")

    @field_validator("run_id", mode="before")
    @classmethod
    def generate_run_id(cls, v: Optional[UUID]) -> UUID:
        """Generate run ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "run_id")


class TrainingRunUpdate(BaseEntity):
    """Training run update schema"""

    training_status: Optional[str] = Field(default=None, max_length=50, description="Training status")
    training_completed_at: Optional[datetime] = Field(default=None, description="Training completed at")
    training_duration_sec: Optional[int] = Field(default=None, ge=0, description="Training duration")
    training_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Training metrics")
    validation_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Validation metrics")
    test_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Test metrics")
    error_message: Optional[str] = Field(default=None, description="Error message")

    @field_validator("training_status")
    @classmethod
    def validate_training_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate training status"""
        if v is None:
            return None
        allowed_statuses = ["RUNNING", "COMPLETED", "FAILED", "CANCELLED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "training_status")

