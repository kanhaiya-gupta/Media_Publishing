"""
OwnLens - ML Models Domain: Model Registry Model

ML model registry validation models.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ModelRegistryBase(BaseEntityWithCompanyBrand):
    """Base model registry schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    model_id: UUID = Field(description="Model ID")
    model_name: str = Field(max_length=255, description="Model name")
    model_code: str = Field(max_length=100, description="Model code")
    model_version: str = Field(max_length=50, description="Model version")
    model_type: str = Field(max_length=100, description="Model type")
    domain: str = Field(max_length=50, description="Domain")
    algorithm: str = Field(max_length=100, description="Algorithm")
    model_framework: Optional[str] = Field(default=None, max_length=50, description="Model framework")
    model_format: Optional[str] = Field(default=None, max_length=50, description="Model format")
    model_path: Optional[str] = Field(default=None, max_length=1000, description="Model path")
    model_storage_type: Optional[str] = Field(default=None, max_length=50, description="Model storage type")
    model_storage_url: Optional[str] = Field(default=None, max_length=1000, description="Model storage URL")
    description: Optional[str] = Field(default=None, description="Model description")
    tags: Optional[List[str]] = Field(default=None, description="Tags")
    training_dataset_path: Optional[str] = Field(default=None, max_length=1000, description="Training dataset path")
    training_dataset_size: Optional[int] = Field(default=None, ge=0, description="Training dataset size")
    training_date: Optional[datetime] = Field(default=None, description="Training date")
    training_duration_sec: Optional[int] = Field(default=None, ge=0, description="Training duration in seconds")
    training_environment: Optional[str] = Field(default=None, max_length=255, description="Training environment")
    performance_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Performance metrics")
    feature_importance: Optional[Dict[str, Any]] = Field(default=None, description="Feature importance")
    evaluation_dataset_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Evaluation metrics")
    model_status: str = Field(default="TRAINING", max_length=50, description="Model status")
    is_active: bool = Field(default=True, description="Is model active")
    deployed_at: Optional[datetime] = Field(default=None, description="Deployed at timestamp")
    deployed_by: Optional[UUID] = Field(default=None, description="Deployed by user ID")
    deployment_environment: Optional[str] = Field(default=None, max_length=50, description="Deployment environment")
    parent_model_id: Optional[UUID] = Field(default=None, description="Parent model ID")
    is_latest_version: bool = Field(default=True, description="Is latest version")
    created_by: Optional[UUID] = Field(default=None, description="Created by user ID")

    @field_validator("model_code")
    @classmethod
    def validate_model_code(cls, v: str) -> str:
        """Validate model code format"""
        return FieldValidators.validate_code(v, "model_code")

    @field_validator("model_status")
    @classmethod
    def validate_model_status(cls, v: str) -> str:
        """Validate model status"""
        allowed_statuses = ["TRAINING", "VALIDATED", "STAGING", "PRODUCTION", "DEPRECATED", "ARCHIVED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "model_status")

    @field_validator("domain")
    @classmethod
    def validate_domain(cls, v: str) -> str:
        """Validate domain"""
        allowed_domains = ["customer", "editorial", "company"]
        return FieldValidators.validate_enum(v.lower(), allowed_domains, "domain")

    @field_validator("deployment_environment")
    @classmethod
    def validate_deployment_environment(cls, v: Optional[str]) -> Optional[str]:
        """Validate deployment environment"""
        if v is None:
            return None
        allowed_environments = ["development", "staging", "production"]
        return FieldValidators.validate_enum(v.lower(), allowed_environments, "deployment_environment")


class ModelRegistry(ModelRegistryBase):
    """Model registry schema (read)"""

    pass


class ModelRegistryCreate(ModelRegistryBase):
    """Model registry creation schema"""

    model_id: Optional[UUID] = Field(default=None, description="Model ID (auto-generated if not provided)")

    @field_validator("model_id", mode="before")
    @classmethod
    def generate_model_id(cls, v: Optional[UUID]) -> UUID:
        """Generate model ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "model_id")


class ModelRegistryUpdate(BaseEntityWithCompanyBrand):
    """Model registry update schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    model_status: Optional[str] = Field(default=None, max_length=50, description="Model status")
    is_active: Optional[bool] = Field(default=None, description="Is model active")
    deployed_at: Optional[datetime] = Field(default=None, description="Deployed at timestamp")
    deployment_environment: Optional[str] = Field(default=None, max_length=50, description="Deployment environment")
    is_latest_version: Optional[bool] = Field(default=None, description="Is latest version")
    performance_metrics: Optional[Dict[str, Any]] = Field(default=None, description="Performance metrics")

    @field_validator("model_status")
    @classmethod
    def validate_model_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate model status"""
        if v is None:
            return None
        allowed_statuses = ["TRAINING", "VALIDATED", "STAGING", "PRODUCTION", "DEPRECATED", "ARCHIVED"]
        return FieldValidators.validate_enum(v.upper(), allowed_statuses, "model_status")

