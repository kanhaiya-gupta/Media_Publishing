"""
OwnLens - ML Models Domain: Model Monitoring Model

Model monitoring validation models.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from ..base import BaseEntityWithCompanyBrand
from ..field_validations import FieldValidators


class ModelMonitoringBase(BaseEntityWithCompanyBrand):
    """Base model monitoring schema"""
    
    model_config = ConfigDict(protected_namespaces=())

    monitoring_id: UUID = Field(description="Monitoring ID")
    model_id: UUID = Field(description="Model ID")
    monitoring_date: date = Field(description="Monitoring date")
    monitoring_start_time: datetime = Field(description="Monitoring start time")
    monitoring_end_time: datetime = Field(description="Monitoring end time")
    total_predictions: int = Field(default=0, ge=0, description="Total predictions")
    predictions_per_hour: Optional[float] = Field(default=None, ge=0, description="Predictions per hour")
    avg_prediction_latency_ms: Optional[float] = Field(default=None, ge=0, description="Average prediction latency (ms)")
    p95_prediction_latency_ms: Optional[float] = Field(default=None, ge=0, description="P95 prediction latency (ms)")
    p99_prediction_latency_ms: Optional[float] = Field(default=None, ge=0, description="P99 prediction latency (ms)")
    accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Accuracy")
    precision: Optional[float] = Field(default=None, ge=0, le=1, description="Precision")
    recall: Optional[float] = Field(default=None, ge=0, le=1, description="Recall")
    f1_score: Optional[float] = Field(default=None, ge=0, le=1, description="F1 score")
    auc_roc: Optional[float] = Field(default=None, ge=0, le=1, description="AUC-ROC")
    data_drift_score: Optional[float] = Field(default=None, ge=0, le=1, description="Data drift score")
    feature_drift_scores: Optional[Dict[str, Any]] = Field(default=None, description="Feature drift scores")
    drift_detected: bool = Field(default=False, description="Drift detected")
    performance_drift_score: Optional[float] = Field(default=None, ge=0, le=1, description="Performance drift score")
    performance_drift_detected: bool = Field(default=False, description="Performance drift detected")
    prediction_distribution: Optional[Dict[str, Any]] = Field(default=None, description="Prediction distribution")
    confidence_distribution: Optional[Dict[str, Any]] = Field(default=None, description="Confidence distribution")
    error_count: int = Field(default=0, ge=0, description="Error count")
    error_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Error rate")
    error_types: Optional[Dict[str, Any]] = Field(default=None, description="Error types")
    alerts_triggered: int = Field(default=0, ge=0, description="Alerts triggered")
    alert_types: Optional[List[str]] = Field(default=None, description="Alert types")

    @field_validator("accuracy", "precision", "recall", "f1_score", "auc_roc", "data_drift_score", "performance_drift_score", "error_rate")
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Validate percentage (0-1)"""
        return FieldValidators.validate_percentage(v, 0.0, 1.0) if v is not None else None


class ModelMonitoring(ModelMonitoringBase):
    """Model monitoring schema (read)"""

    pass


class ModelMonitoringCreate(ModelMonitoringBase):
    """Model monitoring creation schema"""

    monitoring_id: Optional[UUID] = Field(default=None, description="Monitoring ID (auto-generated if not provided)")

    @field_validator("monitoring_id", mode="before")
    @classmethod
    def generate_monitoring_id(cls, v: Optional[UUID]) -> UUID:
        """Generate monitoring ID if not provided"""
        if v is None:
            from uuid import uuid4
            return uuid4()
        return FieldValidators.validate_uuid(v, "monitoring_id")


class ModelMonitoringUpdate(BaseEntityWithCompanyBrand):
    """Model monitoring update schema"""

    total_predictions: Optional[int] = Field(default=None, ge=0, description="Total predictions")
    accuracy: Optional[float] = Field(default=None, ge=0, le=1, description="Accuracy")
    precision: Optional[float] = Field(default=None, ge=0, le=1, description="Precision")
    recall: Optional[float] = Field(default=None, ge=0, le=1, description="Recall")
    f1_score: Optional[float] = Field(default=None, ge=0, le=1, description="F1 score")
    data_drift_score: Optional[float] = Field(default=None, ge=0, le=1, description="Data drift score")
    drift_detected: Optional[bool] = Field(default=None, description="Drift detected")
    error_count: Optional[int] = Field(default=None, ge=0, description="Error count")
    error_rate: Optional[float] = Field(default=None, ge=0, le=1, description="Error rate")

