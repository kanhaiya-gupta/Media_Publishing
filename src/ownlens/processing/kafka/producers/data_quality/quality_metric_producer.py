"""
Quality Metric Producer
========================

Kafka producer for data quality metrics.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.data_quality.quality_metric import QualityMetric, QualityMetricCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class QualityMetricProducer(BaseKafkaProducer):
    """
    Kafka producer for data quality metrics.
    
    Publishes quality metrics to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "quality-metrics",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize quality metric producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.QualityMetricProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate quality metric message against Pydantic model."""
        try:
            if isinstance(message, QualityMetricCreate):
                validated = message
            else:
                validated = QualityMetricCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid quality metric message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating quality metric message: {str(e)}")
    
    def publish_quality_metric(
        self,
        quality_metric: QualityMetricCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish quality metric to Kafka topic."""
        try:
            if key is None:
                key = f"{quality_metric.table_name}_{quality_metric.metric_date}"
            return self.send(message=quality_metric.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing quality metric: {e}", exc_info=True)
            return False

