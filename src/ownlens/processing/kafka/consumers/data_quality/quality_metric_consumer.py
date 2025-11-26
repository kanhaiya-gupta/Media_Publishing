"""
Quality Metric Consumer
========================

Kafka consumer for data quality metrics.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.data_quality.quality_metric import QualityMetric, QualityMetricCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class QualityMetricConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for data quality metrics.
    
    Consumes quality metrics from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "quality-metric-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize quality metric consumer."""
        if topics is None:
            topics = ["quality-metrics"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.QualityMetricConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate quality metric message against Pydantic model."""
        try:
            validated = QualityMetricCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid quality metric message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating quality metric message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a quality metric message."""
        try:
            quality_metric = QualityMetricCreate.model_validate(message)
            self.logger.info(f"Processing quality metric: {quality_metric.metric_id}")
        except Exception as e:
            self.logger.error(f"Error processing quality metric: {e}", exc_info=True)
            raise

