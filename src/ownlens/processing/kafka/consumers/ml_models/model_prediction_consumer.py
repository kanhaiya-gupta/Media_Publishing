"""
Model Prediction Consumer
==========================

Kafka consumer for ML model predictions.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.ml_models.model_prediction import ModelPrediction, ModelPredictionCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ModelPredictionConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for ML model predictions.
    
    Consumes model predictions from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "model-prediction-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize model prediction consumer."""
        if topics is None:
            topics = ["model-predictions"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.ModelPredictionConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate model prediction message against Pydantic model."""
        try:
            validated = ModelPredictionCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid model prediction message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating model prediction message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a model prediction message."""
        try:
            prediction = ModelPredictionCreate.model_validate(message)
            self.logger.info(f"Processing model prediction: {prediction.prediction_id}")
        except Exception as e:
            self.logger.error(f"Error processing model prediction: {e}", exc_info=True)
            raise

