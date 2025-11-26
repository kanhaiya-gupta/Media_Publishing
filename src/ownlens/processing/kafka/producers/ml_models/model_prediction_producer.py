"""
Model Prediction Producer
==========================

Kafka producer for ML model predictions.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.ml_models.model_prediction import ModelPrediction, ModelPredictionCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ModelPredictionProducer(BaseKafkaProducer):
    """
    Kafka producer for ML model predictions.
    
    Publishes model predictions to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "model-predictions",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize model prediction producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.ModelPredictionProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate model prediction message against Pydantic model."""
        try:
            if isinstance(message, ModelPredictionCreate):
                validated = message
            else:
                validated = ModelPredictionCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid model prediction message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating model prediction message: {str(e)}")
    
    def publish_model_prediction(
        self,
        prediction: ModelPredictionCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish model prediction to Kafka topic."""
        try:
            if key is None:
                key = str(prediction.prediction_id)
            return self.send(message=prediction.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing model prediction: {e}", exc_info=True)
            return False

