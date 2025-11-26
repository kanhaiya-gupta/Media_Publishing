"""
ML Models Domain Kafka Consumers
=================================

ML models-specific event consumers.
"""

from .model_prediction_consumer import ModelPredictionConsumer

__all__ = [
    "ModelPredictionConsumer",
]

