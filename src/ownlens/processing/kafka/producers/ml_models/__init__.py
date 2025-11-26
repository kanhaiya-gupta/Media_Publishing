"""
ML Models Domain Kafka Producers
=================================

ML models-specific event producers.
"""

from .model_prediction_producer import ModelPredictionProducer

__all__ = [
    "ModelPredictionProducer",
]

