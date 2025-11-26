"""
OwnLens - ML Module: Prediction Storage

Prediction storage integration for saving predictions to ClickHouse.
"""

from .prediction_storage import PredictionStorage
from .prediction_validator import PredictionValidator

__all__ = [
    "PredictionStorage",
    "PredictionValidator",
]

