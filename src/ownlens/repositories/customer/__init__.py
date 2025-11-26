"""
OwnLens - Customer Domain Repositories

Repositories for customer domain entities (sessions, events, features, segments, predictions, recommendations).
"""

from .session_repository import SessionRepository
from .user_event_repository import UserEventRepository
from .user_features_repository import UserFeaturesRepository
from .user_segment_repository import UserSegmentRepository
from .churn_prediction_repository import ChurnPredictionRepository
from .recommendation_repository import RecommendationRepository
from .conversion_prediction_repository import ConversionPredictionRepository

__all__ = [
    "SessionRepository",
    "UserEventRepository",
    "UserFeaturesRepository",
    "UserSegmentRepository",
    "ChurnPredictionRepository",
    "RecommendationRepository",
    "ConversionPredictionRepository",
]

