"""
OwnLens - Customer Domain Schemas

Customer analytics: user behavior, sessions, events, engagement, ML features.
"""

from .user_event import UserEvent, UserEventCreate, UserEventUpdate
from .session import Session, SessionCreate, SessionUpdate
from .user import User, UserCreate, UserUpdate
from .user_features import UserFeatures, UserFeaturesCreate, UserFeaturesUpdate
from .user_segment import UserSegment, UserSegmentCreate, UserSegmentUpdate
from .churn_prediction import ChurnPrediction, ChurnPredictionCreate, ChurnPredictionUpdate
from .recommendation import Recommendation, RecommendationCreate, RecommendationUpdate
from .conversion_prediction import ConversionPrediction, ConversionPredictionCreate, ConversionPredictionUpdate

__all__ = [
    "UserEvent",
    "UserEventCreate",
    "UserEventUpdate",
    "Session",
    "SessionCreate",
    "SessionUpdate",
    "User",
    "UserCreate",
    "UserUpdate",
    "UserFeatures",
    "UserFeaturesCreate",
    "UserFeaturesUpdate",
    "UserSegment",
    "UserSegmentCreate",
    "UserSegmentUpdate",
    "ChurnPrediction",
    "ChurnPredictionCreate",
    "ChurnPredictionUpdate",
    "Recommendation",
    "RecommendationCreate",
    "RecommendationUpdate",
    "ConversionPrediction",
    "ConversionPredictionCreate",
    "ConversionPredictionUpdate",
]

