"""
Unit Tests for UserRecommendationPredictor
==========================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.customer.recommendation.predictor import UserRecommendationPredictor


class TestUserRecommendationPredictor:
    """Test UserRecommendationPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = UserRecommendationPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = UserRecommendationPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[["a1", "a2"], ["a3", "a4"]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
        })
        
        recommendations = predictor.predict(data, n_recommendations=2)
        
        assert recommendations is not None
        assert len(recommendations) > 0

