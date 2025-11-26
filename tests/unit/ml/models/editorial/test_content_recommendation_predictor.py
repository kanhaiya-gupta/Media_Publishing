"""
Unit Tests for ContentRecommendationPredictor
=============================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.editorial.recommendation.predictor import ContentRecommendationPredictor


class TestContentRecommendationPredictor:
    """Test ContentRecommendationPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = ContentRecommendationPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = ContentRecommendationPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[["a1", "a2"], ["a3", "a4"]])
        
        data = pd.DataFrame({
            "user_id": ["u1", "u2"],
            "article_id": ["a1", "a2"],
        })
        
        recommendations = predictor.predict(data, n_recommendations=2)
        
        assert recommendations is not None
        assert len(recommendations) > 0

