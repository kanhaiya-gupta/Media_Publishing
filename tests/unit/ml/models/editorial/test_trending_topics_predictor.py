"""
Unit Tests for TrendingTopicsPredictor
======================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.editorial.trending.predictor import TrendingTopicsPredictor


class TestTrendingTopicsPredictor:
    """Test TrendingTopicsPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = TrendingTopicsPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = TrendingTopicsPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[["topic1", "topic2"], ["topic3", "topic4"]])
        
        data = pd.DataFrame({
            "article_id": ["a1", "a2"],
            "content": ["text1", "text2"],
        })
        
        predictions = predictor.predict(data, n_topics=2)
        
        assert predictions is not None
        assert len(predictions) > 0

