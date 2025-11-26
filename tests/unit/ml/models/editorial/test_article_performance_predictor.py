"""
Unit Tests for ArticlePerformancePredictor
===========================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.editorial.performance.predictor import ArticlePerformancePredictor


class TestArticlePerformancePredictor:
    """Test ArticlePerformancePredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = ArticlePerformancePredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = ArticlePerformancePredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0.8, 0.9, 0.7])
        
        data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) > 0

