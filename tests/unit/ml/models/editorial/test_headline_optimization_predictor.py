"""
Unit Tests for HeadlineOptimizationPredictor
============================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.editorial.headline.predictor import HeadlineOptimizationPredictor


class TestHeadlineOptimizationPredictor:
    """Test HeadlineOptimizationPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = HeadlineOptimizationPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = HeadlineOptimizationPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0.05, 0.08, 0.06])
        
        data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "headline": ["headline1", "headline2", "headline3"],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) > 0

