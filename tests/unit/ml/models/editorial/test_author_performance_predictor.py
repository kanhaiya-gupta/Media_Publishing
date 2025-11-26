"""
Unit Tests for AuthorPerformancePredictor
==========================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.editorial.author.predictor import AuthorPerformancePredictor


class TestAuthorPerformancePredictor:
    """Test AuthorPerformancePredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = AuthorPerformancePredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = AuthorPerformancePredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0.8, 0.9, 0.7])
        
        data = pd.DataFrame({
            "author_id": ["auth1", "auth2", "auth3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) > 0

