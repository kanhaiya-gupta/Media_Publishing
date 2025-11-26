"""
Unit Tests for UserSegmentationPredictor
=========================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.customer.segmentation.predictor import UserSegmentationPredictor


class TestUserSegmentationPredictor:
    """Test UserSegmentationPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = UserSegmentationPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = UserSegmentationPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1, 2])
        
        data = pd.DataFrame({
            "user_id": ["1", "2", "3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) > 0

