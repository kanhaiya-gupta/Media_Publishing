"""
Unit Tests for ConversionPredictor
===================================
"""

import pytest
from unittest.mock import Mock
import pandas as pd

from src.ownlens.ml.models.customer.conversion.predictor import ConversionPredictor


class TestConversionPredictor:
    """Test ConversionPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = ConversionPredictor()
        
        assert predictor is not None

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = ConversionPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.7, 0.3], [0.2, 0.8]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None

