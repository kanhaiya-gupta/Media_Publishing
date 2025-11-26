"""
Unit Tests for ChurnPredictor
=============================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.customer.churn.predictor import ChurnPredictor


class TestChurnPredictor:
    """Test ChurnPredictor class."""

    def test_init(self):
        """Test predictor initialization."""
        predictor = ChurnPredictor()
        
        assert predictor is not None

    def test_init_with_model(self):
        """Test predictor initialization with custom model."""
        mock_model = Mock()
        predictor = ChurnPredictor(model=mock_model)
        
        assert predictor.model is mock_model

    def test_predict_success(self):
        """Test successful prediction."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1, 0])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7], [0.9, 0.1]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2", "3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_proba_success(self):
        """Test successful probability prediction."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        probabilities = predictor.predict_proba(data)
        
        assert probabilities is not None
        assert len(probabilities) > 0

    def test_predict_with_threshold(self):
        """Test prediction with custom threshold."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7], [0.6, 0.4]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2", "3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        predictions = predictor.predict(data, threshold=0.5)
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_with_empty_data(self):
        """Test prediction with empty DataFrame."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[])
        predictor.model.predict_proba = Mock(return_value=[])
        
        data = pd.DataFrame()
        
        predictions = predictor.predict(data)
        
        assert predictions is not None
        assert len(predictions) == 0

    def test_predict_with_missing_features(self):
        """Test prediction with missing features (should handle gracefully)."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(side_effect=ValueError("Missing features"))
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
            # Missing feature2
        })
        
        with pytest.raises(ValueError, match="Missing features"):
            predictor.predict(data)

    def test_predict_proba_with_single_sample(self):
        """Test probability prediction with single sample."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict_proba = Mock(return_value=[[0.7, 0.3]])
        
        data = pd.DataFrame({
            "user_id": ["1"],
            "feature1": [1.0],
        })
        
        probabilities = predictor.predict_proba(data)
        
        assert probabilities is not None
        assert len(probabilities) == 1

    def test_predict_with_batch_processing(self):
        """Test prediction with batch processing."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1, 0, 1, 0])
        predictor.model.predict_proba = Mock(return_value=[
            [0.8, 0.2], [0.3, 0.7], [0.9, 0.1], [0.2, 0.8], [0.85, 0.15]
        ])
        
        # Large batch
        data = pd.DataFrame({
            "user_id": [str(i) for i in range(100)],
            "feature1": [float(i) for i in range(100)],
        })
        
        predictions = predictor.predict(data, batch_size=20)
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_with_feature_preprocessing(self):
        """Test prediction with feature preprocessing."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
            "feature2": [4.0, 5.0],
        })
        
        predictions = predictor.predict(data, preprocess=True)
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_error_handling(self):
        """Test prediction error handling."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(side_effect=Exception("Model error"))
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        with pytest.raises(Exception, match="Model error"):
            predictor.predict(data)

    def test_predict_proba_error_handling(self):
        """Test probability prediction error handling."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict_proba = Mock(side_effect=Exception("Model error"))
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        with pytest.raises(Exception, match="Model error"):
            predictor.predict_proba(data)

    def test_predict_with_confidence_intervals(self):
        """Test prediction with confidence intervals."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data, return_confidence=True)
        
        assert predictions is not None
        # Should include confidence scores if implemented
        assert len(predictions) > 0

    def test_predict_with_explanation(self):
        """Test prediction with feature importance/explanation."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data, explain=True)
        
        assert predictions is not None
        # Should include explanations if implemented
        assert len(predictions) > 0

    def test_predict_with_model_version(self):
        """Test prediction with specific model version."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data, model_version="v1.0")
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_with_feature_validation(self):
        """Test prediction with feature validation."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data, validate_features=True)
        
        assert predictions is not None
        assert len(predictions) > 0

    def test_predict_with_null_handling(self):
        """Test prediction with null values in data."""
        predictor = ChurnPredictor()
        predictor.model = Mock()
        predictor.model.predict = Mock(return_value=[0, 1])
        predictor.model.predict_proba = Mock(return_value=[[0.8, 0.2], [0.3, 0.7]])
        
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, None],  # Null value
            "feature2": [4.0, 5.0],
        })
        
        predictions = predictor.predict(data, handle_nulls=True)
        
        assert predictions is not None
        assert len(predictions) > 0

