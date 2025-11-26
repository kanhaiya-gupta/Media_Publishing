"""
Integration Tests for ML Predictions
=====================================
"""

import pytest


@pytest.mark.integration
@pytest.mark.ml
class TestMLPredictions:
    """Integration tests for ML predictions."""

    def test_churn_prediction_integration(self):
        """Test churn prediction with test data."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database with data")
        
        from src.ownlens.ml.models.customer.churn.predictor import ChurnPredictor
        from src.ownlens.ml.registry.model_registry import ModelRegistry
        import pandas as pd
        
        registry = ModelRegistry()
        model = registry.get_model("churn_prediction_latest")
        
        predictor = ChurnPredictor(model=model)
        data = pd.DataFrame({
            "user_id": ["1", "2"],
            "feature1": [1.0, 2.0],
        })
        
        predictions = predictor.predict(data)
        
        assert predictions is not None

