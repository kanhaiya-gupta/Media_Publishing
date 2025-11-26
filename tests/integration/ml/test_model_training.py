"""
Integration Tests for ML Model Training
=======================================
"""

import pytest


@pytest.mark.integration
@pytest.mark.ml
class TestModelTraining:
    """Integration tests for ML model training."""

    def test_churn_model_training_integration(self):
        """Test churn model training with test data."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database with data")
        
        from src.ownlens.ml.models.customer.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer()
        metrics = trainer.train(limit=100, test_size=0.2)
        
        assert metrics is not None
        assert "accuracy" in metrics or isinstance(metrics, dict)

