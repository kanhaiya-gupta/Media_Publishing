"""
Integration Tests for Model Registry
=====================================
"""

import pytest


@pytest.mark.integration
@pytest.mark.ml
class TestModelRegistry:
    """Integration tests for model registry."""

    def test_register_model_integration(self):
        """Test model registration with test database."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database")
        
        from src.ownlens.ml.registry.model_registry import ModelRegistry
        from unittest.mock import Mock
        
        registry = ModelRegistry()
        mock_model = Mock()
        
        metadata = {
            "model_code": "test_model",
            "model_version": "1.0.0",
            "model_type": "customer",
            "algorithm": "XGBoost"
        }
        metrics = {
            "accuracy": 0.9
        }
        
        model_id = registry.register_model(
            model=mock_model,
            metadata=metadata,
            metrics=metrics
        )
        
        assert model_id is not None

    def test_list_models_integration(self):
        """Test listing models from registry."""
        # Skip if test environment is not available
        pytest.skip("Requires test ClickHouse database")
        
        from src.ownlens.ml.registry.model_registry import ModelRegistry
        
        registry = ModelRegistry()
        models = registry.list_models()
        
        assert models is not None

