"""
End-to-End Tests for ML Workflow
=================================
"""

import pytest


@pytest.mark.e2e
@pytest.mark.ml
class TestMLWorkflowE2E:
    """End-to-end tests for ML workflow."""

    def test_complete_ml_workflow_customer(self):
        """Test complete ML workflow for customer domain."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow
        
        results = run_ml_workflow(domain="customer", limit=100)
        
        assert results is not None
        assert "workflow_id" in results or isinstance(results, dict)

    def test_complete_ml_workflow_editorial(self):
        """Test complete ML workflow for editorial domain."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow
        
        results = run_ml_workflow(domain="editorial", limit=100)
        
        assert results is not None

    def test_ml_workflow_with_specific_model(self):
        """Test ML workflow with specific model."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow
        
        results = run_ml_workflow(
            domain="customer",
            model_code="churn_prediction",
            limit=100
        )
        
        assert results is not None

    def test_ml_workflow_model_versioning(self):
        """Test ML workflow model versioning."""
        # Skip if test environment is not available
        pytest.skip("Requires full test environment with Docker Compose")
        
        from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow
        from src.ownlens.ml.registry.model_registry import ModelRegistry
        
        # Run workflow
        results = run_ml_workflow(domain="customer", limit=100)
        
        # Verify models were registered
        registry = ModelRegistry()
        models = registry.list_models(model_code="churn_prediction")
        
        assert results is not None
        assert models is not None

