"""
Unit Tests for ModelRegistry
============================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import uuid

from src.ownlens.ml.registry.model_registry import ModelRegistry


class TestModelRegistry:
    """Test ModelRegistry class."""

    def test_init(self):
        """Test registry initialization."""
        registry = ModelRegistry()
        
        assert registry is not None
        assert registry.config is not None

    def test_init_with_client(self):
        """Test registry initialization with custom client."""
        mock_client = Mock()
        registry = ModelRegistry(client=mock_client)
        
        assert registry.client is mock_client

    @patch('src.ownlens.ml.registry.model_registry.save_model')
    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_register_model_success(self, mock_client_class, mock_save_model):
        """Test successful model registration."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        mock_save_model.return_value = True
        
        registry = ModelRegistry(client=mock_client)
        
        mock_model = Mock()
        metadata = {
            "model_code": "churn_prediction",
            "model_version": "1.0.0",
            "model_type": "customer",
            "algorithm": "XGBoost"
        }
        metrics = {
            "accuracy": 0.9,
            "roc_auc": 0.85
        }
        
        model_id = registry.register_model(
            model=mock_model,
            metadata=metadata,
            metrics=metrics
        )
        
        assert model_id is not None
        assert isinstance(model_id, str)

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_success(self, mock_client_class):
        """Test successful model retrieval."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.0.0", "customer", "XGBoost", "model_path")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_model("model_id")
        
        assert model is not None

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_list_models(self, mock_client_class):
        """Test listing models."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id_1", "churn_prediction", "1.0.0"),
            ("model_id_2", "conversion_prediction", "1.0.0")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        models = registry.list_models(model_code="churn_prediction")
        
        assert models is not None
        assert len(models) > 0

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_list_models_with_filters(self, mock_client_class):
        """Test listing models with filters."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id_1", "churn_prediction", "1.0.0", "customer", "XGBoost")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        models = registry.list_models(
            model_code="churn_prediction",
            model_type="customer",
            algorithm="XGBoost"
        )
        
        assert models is not None
        assert len(models) > 0

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_list_models_empty_result(self, mock_client_class):
        """Test listing models with empty result."""
        mock_client = Mock()
        mock_client.execute.return_value = []
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        models = registry.list_models(model_code="non_existent")
        
        assert models is not None
        assert len(models) == 0

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_not_found(self, mock_client_class):
        """Test getting model that doesn't exist."""
        mock_client = Mock()
        mock_client.execute.return_value = []
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_model("non_existent_id")
        
        assert model is None

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_latest_model(self, mock_client_class):
        """Test getting latest model version."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.2.0", "customer", "XGBoost", "model_path")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_latest_model("churn_prediction")
        
        assert model is not None

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_by_version(self, mock_client_class):
        """Test getting model by specific version."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.0.0", "customer", "XGBoost", "model_path")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_model_by_version("churn_prediction", "1.0.0")
        
        assert model is not None

    @patch('src.ownlens.ml.registry.model_registry.save_model')
    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_register_model_with_tags(self, mock_client_class, mock_save_model):
        """Test model registration with tags."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        mock_save_model.return_value = True
        
        registry = ModelRegistry(client=mock_client)
        
        mock_model = Mock()
        metadata = {
            "model_code": "churn_prediction",
            "model_version": "1.0.0",
            "model_type": "customer",
            "algorithm": "XGBoost"
        }
        metrics = {"accuracy": 0.9}
        tags = ["production", "customer", "churn"]
        
        model_id = registry.register_model(
            model=mock_model,
            metadata=metadata,
            metrics=metrics,
            tags=tags
        )
        
        assert model_id is not None

    @patch('src.ownlens.ml.registry.model_registry.save_model')
    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_register_model_error_handling(self, mock_client_class, mock_save_model):
        """Test model registration error handling."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        mock_client_class.return_value = mock_client
        mock_save_model.return_value = True
        
        registry = ModelRegistry(client=mock_client)
        
        mock_model = Mock()
        metadata = {"model_code": "churn_prediction"}
        metrics = {"accuracy": 0.9}
        
        with pytest.raises(Exception, match="Database error"):
            registry.register_model(model=mock_model, metadata=metadata, metrics=metrics)

    @patch('src.ownlens.ml.registry.model_registry.save_model')
    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_register_model_save_failure(self, mock_client_class, mock_save_model):
        """Test model registration when model save fails."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        mock_save_model.side_effect = Exception("Save error")
        
        registry = ModelRegistry(client=mock_client)
        
        mock_model = Mock()
        metadata = {"model_code": "churn_prediction"}
        metrics = {"accuracy": 0.9}
        
        with pytest.raises(Exception, match="Save error"):
            registry.register_model(model=mock_model, metadata=metadata, metrics=metrics)

    @patch('src.ownlens.ml.registry.model_registry.load_model')
    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_loads_from_storage(self, mock_client_class, mock_load_model):
        """Test getting model loads from storage."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.0.0", "customer", "XGBoost", "/path/to/model")
        ]
        mock_client_class.return_value = mock_client
        mock_load_model.return_value = Mock()
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_model("model_id")
        
        assert model is not None
        mock_load_model.assert_called()

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_update_model_metrics(self, mock_client_class):
        """Test updating model metrics."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        new_metrics = {"accuracy": 0.95, "roc_auc": 0.90}
        result = registry.update_model_metrics("model_id", new_metrics)
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_update_model_metadata(self, mock_client_class):
        """Test updating model metadata."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        new_metadata = {"description": "Updated model", "status": "production"}
        result = registry.update_model_metadata("model_id", new_metadata)
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_delete_model(self, mock_client_class):
        """Test deleting a model."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        result = registry.delete_model("model_id")
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_mark_model_as_production(self, mock_client_class):
        """Test marking model as production."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        result = registry.mark_as_production("model_id")
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_production_model(self, mock_client_class):
        """Test getting production model."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.0.0", "customer", "XGBoost", "model_path", True)
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        model = registry.get_production_model("churn_prediction")
        
        assert model is not None

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_list_model_versions(self, mock_client_class):
        """Test listing all versions of a model."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id_1", "churn_prediction", "1.0.0"),
            ("model_id_2", "churn_prediction", "1.1.0"),
            ("model_id_3", "churn_prediction", "1.2.0")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        versions = registry.list_model_versions("churn_prediction")
        
        assert versions is not None
        assert len(versions) == 3

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_metrics(self, mock_client_class):
        """Test getting model metrics."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "accuracy", 0.9),
            ("model_id", "roc_auc", 0.85),
            ("model_id", "precision", 0.88)
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        metrics = registry.get_model_metrics("model_id")
        
        assert metrics is not None
        assert len(metrics) > 0

    @patch('src.ownlens.ml.registry.model_registry.Client')
    def test_get_model_metadata(self, mock_client_class):
        """Test getting model metadata."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("model_id", "churn_prediction", "1.0.0", "customer", "XGBoost", "model_path", "2024-01-01")
        ]
        mock_client_class.return_value = mock_client
        
        registry = ModelRegistry(client=mock_client)
        
        metadata = registry.get_model_metadata("model_id")
        
        assert metadata is not None
        assert "model_code" in metadata or isinstance(metadata, dict)

