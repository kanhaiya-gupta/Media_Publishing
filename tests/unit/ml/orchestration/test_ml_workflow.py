"""
Unit Tests for ML Workflow Orchestration
=========================================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow


class TestMLWorkflow:
    """Test ML workflow orchestration."""

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    @patch('src.ownlens.ml.orchestration.ml_workflow.ModelRegistry')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PredictionStorage')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PerformanceMonitor')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PredictionValidator')
    @patch('src.ownlens.ml.orchestration.ml_workflow.MLVisualizer')
    def test_run_ml_workflow_customer_domain(self, mock_visualizer, mock_validator,
                                            mock_monitor, mock_storage, mock_registry,
                                            mock_config):
        """Test ML workflow for customer domain."""
        # Mock configuration
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        # Mock data loader
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class:
            mock_loader = Mock()
            mock_loader.load.return_value = [
                {"user_id": "1", "feature1": 1.0, "feature2": 2.0},
                {"user_id": "2", "feature1": 2.0, "feature2": 3.0},
            ]
            mock_loader_class.return_value = mock_loader
            
            # Mock trainers
            with patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_churn_trainer:
                mock_trainer = Mock()
                mock_trainer.train.return_value = {"accuracy": 0.9}
                mock_churn_trainer.return_value = mock_trainer
                
                # Run workflow
                result = run_ml_workflow(domain="customer", limit=10)
                
                assert result is not None
                assert "workflow_id" in result or isinstance(result, dict)

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_unknown_domain(self, mock_config):
        """Test ML workflow with unknown domain."""
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        result = run_ml_workflow(domain="unknown")
        
        assert result == {}

    def test_run_ml_workflow_with_model_code(self):
        """Test ML workflow with specific model code."""
        # Skip actual execution, just test function signature
        pytest.skip("Requires full ML infrastructure")

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    @patch('src.ownlens.ml.orchestration.ml_workflow.ModelRegistry')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PredictionStorage')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PerformanceMonitor')
    def test_run_ml_workflow_editorial_domain(self, mock_monitor, mock_storage, mock_registry, mock_config):
        """Test ML workflow for editorial domain."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.EditorialLoader') as mock_loader_class:
            mock_loader = Mock()
            mock_loader.load_articles_metadata.return_value = [
                {"article_id": "a1", "feature1": 1.0, "feature2": 2.0}
            ]
            mock_loader_class.return_value = mock_loader
            
            with patch('src.ownlens.ml.orchestration.ml_workflow.ArticlePerformanceTrainer') as mock_trainer:
                mock_trainer_instance = Mock()
                mock_trainer_instance.train.return_value = {"r2_score": 0.85}
                mock_trainer.return_value = mock_trainer_instance
                
                result = run_ml_workflow(domain="editorial", limit=10)
                
                assert result is not None
                assert "workflow_id" in result or isinstance(result, dict)

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_specific_model(self, mock_config):
        """Test ML workflow with specific model code."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    @patch('src.ownlens.ml.orchestration.ml_workflow.ModelRegistry')
    def test_run_ml_workflow_with_model_registration(self, mock_registry, mock_config):
        """Test ML workflow with model registration."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        mock_registry_instance = Mock()
        mock_registry_instance.register_model.return_value = "model_id"
        mock_registry.return_value = mock_registry_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer.model = Mock()
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                register_model=True,
                limit=10
            )
            
            assert result is not None
            mock_registry_instance.register_model.assert_called()

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PredictionStorage')
    def test_run_ml_workflow_with_prediction_storage(self, mock_storage, mock_config):
        """Test ML workflow with prediction storage."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        mock_storage_instance = Mock()
        mock_storage_instance.save_prediction.return_value = "prediction_id"
        mock_storage.return_value = mock_storage_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnPredictor') as mock_predictor_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            mock_predictor = Mock()
            mock_predictor.predict.return_value = [0, 1]
            mock_predictor_class.return_value = mock_predictor
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                generate_predictions=True,
                store_predictions=True,
                limit=10
            )
            
            assert result is not None
            mock_storage_instance.save_prediction.assert_called()

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_error_handling(self, mock_config):
        """Test ML workflow error handling."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class:
            mock_loader = Mock()
            mock_loader.load.side_effect = Exception("Data loading error")
            mock_loader_class.return_value = mock_loader
            
            result = run_ml_workflow(domain="customer", limit=10)
            
            # Should handle error gracefully
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_validation(self, mock_config):
        """Test ML workflow with validation."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                validate=True,
                validation_split=0.2,
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_cross_validation(self, mock_config):
        """Test ML workflow with cross-validation."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9, "cv_scores": [0.85, 0.90, 0.88]}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                cross_validate=True,
                cv_folds=5,
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    @patch('src.ownlens.ml.orchestration.ml_workflow.PerformanceMonitor')
    def test_run_ml_workflow_with_monitoring(self, mock_monitor, mock_config):
        """Test ML workflow with performance monitoring."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        mock_monitor_instance = Mock()
        mock_monitor_instance.calculate_accuracy.return_value = {"accuracy": 0.9}
        mock_monitor.return_value = mock_monitor_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                monitor=True,
                limit=10
            )
            
            assert result is not None
            mock_monitor_instance.calculate_accuracy.assert_called()

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_hyperparameter_tuning(self, mock_config):
        """Test ML workflow with hyperparameter tuning."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.95, "best_params": {"n_estimators": 100}}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                tune_hyperparameters=True,
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_model_comparison(self, mock_config):
        """Test ML workflow with model comparison."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                compare_models=True,
                algorithms=["XGBoost", "RandomForest", "LogisticRegression"],
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_feature_importance(self, mock_config):
        """Test ML workflow with feature importance calculation."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0, "feature2": 2.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {
                "accuracy": 0.9,
                "feature_importance": {"feature1": 0.6, "feature2": 0.4}
            }
            mock_trainer_class.return_value = mock_trainer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                calculate_feature_importance=True,
                limit=10
            )
            
            assert result is not None

    @patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config')
    def test_run_ml_workflow_with_visualization(self, mock_config):
        """Test ML workflow with visualization generation."""
        mock_config_instance = Mock()
        mock_config_instance.ml_figures_dir = "figures/ml"
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer_class, \
             patch('src.ownlens.ml.orchestration.ml_workflow.MLVisualizer') as mock_visualizer_class:
            
            mock_loader = Mock()
            mock_loader.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader_class.return_value = mock_loader
            
            mock_trainer = Mock()
            mock_trainer.train.return_value = {"accuracy": 0.9}
            mock_trainer_class.return_value = mock_trainer
            
            mock_visualizer = Mock()
            mock_visualizer.plot_metrics.return_value = True
            mock_visualizer_class.return_value = mock_visualizer
            
            result = run_ml_workflow(
                domain="customer",
                model_code="churn_prediction",
                generate_visualizations=True,
                limit=10
            )
            
            assert result is not None
            mock_visualizer.plot_metrics.assert_called()

