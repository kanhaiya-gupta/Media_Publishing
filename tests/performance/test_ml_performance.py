"""
Performance Tests for ML Components
====================================
"""

import pytest
import time
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np

from tests.performance.conftest import perf_monitor, performance_config
from src.ownlens.ml.models.customer.churn.trainer import ChurnTrainer
from src.ownlens.ml.models.customer.churn.predictor import ChurnPredictor
from src.ownlens.ml.models.editorial.performance.trainer import ArticlePerformanceTrainer
from src.ownlens.ml.models.editorial.performance.predictor import ArticlePerformancePredictor
from src.ownlens.ml.registry.model_registry import ModelRegistry
from src.ownlens.ml.storage.prediction_storage import PredictionStorage
from src.ownlens.ml.monitoring.performance_monitor import PerformanceMonitor
from src.ownlens.ml.orchestration.ml_workflow import run_ml_workflow


class TestMLTrainerPerformance:
    """Performance tests for ML trainers."""
    
    def test_churn_trainer_training_speed(self, perf_monitor):
        """Test churn trainer training speed."""
        trainer = ChurnTrainer()
        
        # Generate large training dataset
        n_samples = 100000
        mock_data = pd.DataFrame({
            "user_id": [f"user_{i}" for i in range(n_samples)],
            "feature1": np.random.rand(n_samples),
            "feature2": np.random.rand(n_samples),
            "feature3": np.random.rand(n_samples),
            "churn_label": np.random.randint(0, 2, n_samples),
        })
        
        trainer.model.train = Mock(return_value={"accuracy": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"accuracy": 0.85, "roc_auc": 0.80})
        
        perf_monitor.start()
        
        with patch('src.ownlens.ml.models.customer.churn.trainer.UserFeaturesLoader') as mock_loader:
            mock_loader_instance = Mock()
            mock_loader_instance.load.return_value = mock_data
            mock_loader.return_value = mock_loader_instance
            
            metrics = trainer.train(limit=n_samples)
            assert metrics is not None
        
        perf_metrics = perf_monitor.stop()
        
        # Training 100K samples should complete in reasonable time
        assert perf_metrics["execution_time"] < 120.0  # < 2 minutes
        assert perf_metrics["memory_delta"] < 2000  # < 2GB memory increase
    
    def test_trainer_with_large_features(self, perf_monitor):
        """Test trainer performance with large feature sets."""
        trainer = ChurnTrainer()
        
        # Generate dataset with many features
        n_samples = 10000
        n_features = 1000
        feature_dict = {f"feature_{i}": np.random.rand(n_samples) for i in range(n_features)}
        feature_dict["user_id"] = [f"user_{i}" for i in range(n_samples)]
        feature_dict["churn_label"] = np.random.randint(0, 2, n_samples)
        
        mock_data = pd.DataFrame(feature_dict)
        
        trainer.model.train = Mock(return_value={"accuracy": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"accuracy": 0.85})
        
        perf_monitor.start()
        
        with patch('src.ownlens.ml.models.customer.churn.trainer.UserFeaturesLoader') as mock_loader:
            mock_loader_instance = Mock()
            mock_loader_instance.load.return_value = mock_data
            mock_loader.return_value = mock_loader_instance
            
            metrics = trainer.train(limit=n_samples)
            assert metrics is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 180.0  # < 3 minutes for 1000 features
        assert perf_metrics["memory_delta"] < 3000  # < 3GB memory
    
    def test_trainer_batch_processing(self, perf_monitor):
        """Test trainer performance with batch processing."""
        trainer = ChurnTrainer()
        
        trainer.model.train = Mock(return_value={"accuracy": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"accuracy": 0.85})
        
        perf_monitor.start()
        
        # Simulate multiple training runs
        for batch in range(10):
            n_samples = 10000
            mock_data = pd.DataFrame({
                "user_id": [f"user_{i}" for i in range(n_samples)],
                "feature1": np.random.rand(n_samples),
                "churn_label": np.random.randint(0, 2, n_samples),
            })
            
            with patch('src.ownlens.ml.models.customer.churn.trainer.UserFeaturesLoader') as mock_loader:
                mock_loader_instance = Mock()
                mock_loader_instance.load.return_value = mock_data
                mock_loader.return_value = mock_loader_instance
                
                metrics = trainer.train(limit=n_samples)
                assert metrics is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 300.0  # < 5 minutes for 10 batches
        assert perf_metrics["memory_delta"] < 2000  # No significant memory leak


class TestMLPredictorPerformance:
    """Performance tests for ML predictors."""
    
    def test_churn_predictor_prediction_speed(self, perf_monitor):
        """Test churn predictor prediction speed."""
        predictor = ChurnPredictor()
        
        # Generate large prediction dataset
        n_samples = 100000
        mock_data = pd.DataFrame({
            "user_id": [f"user_{i}" for i in range(n_samples)],
            "feature1": np.random.rand(n_samples),
            "feature2": np.random.rand(n_samples),
            "feature3": np.random.rand(n_samples),
        })
        
        predictor.model.predict = Mock(return_value=np.random.randint(0, 2, n_samples))
        predictor.model.predict_proba = Mock(return_value=np.random.rand(n_samples, 2))
        
        perf_monitor.start()
        
        predictions = predictor.predict(mock_data)
        
        perf_metrics = perf_monitor.stop()
        
        assert predictions is not None
        # Should predict 100K samples in < 10 seconds
        assert perf_metrics["execution_time"] < 10.0
        assert perf_metrics["memory_delta"] < 500  # < 500MB memory
    
    def test_predictor_throughput(self, perf_monitor):
        """Test predictor throughput with multiple predictions."""
        predictor = ChurnPredictor()
        
        predictor.model.predict = Mock(return_value=np.array([0, 1, 0]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3], [0.2, 0.8], [0.6, 0.4]]))
        
        mock_data = pd.DataFrame({
            "user_id": ["user_1", "user_2", "user_3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        perf_monitor.start()
        
        # Simulate high-throughput predictions
        for _ in range(1000):
            predictions = predictor.predict(mock_data)
            assert predictions is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 30.0  # 1000 predictions in < 30 seconds
        assert perf_metrics["memory_delta"] < 500  # No memory leak
    
    def test_predictor_latency(self, perf_monitor):
        """Test predictor latency for single predictions."""
        predictor = ChurnPredictor()
        
        predictor.model.predict = Mock(return_value=np.array([0]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3]]))
        
        mock_data = pd.DataFrame({
            "user_id": ["user_1"],
            "feature1": [1.0],
        })
        
        latencies = []
        
        # Measure latency for single predictions
        for _ in range(100):
            perf_monitor.start()
            prediction = predictor.predict(mock_data)
            metrics = perf_monitor.stop()
            latencies.append(metrics["execution_time"])
            assert prediction is not None
        
        avg_latency = sum(latencies) / len(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        # Average latency should be < 100ms
        assert avg_latency < 0.1
        # P95 latency should be < 200ms
        assert p95_latency < 0.2


class TestMLInfrastructurePerformance:
    """Performance tests for ML infrastructure components."""
    
    def test_model_registry_throughput(self, perf_monitor):
        """Test model registry throughput."""
        registry = ModelRegistry()
        
        mock_client = Mock()
        mock_client.execute.return_value = None
        registry.client = mock_client
        
        perf_monitor.start()
        
        # Simulate multiple model registrations
        for i in range(100):
            with patch.object(registry, 'register_model', return_value=f"model_{i}"):
                model_id = registry.register_model(
                    model_name="test_model",
                    model_version="1.0.0",
                    model_path="/tmp/model"
                )
                assert model_id is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 10.0
        assert perf_metrics["memory_delta"] < 500
    
    def test_prediction_storage_throughput(self, perf_monitor):
        """Test prediction storage throughput."""
        storage = PredictionStorage()
        
        mock_client = Mock()
        mock_client.execute.return_value = None
        storage.client = mock_client
        
        perf_monitor.start()
        
        # Simulate batch prediction storage
        for i in range(1000):
            with patch.object(storage, 'save_prediction', return_value=f"pred_{i}"):
                pred_id = storage.save_prediction(
                    model_id="model_id",
                    entity_id=f"user_{i}",
                    entity_type="user",
                    prediction_type="churn",
                    prediction_probability=0.85,
                    prediction_class="churn"
                )
                assert pred_id is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 15.0
        assert perf_metrics["memory_delta"] < 500
    
    def test_performance_monitor_throughput(self, perf_monitor):
        """Test performance monitor throughput."""
        monitor = PerformanceMonitor()
        
        mock_client = Mock()
        mock_client.execute.return_value = None
        monitor.client = mock_client
        
        perf_monitor.start()
        
        # Simulate multiple metric calculations
        for _ in range(500):
            with patch.object(monitor, 'calculate_accuracy', return_value={"accuracy": 0.85}):
                metrics = monitor.calculate_accuracy(
                    predictions=[0, 1, 0],
                    actuals=[0, 1, 1]
                )
                assert metrics is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 10.0
        assert perf_metrics["memory_delta"] < 500


class TestMLWorkflowPerformance:
    """Performance tests for ML workflow orchestration."""
    
    def test_ml_workflow_throughput(self, perf_monitor):
        """Test ML workflow throughput."""
        perf_monitor.start()
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config') as mock_config, \
             patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer:
            
            mock_config_instance = Mock()
            mock_config_instance.ml_figures_dir = "figures/ml"
            mock_config.return_value = mock_config_instance
            
            mock_loader_instance = Mock()
            mock_loader_instance.load.return_value = [{"user_id": "1", "feature1": 1.0}]
            mock_loader.return_value = mock_loader_instance
            
            mock_trainer_instance = Mock()
            mock_trainer_instance.train.return_value = {"accuracy": 0.9}
            mock_trainer.return_value = mock_trainer_instance
            
            # Simulate multiple workflow runs
            for _ in range(10):
                result = run_ml_workflow(domain="customer", limit=1000)
                assert result is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 30.0
        assert perf_metrics["memory_delta"] < 1000
    
    def test_ml_workflow_with_large_data(self, perf_monitor):
        """Test ML workflow performance with large datasets."""
        perf_monitor.start()
        
        with patch('src.ownlens.ml.orchestration.ml_workflow.get_ml_config') as mock_config, \
             patch('src.ownlens.ml.orchestration.ml_workflow.UserFeaturesLoader') as mock_loader, \
             patch('src.ownlens.ml.orchestration.ml_workflow.ChurnTrainer') as mock_trainer:
            
            mock_config_instance = Mock()
            mock_config_instance.ml_figures_dir = "figures/ml"
            mock_config.return_value = mock_config_instance
            
            # Simulate large dataset
            n_samples = 100000
            mock_data = [{"user_id": f"user_{i}", "feature1": float(i)} for i in range(n_samples)]
            
            mock_loader_instance = Mock()
            mock_loader_instance.load.return_value = mock_data
            mock_loader.return_value = mock_loader_instance
            
            mock_trainer_instance = Mock()
            mock_trainer_instance.train.return_value = {"accuracy": 0.9}
            mock_trainer.return_value = mock_trainer_instance
            
            result = run_ml_workflow(domain="customer", limit=n_samples)
            assert result is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 180.0  # < 3 minutes for 100K samples
        assert perf_metrics["memory_delta"] < 3000  # < 3GB memory


class TestMLStressTests:
    """Stress tests for ML components."""
    
    def test_trainer_stress_test(self, perf_monitor):
        """Stress test for trainers with high iteration count."""
        trainer = ChurnTrainer()
        
        trainer.model.train = Mock(return_value={"accuracy": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"accuracy": 0.85})
        
        perf_monitor.start()
        
        # High iteration stress test
        for i in range(100):
            mock_data = pd.DataFrame({
                "user_id": [f"user_{j}" for j in range(1000)],
                "feature1": np.random.rand(1000),
                "churn_label": np.random.randint(0, 2, 1000),
            })
            
            with patch('src.ownlens.ml.models.customer.churn.trainer.UserFeaturesLoader') as mock_loader:
                mock_loader_instance = Mock()
                mock_loader_instance.load.return_value = mock_data
                mock_loader.return_value = mock_loader_instance
                
                metrics = trainer.train(limit=1000)
                assert metrics is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 300.0  # < 5 minutes
        assert perf_metrics["memory_delta"] < 2000  # No memory leak
    
    def test_predictor_stress_test(self, perf_monitor):
        """Stress test for predictors with high iteration count."""
        predictor = ChurnPredictor()
        
        predictor.model.predict = Mock(return_value=np.array([0, 1]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3], [0.2, 0.8]]))
        
        mock_data = pd.DataFrame({
            "user_id": ["user_1", "user_2"],
            "feature1": [1.0, 2.0],
        })
        
        perf_monitor.start()
        
        # High iteration stress test
        for _ in range(10000):
            predictions = predictor.predict(mock_data)
            assert predictions is not None
        
        perf_metrics = perf_monitor.stop()
        
        assert perf_metrics["execution_time"] < 60.0  # < 1 minute
        assert perf_metrics["memory_delta"] < 500  # No memory leak

