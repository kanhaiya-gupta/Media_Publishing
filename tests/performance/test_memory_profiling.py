"""
Memory Profiling Tests
=======================
"""

import pytest
import psutil
import os
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np

from tests.performance.conftest import perf_monitor
from src.ownlens.processing.etl.transformers.customer.customer import CustomerTransformer
from src.ownlens.processing.etl.loaders.postgresql_loader import PostgreSQLLoader
from src.ownlens.ml.models.customer.churn.trainer import ChurnTrainer
from src.ownlens.ml.models.customer.churn.predictor import ChurnPredictor


class TestMemoryProfiling:
    """Memory profiling tests for components."""
    
    def test_transformer_memory_profiling(self, mock_spark_session, perf_monitor):
        """Profile memory usage of transformers."""
        transformer = CustomerTransformer(mock_spark_session)
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Simulate multiple transformations
        for i in range(100):
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10000
            mock_df.columns = ["user_id", "email", "name"]
            mock_df.withColumn = Mock(return_value=mock_df)
            mock_df.drop = Mock(return_value=mock_df)
            
            result = transformer.transform(mock_df)
            assert result is not None
            
            # Check memory after each transformation
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should not grow unbounded
            assert memory_increase < 1000  # < 1GB increase
        
        final_memory = process.memory_info().rss / 1024 / 1024
        total_increase = final_memory - initial_memory
        
        # Total memory increase should be reasonable
        assert total_increase < 500  # < 500MB total increase
    
    def test_loader_memory_profiling(self, mock_spark_session, test_config, perf_monitor):
        """Profile memory usage of loaders."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100000  # Large dataset
        mock_df.columns = ["id", "name", "email"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        # Simulate multiple loads
        for _ in range(50):
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(mock_df, table="test_table")
                assert result is True
            
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should not grow unbounded
            assert memory_increase < 2000  # < 2GB increase
        
        final_memory = process.memory_info().rss / 1024 / 1024
        total_increase = final_memory - initial_memory
        
        assert total_increase < 1000  # < 1GB total increase
    
    def test_trainer_memory_profiling(self, perf_monitor):
        """Profile memory usage of trainers."""
        trainer = ChurnTrainer()
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        trainer.model.train = Mock(return_value={"accuracy": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"accuracy": 0.85})
        
        # Simulate training with increasing dataset sizes
        for size in [1000, 5000, 10000, 50000]:
            mock_data = pd.DataFrame({
                "user_id": [f"user_{i}" for i in range(size)],
                "feature1": np.random.rand(size),
                "churn_label": np.random.randint(0, 2, size),
            })
            
            with patch('src.ownlens.ml.models.customer.churn.trainer.UserFeaturesLoader') as mock_loader:
                mock_loader_instance = Mock()
                mock_loader_instance.load.return_value = mock_data
                mock_loader.return_value = mock_loader_instance
                
                metrics = trainer.train(limit=size)
                assert metrics is not None
            
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should scale reasonably with data size
            assert memory_increase < size * 0.01  # < 10KB per sample
        
        final_memory = process.memory_info().rss / 1024 / 1024
        total_increase = final_memory - initial_memory
        
        assert total_increase < 2000  # < 2GB total increase
    
    def test_predictor_memory_profiling(self, perf_monitor):
        """Profile memory usage of predictors."""
        predictor = ChurnPredictor()
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        predictor.model.predict = Mock(return_value=np.array([0, 1, 0]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3], [0.2, 0.8], [0.6, 0.4]]))
        
        # Simulate predictions with increasing batch sizes
        for batch_size in [100, 1000, 10000, 100000]:
            mock_data = pd.DataFrame({
                "user_id": [f"user_{i}" for i in range(batch_size)],
                "feature1": np.random.rand(batch_size),
            })
            
            predictions = predictor.predict(mock_data)
            assert predictions is not None
            
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_increase = current_memory - initial_memory
            
            # Memory should scale reasonably with batch size
            assert memory_increase < batch_size * 0.001  # < 1KB per sample
        
        final_memory = process.memory_info().rss / 1024 / 1024
        total_increase = final_memory - initial_memory
        
        assert total_increase < 500  # < 500MB total increase
    
    def test_memory_leak_detection(self, mock_spark_session, perf_monitor):
        """Detect memory leaks in transformers."""
        transformer = CustomerTransformer(mock_spark_session)
        
        process = psutil.Process(os.getpid())
        memory_samples = []
        
        # Run many iterations and sample memory
        for i in range(1000):
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 1000
            mock_df.columns = ["user_id", "email", "name"]
            mock_df.withColumn = Mock(return_value=mock_df)
            mock_df.drop = Mock(return_value=mock_df)
            
            result = transformer.transform(mock_df)
            assert result is not None
            
            # Sample memory every 100 iterations
            if i % 100 == 0:
                current_memory = process.memory_info().rss / 1024 / 1024
                memory_samples.append(current_memory)
        
        # Check for memory leak (memory should not grow linearly)
        if len(memory_samples) > 1:
            memory_growth = memory_samples[-1] - memory_samples[0]
            # Memory growth should be minimal (< 200MB over 1000 iterations)
            assert memory_growth < 200
    
    def test_memory_cleanup(self, mock_spark_session, perf_monitor):
        """Test memory cleanup after operations."""
        transformer = CustomerTransformer(mock_spark_session)
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024
        
        # Run operations
        for _ in range(100):
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10000
            mock_df.columns = ["user_id", "email", "name"]
            mock_df.withColumn = Mock(return_value=mock_df)
            mock_df.drop = Mock(return_value=mock_df)
            
            result = transformer.transform(mock_df)
            assert result is not None
        
        # Force garbage collection
        import gc
        gc.collect()
        
        final_memory = process.memory_info().rss / 1024 / 1024
        memory_increase = final_memory - initial_memory
        
        # After GC, memory should be reasonable
        assert memory_increase < 500  # < 500MB increase

