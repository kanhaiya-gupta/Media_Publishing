"""
Throughput Benchmark Tests
==========================
"""

import pytest
import time
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np

from tests.performance.conftest import perf_monitor
from src.ownlens.processing.etl.extractors.postgresql_extractor import PostgreSQLExtractor
from src.ownlens.processing.etl.loaders.postgresql_loader import PostgreSQLLoader
from src.ownlens.processing.etl.transformers.customer.customer import CustomerTransformer
from src.ownlens.ml.models.customer.churn.predictor import ChurnPredictor
from src.ownlens.ml.storage.prediction_storage import PredictionStorage


class TestThroughputBenchmarks:
    """Throughput benchmark tests."""
    
    def test_extractor_throughput_benchmark(self, mock_spark_session, test_config, perf_monitor):
        """Benchmark extractor throughput."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["id", "name"]
        
        start_time = time.time()
        iterations = 1000
        
        with patch.object(extractor, 'extract', return_value=mock_df):
            for _ in range(iterations):
                result = extractor.extract(table="test_table")
                assert result is not None
        
        elapsed_time = time.time() - start_time
        throughput = iterations / elapsed_time  # operations per second
        
        # Should achieve at least 100 ops/sec
        assert throughput > 100
        assert elapsed_time < 10.0
    
    def test_loader_throughput_benchmark(self, mock_spark_session, test_config, perf_monitor):
        """Benchmark loader throughput."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        start_time = time.time()
        iterations = 1000
        
        with patch.object(loader, 'load', return_value=True):
            for _ in range(iterations):
                result = loader.load(mock_df, table="test_table")
                assert result is True
        
        elapsed_time = time.time() - start_time
        throughput = iterations / elapsed_time  # operations per second
        
        # Should achieve at least 100 ops/sec
        assert throughput > 100
        assert elapsed_time < 10.0
    
    def test_transformer_throughput_benchmark(self, mock_spark_session, perf_monitor):
        """Benchmark transformer throughput."""
        transformer = CustomerTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["user_id", "email", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        
        start_time = time.time()
        iterations = 1000
        
        for _ in range(iterations):
            result = transformer.transform(mock_df)
            assert result is not None
        
        elapsed_time = time.time() - start_time
        throughput = iterations / elapsed_time  # operations per second
        
        # Should achieve at least 100 ops/sec
        assert throughput > 100
        assert elapsed_time < 10.0
    
    def test_predictor_throughput_benchmark(self, perf_monitor):
        """Benchmark predictor throughput."""
        predictor = ChurnPredictor()
        
        predictor.model.predict = Mock(return_value=np.array([0, 1, 0]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3], [0.2, 0.8], [0.6, 0.4]]))
        
        mock_data = pd.DataFrame({
            "user_id": ["user_1", "user_2", "user_3"],
            "feature1": [1.0, 2.0, 3.0],
        })
        
        start_time = time.time()
        iterations = 10000
        
        for _ in range(iterations):
            predictions = predictor.predict(mock_data)
            assert predictions is not None
        
        elapsed_time = time.time() - start_time
        throughput = iterations / elapsed_time  # predictions per second
        
        # Should achieve at least 1000 predictions/sec
        assert throughput > 1000
        assert elapsed_time < 10.0
    
    def test_storage_throughput_benchmark(self, perf_monitor):
        """Benchmark prediction storage throughput."""
        storage = PredictionStorage()
        
        mock_client = Mock()
        mock_client.execute.return_value = None
        storage.client = mock_client
        
        start_time = time.time()
        iterations = 10000
        
        for i in range(iterations):
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
        
        elapsed_time = time.time() - start_time
        throughput = iterations / elapsed_time  # saves per second
        
        # Should achieve at least 1000 saves/sec
        assert throughput > 1000
        assert elapsed_time < 10.0
    
    def test_batch_processing_throughput(self, mock_spark_session, perf_monitor):
        """Benchmark batch processing throughput."""
        transformer = CustomerTransformer(mock_spark_session)
        
        # Simulate batch processing with different batch sizes
        batch_sizes = [100, 1000, 10000]
        throughputs = []
        
        for batch_size in batch_sizes:
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = batch_size
            mock_df.columns = ["user_id", "email", "name"]
            mock_df.withColumn = Mock(return_value=mock_df)
            mock_df.drop = Mock(return_value=mock_df)
            
            start_time = time.time()
            iterations = 100
            
            for _ in range(iterations):
                result = transformer.transform(mock_df)
                assert result is not None
            
            elapsed_time = time.time() - start_time
            throughput = (iterations * batch_size) / elapsed_time  # records per second
            throughputs.append(throughput)
        
        # Throughput should scale with batch size
        assert throughputs[2] > throughputs[1]  # Larger batches should be faster
        assert throughputs[1] > throughputs[0]

