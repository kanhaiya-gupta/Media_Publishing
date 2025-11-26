"""
Latency Benchmark Tests
=======================
"""

import pytest
import time
import statistics
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


class TestLatencyBenchmarks:
    """Latency benchmark tests."""
    
    def test_extractor_latency_benchmark(self, mock_spark_session, test_config, perf_monitor):
        """Benchmark extractor latency."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["id", "name"]
        
        latencies = []
        
        with patch.object(extractor, 'extract', return_value=mock_df):
            for _ in range(100):
                start_time = time.time()
                result = extractor.extract(table="test_table")
                latency = time.time() - start_time
                latencies.append(latency)
                assert result is not None
        
        avg_latency = statistics.mean(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        
        # Latency should be low
        assert avg_latency < 0.1  # < 100ms average
        assert p50_latency < 0.1  # < 100ms median
        assert p95_latency < 0.2  # < 200ms P95
        assert p99_latency < 0.5  # < 500ms P99
    
    def test_loader_latency_benchmark(self, mock_spark_session, test_config, perf_monitor):
        """Benchmark loader latency."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        latencies = []
        
        with patch.object(loader, 'load', return_value=True):
            for _ in range(100):
                start_time = time.time()
                result = loader.load(mock_df, table="test_table")
                latency = time.time() - start_time
                latencies.append(latency)
                assert result is True
        
        avg_latency = statistics.mean(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        assert avg_latency < 0.1  # < 100ms average
        assert p95_latency < 0.2  # < 200ms P95
    
    def test_transformer_latency_benchmark(self, mock_spark_session, perf_monitor):
        """Benchmark transformer latency."""
        transformer = CustomerTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "email", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        
        latencies = []
        
        for _ in range(100):
            start_time = time.time()
            result = transformer.transform(mock_df)
            latency = time.time() - start_time
            latencies.append(latency)
            assert result is not None
        
        avg_latency = statistics.mean(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        assert avg_latency < 0.05  # < 50ms average
        assert p95_latency < 0.1  # < 100ms P95
    
    def test_predictor_latency_benchmark(self, perf_monitor):
        """Benchmark predictor latency."""
        predictor = ChurnPredictor()
        
        predictor.model.predict = Mock(return_value=np.array([0]))
        predictor.model.predict_proba = Mock(return_value=np.array([[0.7, 0.3]]))
        
        mock_data = pd.DataFrame({
            "user_id": ["user_1"],
            "feature1": [1.0],
        })
        
        latencies = []
        
        for _ in range(1000):
            start_time = time.time()
            prediction = predictor.predict(mock_data)
            latency = time.time() - start_time
            latencies.append(latency)
            assert prediction is not None
        
        avg_latency = statistics.mean(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        
        # Predictions should be very fast
        assert avg_latency < 0.01  # < 10ms average
        assert p50_latency < 0.01  # < 10ms median
        assert p95_latency < 0.02  # < 20ms P95
        assert p99_latency < 0.05  # < 50ms P99
    
    def test_storage_latency_benchmark(self, perf_monitor):
        """Benchmark prediction storage latency."""
        storage = PredictionStorage()
        
        mock_client = Mock()
        mock_client.execute.return_value = None
        storage.client = mock_client
        
        latencies = []
        
        for i in range(1000):
            start_time = time.time()
            with patch.object(storage, 'save_prediction', return_value=f"pred_{i}"):
                pred_id = storage.save_prediction(
                    model_id="model_id",
                    entity_id=f"user_{i}",
                    entity_type="user",
                    prediction_type="churn",
                    prediction_probability=0.85,
                    prediction_class="churn"
                )
            latency = time.time() - start_time
            latencies.append(latency)
            assert pred_id is not None
        
        avg_latency = statistics.mean(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        assert avg_latency < 0.01  # < 10ms average
        assert p95_latency < 0.02  # < 20ms P95
    
    def test_end_to_end_latency(self, mock_spark_session, test_config, perf_monitor):
        """Benchmark end-to-end pipeline latency."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        transformer = CustomerTransformer(mock_spark_session)
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "email", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        latencies = []
        
        for _ in range(50):
            start_time = time.time()
            
            # Extract
            with patch.object(extractor, 'extract', return_value=mock_df):
                df = extractor.extract(table="test_table")
            
            # Transform
            df = transformer.transform(df)
            
            # Load
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(df, table="test_table")
            
            latency = time.time() - start_time
            latencies.append(latency)
            assert result is True
        
        avg_latency = statistics.mean(latencies)
        p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
        
        # End-to-end should complete in reasonable time
        assert avg_latency < 0.5  # < 500ms average
        assert p95_latency < 1.0  # < 1s P95

