"""
Performance Tests for ETL Components
=====================================
"""

import pytest
import time
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame
import numpy as np

from tests.performance.conftest import perf_monitor, performance_config
from src.ownlens.processing.etl.extractors.postgresql_extractor import PostgreSQLExtractor
from src.ownlens.processing.etl.extractors.kafka_extractor import KafkaExtractor
from src.ownlens.processing.etl.extractors.s3_extractor import S3Extractor
from src.ownlens.processing.etl.loaders.postgresql_loader import PostgreSQLLoader
from src.ownlens.processing.etl.loaders.clickhouse_loader import ClickHouseLoader
from src.ownlens.processing.etl.loaders.s3_loader import S3Loader
from src.ownlens.processing.etl.transformers.customer.customer import CustomerTransformer
from src.ownlens.processing.etl.transformers.editorial.editorial import EditorialTransformer
from src.ownlens.processing.etl.orchestration.etl_pipeline import run_etl_pipeline


class TestETLExtractorPerformance:
    """Performance tests for ETL extractors."""
    
    def test_postgresql_extractor_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test PostgreSQL extractor throughput."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["id", "name", "email"]
        
        perf_monitor.start()
        
        # Simulate multiple extractions
        for _ in range(100):
            with patch.object(extractor, 'extract', return_value=mock_df):
                result = extractor.extract(table="test_table")
                assert result is not None
        
        metrics = perf_monitor.stop()
        
        # Assert performance thresholds
        assert metrics["execution_time"] < 10.0  # Should complete in < 10 seconds
        assert metrics["memory_delta"] < 500  # Memory increase < 500 MB
    
    def test_kafka_extractor_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test Kafka extractor throughput."""
        extractor = KafkaExtractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50000
        mock_df.columns = ["event_id", "user_id", "event_type"]
        
        perf_monitor.start()
        
        # Simulate streaming extraction
        for _ in range(50):
            with patch.object(extractor, 'extract', return_value=mock_df):
                result = extractor.extract(topics=["test_topic"])
                assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 15.0
        assert metrics["memory_delta"] < 1000
    
    def test_s3_extractor_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test S3 extractor throughput."""
        extractor = S3Extractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100000
        mock_df.columns = ["id", "data"]
        
        perf_monitor.start()
        
        # Simulate large file extraction
        for _ in range(20):
            with patch.object(extractor, 'extract', return_value=mock_df):
                result = extractor.extract(paths=["s3://bucket/data/"])
                assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 20.0
        assert metrics["memory_delta"] < 2000


class TestETLLoaderPerformance:
    """Performance tests for ETL loaders."""
    
    def test_postgresql_loader_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test PostgreSQL loader throughput."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["id", "name", "email"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        perf_monitor.start()
        
        # Simulate batch loading
        for _ in range(100):
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(mock_df, table="test_table")
                assert result is True
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 10.0
        assert metrics["memory_delta"] < 500
    
    def test_clickhouse_loader_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test ClickHouse loader throughput."""
        loader = ClickHouseLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 50000
        mock_df.columns = ["id", "data", "timestamp"]
        
        perf_monitor.start()
        
        # Simulate high-volume loading
        for _ in range(50):
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(mock_df, table="test_table")
                assert result is True
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 15.0
        assert metrics["memory_delta"] < 1000
    
    def test_s3_loader_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test S3 loader throughput."""
        loader = S3Loader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 100000
        mock_df.columns = ["id", "data"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().mode().save = Mock()
        
        perf_monitor.start()
        
        # Simulate large file writing
        for _ in range(20):
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(mock_df, path="s3://bucket/data/")
                assert result is True
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 20.0
        assert metrics["memory_delta"] < 2000


class TestETLTransformerPerformance:
    """Performance tests for ETL transformers."""
    
    def test_customer_transformer_throughput(self, mock_spark_session, perf_monitor):
        """Test customer transformer throughput."""
        transformer = CustomerTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 10000
        mock_df.columns = ["user_id", "email", "name", "created_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        perf_monitor.start()
        
        # Simulate multiple transformations
        for _ in range(100):
            result = transformer.transform(mock_df)
            assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 10.0
        assert metrics["memory_delta"] < 500
    
    def test_editorial_transformer_throughput(self, mock_spark_session, perf_monitor):
        """Test editorial transformer throughput."""
        transformer = EditorialTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 20000
        mock_df.columns = ["article_id", "title", "content", "author_id"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        mock_df.select = Mock(return_value=mock_df)
        mock_df.filter = Mock(return_value=mock_df)
        
        perf_monitor.start()
        
        # Simulate content processing
        for _ in range(50):
            result = transformer.transform(mock_df)
            assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 15.0
        assert metrics["memory_delta"] < 1000
    
    def test_transformer_memory_efficiency(self, mock_spark_session, perf_monitor):
        """Test transformer memory efficiency with large datasets."""
        transformer = CustomerTransformer(mock_spark_session)
        
        # Simulate large dataset
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000000  # 1M rows
        mock_df.columns = ["user_id", "email", "name", "created_at"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        
        perf_monitor.start()
        
        result = transformer.transform(mock_df)
        
        metrics = perf_monitor.stop()
        
        assert result is not None
        assert metrics["memory_delta"] < 2000  # Should handle 1M rows with < 2GB memory
        assert metrics["execution_time"] < 30.0


class TestETLPipelinePerformance:
    """Performance tests for ETL pipeline orchestration."""
    
    def test_full_pipeline_throughput(self, mock_spark_session, test_config, perf_monitor):
        """Test full ETL pipeline throughput."""
        perf_monitor.start()
        
        with patch('src.ownlens.processing.etl.orchestration.get_spark_session', return_value=mock_spark_session), \
             patch('src.ownlens.processing.etl.orchestration.get_etl_config', return_value=test_config), \
             patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            
            mock_extract.return_value = {
                "test_table": {"success": True, "rows": 10000}
            }
            
            # Simulate multiple pipeline runs
            for _ in range(10):
                result = run_etl_pipeline(source="postgresql")
                assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 20.0
        assert metrics["memory_delta"] < 1000
    
    def test_pipeline_with_large_data(self, mock_spark_session, test_config, perf_monitor):
        """Test pipeline performance with large datasets."""
        perf_monitor.start()
        
        with patch('src.ownlens.processing.etl.orchestration.get_spark_session', return_value=mock_spark_session), \
             patch('src.ownlens.processing.etl.orchestration.get_etl_config', return_value=test_config), \
             patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            
            mock_extract.return_value = {
                "large_table": {"success": True, "rows": 1000000}  # 1M rows
            }
            
            result = run_etl_pipeline(source="postgresql")
            assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 60.0  # Should handle 1M rows in < 60 seconds
        assert metrics["memory_delta"] < 3000  # Memory increase < 3GB
    
    def test_pipeline_concurrent_execution(self, mock_spark_session, test_config, perf_monitor):
        """Test pipeline performance with concurrent execution."""
        import concurrent.futures
        
        perf_monitor.start()
        
        def run_pipeline():
            with patch('src.ownlens.processing.etl.orchestration.get_spark_session', return_value=mock_spark_session), \
                 patch('src.ownlens.processing.etl.orchestration.get_etl_config', return_value=test_config), \
                 patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
                
                mock_extract.return_value = {
                    "test_table": {"success": True, "rows": 10000}
                }
                
                return run_etl_pipeline(source="postgresql")
        
        # Run 5 pipelines concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(run_pipeline) for _ in range(5)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        metrics = perf_monitor.stop()
        
        assert all(r is not None for r in results)
        assert metrics["execution_time"] < 30.0  # Concurrent execution should be faster
        assert metrics["memory_delta"] < 2000


class TestETLStressTests:
    """Stress tests for ETL components."""
    
    def test_extractor_stress_test(self, mock_spark_session, test_config, perf_monitor):
        """Stress test for extractors with high iteration count."""
        extractor = PostgreSQLExtractor(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["id", "name"]
        
        perf_monitor.start()
        
        # High iteration stress test
        for _ in range(1000):
            with patch.object(extractor, 'extract', return_value=mock_df):
                result = extractor.extract(table="test_table")
                assert result is not None
        
        metrics = perf_monitor.stop()
        
        # Stress test should complete without memory leaks
        assert metrics["execution_time"] < 60.0
        assert metrics["memory_delta"] < 1000  # No significant memory leak
    
    def test_loader_stress_test(self, mock_spark_session, test_config, perf_monitor):
        """Stress test for loaders with high iteration count."""
        loader = PostgreSQLLoader(mock_spark_session, test_config)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["id", "name"]
        mock_df.write = Mock()
        mock_df.write.format = Mock(return_value=mock_df.write)
        mock_df.write.format().option = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode = Mock(return_value=mock_df.write)
        mock_df.write.format().option().mode().save = Mock()
        
        perf_monitor.start()
        
        # High iteration stress test
        for _ in range(1000):
            with patch.object(loader, 'load', return_value=True):
                result = loader.load(mock_df, table="test_table")
                assert result is True
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 60.0
        assert metrics["memory_delta"] < 1000  # No memory leak
    
    def test_transformer_stress_test(self, mock_spark_session, perf_monitor):
        """Stress test for transformers with high iteration count."""
        transformer = CustomerTransformer(mock_spark_session)
        
        mock_df = Mock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ["user_id", "email", "name"]
        mock_df.withColumn = Mock(return_value=mock_df)
        mock_df.drop = Mock(return_value=mock_df)
        
        perf_monitor.start()
        
        # High iteration stress test
        for _ in range(1000):
            result = transformer.transform(mock_df)
            assert result is not None
        
        metrics = perf_monitor.stop()
        
        assert metrics["execution_time"] < 60.0
        assert metrics["memory_delta"] < 1000  # No memory leak

