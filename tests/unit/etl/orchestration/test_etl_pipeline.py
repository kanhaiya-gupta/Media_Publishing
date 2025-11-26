"""
Unit Tests for ETL Pipeline Orchestration
==========================================
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame

from src.ownlens.processing.etl.orchestration import (
    run_etl_pipeline,
    extract_postgresql_tables,
    extract_kafka_topics,
    extract_s3_paths,
)


class TestETLPipeline:
    """Test run_etl_pipeline function."""

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_postgresql(self, mock_config, mock_spark):
        """Test ETL pipeline with PostgreSQL source."""
        # Mock Spark session
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        # Mock config
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        # Mock extract_postgresql_tables
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            mock_extract.return_value = {
                "countries": {"success": True, "rows": 10},
                "cities": {"success": True, "rows": 20}
            }
            
            results = run_etl_pipeline(source="postgresql")
            
            assert results is not None
            assert "postgresql" in results

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_all_sources(self, mock_config, mock_spark):
        """Test ETL pipeline with all sources."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_pg, \
             patch('src.ownlens.processing.etl.orchestration.extract_kafka_topics') as mock_kafka, \
             patch('src.ownlens.processing.etl.orchestration.extract_s3_paths') as mock_s3:
            
            mock_pg.return_value = {"countries": {"success": True}}
            mock_kafka.return_value = {"customer-user-events": {"success": True}}
            mock_s3.return_value = {"data/": {"success": True}}
            
            results = run_etl_pipeline(source=None)
            
            assert results is not None

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_load(self, mock_config, mock_spark):
        """Test ETL pipeline with loading enabled."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            mock_extract.return_value = {
                "countries": {
                    "success": True,
                    "rows": 10,
                    "loaded": True,
                    "load_results": {"postgresql": {"success": True, "rows_loaded": 10}}
                }
            }
            
            results = run_etl_pipeline(
                source="postgresql",
                load=True,
                load_destinations=["postgresql"]
            )
            
            assert results is not None


class TestExtractPostgreSQLTables:
    """Test extract_postgresql_tables function."""

    def test_extract_postgresql_tables_success(self, mock_spark_session, test_config):
        """Test successful PostgreSQL table extraction."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["countries"]
            )
            
            assert results is not None
            assert "countries" in results

    def test_extract_postgresql_tables_empty(self, mock_spark_session, test_config):
        """Test extraction with empty table."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 0
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["empty_table"]
            )
            
            assert results is not None
            assert "empty_table" in results

    def test_extract_postgresql_tables_with_dependency_sorting(self, mock_spark_session, test_config):
        """Test extraction with dependency sorting."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class, \
             patch('src.ownlens.processing.etl.orchestration.sort_tables_by_dependency') as mock_sort:
            
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            mock_sort.return_value = ["countries", "cities"]
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["cities", "countries"],
                load=True,
                load_destinations=["postgresql"]
            )
            
            assert results is not None
            mock_sort.assert_called_once()


class TestExtractKafkaTopics:
    """Test extract_kafka_topics function."""

    def test_extract_kafka_topics_success(self, mock_spark_session, test_config):
        """Test successful Kafka topic extraction."""
        with patch('src.ownlens.processing.etl.orchestration.KafkaExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 100
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_kafka_topics(
                mock_spark_session,
                test_config,
                topics=["customer-user-events"]
            )
            
            assert results is not None


class TestExtractS3Paths:
    """Test extract_s3_paths function."""

    def test_extract_s3_paths_success(self, mock_spark_session, test_config):
        """Test successful S3 path extraction."""
        with patch('src.ownlens.processing.etl.orchestration.S3Extractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 50
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_s3_paths(
                mock_spark_session,
                test_config,
                paths=["data/"]
            )
            
            assert results is not None

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_transformation(self, mock_config, mock_spark):
        """Test ETL pipeline with transformation."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract, \
             patch('src.ownlens.processing.etl.orchestration.transform_data') as mock_transform:
            
            mock_extract.return_value = {
                "countries": {
                    "success": True,
                    "rows": 10,
                    "dataframe": Mock(spec=DataFrame)
                }
            }
            mock_transform.return_value = Mock(spec=DataFrame)
            
            results = run_etl_pipeline(
                source="postgresql",
                transform=True
            )
            
            assert results is not None
            mock_transform.assert_called()

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_error_handling(self, mock_config, mock_spark):
        """Test ETL pipeline error handling."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            mock_extract.side_effect = Exception("Extraction error")
            
            results = run_etl_pipeline(source="postgresql")
            
            # Should handle error gracefully
            assert results is not None

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_partial_failure(self, mock_config, mock_spark):
        """Test ETL pipeline with partial failure."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            mock_extract.return_value = {
                "countries": {"success": True, "rows": 10},
                "cities": {"success": False, "error": "Connection error"}
            }
            
            results = run_etl_pipeline(source="postgresql")
            
            assert results is not None
            assert "countries" in results
            assert "cities" in results

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_incremental_mode(self, mock_config, mock_spark):
        """Test ETL pipeline with incremental mode."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            mock_extract.return_value = {
                "customer_events": {"success": True, "rows": 100}
            }
            
            results = run_etl_pipeline(
                source="postgresql",
                incremental=True,
                last_timestamp="2024-01-01T00:00:00"
            )
            
            assert results is not None

    def test_extract_postgresql_tables_with_transformation(self, mock_spark_session, test_config):
        """Test PostgreSQL extraction with transformation."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class, \
             patch('src.ownlens.processing.etl.orchestration.CustomerTransformer') as mock_transformer_class:
            
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            mock_transformer = Mock()
            mock_transformed_df = Mock(spec=DataFrame)
            mock_transformed_df.count.return_value = 10
            mock_transformer.transform.return_value = mock_transformed_df
            mock_transformer_class.return_value = mock_transformer
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["customer_events"],
                transform=True,
                transformer="customer"
            )
            
            assert results is not None
            assert "customer_events" in results

    def test_extract_postgresql_tables_with_multiple_loaders(self, mock_spark_session, test_config):
        """Test PostgreSQL extraction with multiple loaders."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class, \
             patch('src.ownlens.processing.etl.orchestration.PostgreSQLLoader') as mock_pg_loader_class, \
             patch('src.ownlens.processing.etl.orchestration.ClickHouseLoader') as mock_ch_loader_class:
            
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            mock_pg_loader = Mock()
            mock_pg_loader.load.return_value = True
            mock_pg_loader_class.return_value = mock_pg_loader
            
            mock_ch_loader = Mock()
            mock_ch_loader.load.return_value = True
            mock_ch_loader_class.return_value = mock_ch_loader
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["countries"],
                load=True,
                load_destinations=["postgresql", "clickhouse"]
            )
            
            assert results is not None
            assert "countries" in results

    def test_extract_kafka_topics_with_streaming(self, mock_spark_session, test_config):
        """Test Kafka extraction with streaming mode."""
        with patch('src.ownlens.processing.etl.orchestration.KafkaExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 100
            mock_extractor.extract_stream.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_kafka_topics(
                mock_spark_session,
                test_config,
                topics=["customer-user-events"],
                streaming=True
            )
            
            assert results is not None

    def test_extract_kafka_topics_with_batch_mode(self, mock_spark_session, test_config):
        """Test Kafka extraction with batch mode."""
        with patch('src.ownlens.processing.etl.orchestration.KafkaExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 100
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_kafka_topics(
                mock_spark_session,
                test_config,
                topics=["customer-user-events"],
                streaming=False,
                starting_offsets="earliest",
                ending_offsets="latest"
            )
            
            assert results is not None

    def test_extract_s3_paths_with_multiple_formats(self, mock_spark_session, test_config):
        """Test S3 extraction with multiple formats."""
        with patch('src.ownlens.processing.etl.orchestration.S3Extractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 50
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_s3_paths(
                mock_spark_session,
                test_config,
                paths=["data/parquet/", "data/delta/", "data/json/"],
                formats=["parquet", "delta", "json"]
            )
            
            assert results is not None

    def test_extract_postgresql_tables_error_handling(self, mock_spark_session, test_config):
        """Test PostgreSQL extraction error handling."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract.side_effect = Exception("Connection error")
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["countries"]
            )
            
            assert results is not None
            assert "countries" in results
            assert results["countries"].get("success", False) is False

    def test_extract_kafka_topics_error_handling(self, mock_spark_session, test_config):
        """Test Kafka extraction error handling."""
        with patch('src.ownlens.processing.etl.orchestration.KafkaExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract.side_effect = Exception("Kafka error")
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_kafka_topics(
                mock_spark_session,
                test_config,
                topics=["customer-user-events"]
            )
            
            assert results is not None

    def test_extract_s3_paths_error_handling(self, mock_spark_session, test_config):
        """Test S3 extraction error handling."""
        with patch('src.ownlens.processing.etl.orchestration.S3Extractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_extractor.extract.side_effect = Exception("S3 error")
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_s3_paths(
                mock_spark_session,
                test_config,
                paths=["data/"]
            )
            
            assert results is not None

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_parallel_execution(self, mock_config, mock_spark):
        """Test ETL pipeline with parallel execution."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_pg, \
             patch('src.ownlens.processing.etl.orchestration.extract_kafka_topics') as mock_kafka, \
             patch('src.ownlens.processing.etl.orchestration.extract_s3_paths') as mock_s3:
            
            mock_pg.return_value = {"countries": {"success": True}}
            mock_kafka.return_value = {"customer-user-events": {"success": True}}
            mock_s3.return_value = {"data/": {"success": True}}
            
            results = run_etl_pipeline(
                source=None,
                parallel=True,
                max_workers=3
            )
            
            assert results is not None

    @patch('src.ownlens.processing.etl.orchestration.get_spark_session')
    @patch('src.ownlens.processing.etl.orchestration.get_etl_config')
    def test_run_etl_pipeline_with_retry(self, mock_config, mock_spark):
        """Test ETL pipeline with retry logic."""
        mock_spark_session = Mock()
        mock_spark.return_value = mock_spark_session
        
        mock_config_instance = Mock()
        mock_config.return_value = mock_config_instance
        
        with patch('src.ownlens.processing.etl.orchestration.extract_postgresql_tables') as mock_extract:
            # First call fails, second succeeds
            mock_extract.side_effect = [
                Exception("Temporary error"),
                {"countries": {"success": True, "rows": 10}}
            ]
            
            results = run_etl_pipeline(
                source="postgresql",
                retry=True,
                max_retries=3
            )
            
            assert results is not None
            assert mock_extract.call_count == 2

    def test_extract_postgresql_tables_with_schema_filtering(self, mock_spark_session, test_config):
        """Test PostgreSQL extraction with schema filtering."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 10
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["countries"],
                schema="public"
            )
            
            assert results is not None
            assert "countries" in results

    def test_extract_postgresql_tables_with_where_clause(self, mock_spark_session, test_config):
        """Test PostgreSQL extraction with WHERE clause."""
        with patch('src.ownlens.processing.etl.orchestration.PostgreSQLExtractor') as mock_extractor_class:
            mock_extractor = Mock()
            mock_df = Mock(spec=DataFrame)
            mock_df.count.return_value = 5
            mock_extractor.extract.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            results = extract_postgresql_tables(
                mock_spark_session,
                test_config,
                tables=["countries"],
                where="id > 10"
            )
            
            assert results is not None
            assert "countries" in results

