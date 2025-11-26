"""
Kafka Extractor
===============

Extract data from Kafka topics (for streaming ETL).
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamReader

from ..base.extractor import BaseExtractor
from ..utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


# Comprehensive list of Kafka topics (matching actual producer topics)
# These match the topic names used by the producers in src/ownlens/processing/kafka/
KAFKA_TOPICS = [
    "customer-user-events",      # user_events.py producer
    "customer-sessions",          # session_events.py producer (NOT customer-session-events)
    "editorial-content-events",   # content_events.py producer
    "security-events",            # security_events.py producer (NOT security-security-events)
    "compliance-events",          # compliance_event_producer.py (NOT compliance-compliance-events)
]


class KafkaExtractor(BaseExtractor):
    """
    Extract data from Kafka topics.
    
    Supports:
    - Streaming extraction (for Spark Streaming)
    - Batch extraction (for Spark Batch)
    - Multiple topics
    - Schema inference
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka extractor.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            extractor_config: Extractor-specific configuration
        """
        super().__init__(spark, extractor_config)
        self.etl_config = config or get_etl_config()
        self.kafka_config = self.etl_config.get_kafka_config()
    
    def extract_stream(
        self,
        topics: str,
        starting_offsets: str = "latest",
        max_offsets_per_trigger: Optional[int] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract data from Kafka topics as streaming DataFrame.
        
        Args:
            topics: Comma-separated topic names or topic pattern
            starting_offsets: Starting offsets ("earliest", "latest", or JSON string)
            max_offsets_per_trigger: Maximum offsets per trigger (optional)
            **kwargs: Additional Kafka options
            
        Returns:
            Streaming DataFrame
        """
        try:
            kafka_options = {
                "kafka.bootstrap.servers": self.kafka_config["kafka.bootstrap.servers"],
                "subscribe": topics,
                "startingOffsets": starting_offsets,
                **kwargs
            }
            
            if max_offsets_per_trigger:
                kafka_options["maxOffsetsPerTrigger"] = str(max_offsets_per_trigger)
            
            self.logger.info(f"Extracting streaming data from Kafka topics: {topics}")
            
            df = (
                self.spark
                .readStream
                .format("kafka")
                .options(**kafka_options)
                .load()
            )
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting streaming data from Kafka: {e}", exc_info=True)
            raise
    
    def extract_batch(
        self,
        topics: str,
        starting_offsets: str = "earliest",
        ending_offsets: str = "latest",
        **kwargs
    ) -> DataFrame:
        """
        Extract data from Kafka topics as batch DataFrame.
        
        Args:
            topics: Comma-separated topic names
            starting_offsets: Starting offsets ("earliest", "latest", or JSON string)
            ending_offsets: Ending offsets ("earliest", "latest", or JSON string)
            **kwargs: Additional Kafka options
            
        Returns:
            Batch DataFrame
        """
        try:
            kafka_options = {
                "kafka.bootstrap.servers": self.kafka_config["kafka.bootstrap.servers"],
                "subscribe": topics,
                "startingOffsets": starting_offsets,
                "endingOffsets": ending_offsets,
                **kwargs
            }
            
            self.logger.info(f"Extracting batch data from Kafka topics: {topics}")
            
            df = (
                self.spark
                .read
                .format("kafka")
                .options(**kafka_options)
                .load()
            )
            
            self.log_extraction_stats(df, f"Kafka: {topics}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting batch data from Kafka: {e}", exc_info=True)
            raise
    
    def extract(
        self,
        topics: str,
        streaming: bool = True,
        **kwargs
    ) -> DataFrame:
        """
        Extract data from Kafka (convenience method).
        
        Args:
            topics: Comma-separated topic names
            streaming: Whether to extract as streaming DataFrame (default: True)
            **kwargs: Additional Kafka options
            
        Returns:
            Spark DataFrame (streaming or batch)
        """
        if streaming:
            return self.extract_stream(topics, **kwargs)
        else:
            return self.extract_batch(topics, **kwargs)
    
    @staticmethod
    def parse_kafka_value(df: DataFrame, value_column: str = "value") -> DataFrame:
        """
        Parse Kafka value column (assumes JSON format).
        
        Args:
            df: DataFrame with Kafka data
            value_column: Name of value column (default: "value")
            
        Returns:
            DataFrame with parsed JSON values
        """
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StringType
        
        # Parse JSON from value column
        # Note: Schema should be provided for proper parsing
        # This is a basic implementation - should be extended with actual schema
        return df.select(
            col("key").cast(StringType()).alias("kafka_key"),
            col("value").cast(StringType()).alias("kafka_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        )

