"""
Kafka Loader
============

Load data to Kafka topics.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from ..base.loader import BaseLoader
from ..utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


class KafkaLoader(BaseLoader):
    """
    Load data to Kafka topics.
    
    Supports:
    - Streaming writes
    - Batch writes
    - JSON serialization
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka loader.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            loader_config: Loader-specific configuration
        """
        super().__init__(spark, loader_config)
        self.etl_config = config or get_etl_config()
        self.kafka_config = self.etl_config.get_kafka_config()
    
    def load(
        self,
        df: DataFrame,
        destination: str,
        streaming: bool = False,
        checkpoint_location: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Load data to Kafka topic.
        
        Args:
            df: DataFrame to load
            destination: Kafka topic name
            streaming: Whether to write as streaming (default: False)
            checkpoint_location: Checkpoint location for streaming (optional)
            **kwargs: Additional Kafka options
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            if not self.validate_input(df):
                return False
            
            self.log_load_stats(df, f"Kafka: {destination}")
            
            kafka_options = {
                "kafka.bootstrap.servers": self.kafka_config["kafka.bootstrap.servers"],
                "topic": destination,
                **kwargs
            }
            
            if streaming:
                writer = (
                    df.writeStream
                    .format("kafka")
                    .options(**kafka_options)
                )
                
                if checkpoint_location:
                    writer = writer.option("checkpointLocation", checkpoint_location)
                
                query = writer.start()
                query.awaitTermination()
            else:
                df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format("kafka").options(**kafka_options).save()
            
            self.logger.info(f"Successfully loaded data to Kafka topic: {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to Kafka: {e}", exc_info=True)
            return False

