"""
User Event Extractor
====================

Extract customer user events from PostgreSQL or Kafka.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime

from ...postgresql_extractor import PostgreSQLExtractor
from ...kafka_extractor import KafkaExtractor
from ...utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


class UserEventExtractor(PostgreSQLExtractor):
    """
    Extract customer user events.
    
    Supports extraction from:
    - PostgreSQL (customer_events table)
    - Kafka (customer_events topic)
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize user event extractor."""
        super().__init__(spark, config, extractor_config)
        self.table_name = "customer_events"
        self.kafka_topic = self.config.get("kafka_topic", "customer_events")
    
    def extract_from_postgresql(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        brand_id: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract user events from PostgreSQL.
        
        Args:
            start_date: Start date filter (ISO format)
            end_date: End date filter (ISO format)
            brand_id: Brand ID filter (optional)
            **kwargs: Additional extraction parameters
            
        Returns:
            DataFrame with user events
        """
        query = f"SELECT * FROM {self.table_name} WHERE 1=1"
        
        if start_date:
            query += f" AND event_timestamp >= '{start_date}'"
        if end_date:
            query += f" AND event_timestamp <= '{end_date}'"
        if brand_id:
            query += f" AND brand_id = '{brand_id}'"
        
        query += " ORDER BY event_timestamp"
        
        self.logger.info(f"Extracting user events from PostgreSQL: {query[:100]}...")
        return self.extract(query=query, **kwargs)
    
    def extract_from_kafka(
        self,
        streaming: bool = True,
        starting_offsets: str = "latest",
        **kwargs
    ) -> DataFrame:
        """
        Extract user events from Kafka.
        
        Args:
            streaming: Whether to extract as streaming DataFrame
            starting_offsets: Starting offsets
            **kwargs: Additional Kafka options
            
        Returns:
            DataFrame with user events
        """
        kafka_extractor = KafkaExtractor(self.spark, self.etl_config)
        
        if streaming:
            df = kafka_extractor.extract_stream(
                topics=self.kafka_topic,
                starting_offsets=starting_offsets,
                **kwargs
            )
        else:
            df = kafka_extractor.extract_batch(
                topics=self.kafka_topic,
                starting_offsets=starting_offsets,
                **kwargs
            )
        
        return df
    
    def extract(self, source: str = "postgresql", **kwargs) -> DataFrame:
        """
        Extract user events from specified source.
        
        Args:
            source: Source type ("postgresql" or "kafka")
            **kwargs: Source-specific parameters
            
        Returns:
            DataFrame with user events
        """
        if source == "kafka":
            return self.extract_from_kafka(**kwargs)
        else:
            return self.extract_from_postgresql(**kwargs)

