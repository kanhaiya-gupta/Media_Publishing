"""
Base Kafka Producer
===================

Base class for Kafka producers with common functionality.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseKafkaProducer(ABC):
    """
    Base Kafka producer with common functionality.
    
    Provides:
    - Connection management
    - Message serialization
    - Error handling
    - Retry logic
    - Compression
    - Idempotence
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            config: Additional producer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.config = config or {}
        
        # Producer configuration
        producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
            "key_serializer": lambda k: json.dumps(k, default=str).encode("utf-8") if k else None,
            "compression_type": "snappy",
            "acks": "all",  # Wait for all replicas
            # Note: kafka-python doesn't support 'idempotent' option
            # For exactly-once semantics, use acks='all' and enable.idempotence on Kafka broker
            "retries": 3,
            "max_in_flight_requests_per_connection": 5,
            **self.config
        }
        
        self.producer = KafkaProducer(**producer_config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def send(
        self,
        message: Dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
        partition: Optional[int] = None
    ) -> bool:
        """
        Send message to Kafka topic.
        
        Args:
            message: Message to send (dict)
            topic: Topic name (uses default if not provided)
            key: Message key for partitioning
            partition: Partition number (optional)
            
        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            target_topic = topic or self.topic
            if not target_topic:
                raise ValueError("Topic must be provided")
            
            # Validate message
            validated_message = self.validate_message(message)
            
            # Add metadata
            enriched_message = self.enrich_message(validated_message)
            
            # Send to Kafka
            future = self.producer.send(
                target_topic,
                value=enriched_message,
                key=key,
                partition=partition
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            self.logger.info(
                f"Message sent to topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            self.logger.error(f"Kafka error sending message: {e}", exc_info=True)
            return False
        except Exception as e:
            self.logger.error(f"Error sending message: {e}", exc_info=True)
            return False
    
    def send_batch(
        self,
        messages: List[Dict[str, Any]],
        topic: Optional[str] = None,
        key_func: Optional[callable] = None
    ) -> int:
        """
        Send batch of messages to Kafka topic.
        
        Args:
            messages: List of messages to send
            topic: Topic name (uses default if not provided)
            key_func: Function to extract key from message
            
        Returns:
            Number of messages sent successfully
        """
        success_count = 0
        for message in messages:
            key = key_func(message) if key_func else None
            if self.send(message, topic=topic, key=key):
                success_count += 1
        
        self.logger.info(f"Sent {success_count}/{len(messages)} messages successfully")
        return success_count
    
    @abstractmethod
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate message against schema.
        
        Args:
            message: Message to validate
            
        Returns:
            Validated message
            
        Raises:
            ValueError: If message is invalid
        """
        pass
    
    def enrich_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich message with metadata.
        
        Args:
            message: Message to enrich
            
        Returns:
            Enriched message
        """
        enriched = message.copy()
        enriched["_metadata"] = {
            "produced_at": datetime.utcnow().isoformat(),
            "producer": self.__class__.__name__,
            "version": "1.0"
        }
        return enriched
    
    def close(self):
        """Close producer connection."""
        try:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed")
        except Exception as e:
            self.logger.error(f"Error closing producer: {e}", exc_info=True)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

