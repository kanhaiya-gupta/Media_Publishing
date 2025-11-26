"""
Base Kafka Consumer
===================

Base class for Kafka consumers with common functionality.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseKafkaConsumer(ABC):
    """
    Base Kafka consumer with common functionality.
    
    Provides:
    - Connection management
    - Message deserialization
    - Error handling
    - Offset management
    - Consumer groups
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topics: List of topics to consume from
            group_id: Consumer group ID
            config: Additional consumer configuration
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or []
        self.group_id = group_id
        self.config = config or {}
        
        # Consumer configuration
        consumer_config = {
            "bootstrap_servers": bootstrap_servers,
            "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
            "key_deserializer": lambda k: json.loads(k.decode("utf-8")) if k else None,
            "auto_offset_reset": "latest",
            "enable_auto_commit": True,
            "auto_commit_interval_ms": 1000,
            "group_id": group_id,
            **self.config
        }
        
        self.consumer = KafkaConsumer(*self.topics, **consumer_config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def consume(
        self,
        message_handler: Callable[[Dict[str, Any]], None],
        timeout_ms: int = 1000,
        max_messages: Optional[int] = None
    ):
        """
        Consume messages from Kafka topics.
        
        Args:
            message_handler: Function to handle each message
            timeout_ms: Timeout in milliseconds
            max_messages: Maximum number of messages to consume (None for unlimited)
        """
        try:
            message_count = 0
            for message in self.consumer:
                try:
                    # Extract message data
                    message_data = message.value
                    
                    # Validate message
                    validated_message = self.validate_message(message_data)
                    
                    # Process message
                    message_handler(validated_message)
                    
                    message_count += 1
                    if max_messages and message_count >= max_messages:
                        break
                        
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)
                    # Continue processing other messages
                    continue
                    
        except KafkaError as e:
            self.logger.error(f"Kafka error consuming messages: {e}", exc_info=True)
            raise
        except Exception as e:
            self.logger.error(f"Error consuming messages: {e}", exc_info=True)
            raise
    
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
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a single message.
        
        Args:
            message: Message to process
        """
        # Override in subclasses for specific processing logic
        self.logger.info(f"Processing message: {message}")
    
    def close(self):
        """Close consumer connection."""
        try:
            self.consumer.close()
            self.logger.info("Kafka consumer closed")
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}", exc_info=True)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

