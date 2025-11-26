"""
User Event Producer
===================

Kafka producer for customer user events.
"""

from typing import Optional, Dict, Any
from kafka.errors import KafkaError
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.customer.user_event import UserEvent, UserEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class UserEventProducer(BaseKafkaProducer):
    """
    Kafka producer for customer user events.
    
    Publishes user events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "customer-user-events",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize user event producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            config: Additional producer configuration
        """
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.UserEventProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate user event message against Pydantic model.
        
        Args:
            message: Message to validate
            
        Returns:
            Validated message as dict
            
        Raises:
            ValueError: If message is invalid
        """
        try:
            # Validate using Pydantic model
            if isinstance(message, UserEventCreate):
                validated = message
            else:
                validated = UserEventCreate.model_validate(message)
            
            # Convert to dict
            return validated.model_dump()
            
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid user event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating user event message: {str(e)}")
    
    def publish_user_event(
        self,
        user_event: UserEventCreate,
        key: Optional[str] = None
    ) -> bool:
        """
        Publish user event to Kafka topic.
        
        Args:
            user_event: User event to publish
            key: Message key for partitioning (defaults to user_id)
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Use user_id as key if not provided
            if key is None:
                key = str(user_event.user_id)
            
            # Send to Kafka
            return self.send(
                message=user_event.model_dump(),
                key=key
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing user event: {e}", exc_info=True)
            return False
    
    def publish_batch(
        self,
        user_events: list[UserEventCreate],
        key_func: Optional[callable] = None
    ) -> int:
        """
        Publish batch of user events to Kafka topic.
        
        Args:
            user_events: List of user events to publish
            key_func: Function to extract key from event (defaults to user_id)
            
        Returns:
            Number of events published successfully
        """
        if key_func is None:
            key_func = lambda event: str(event.user_id)
        
        messages = [event.model_dump() for event in user_events]
        return self.send_batch(messages, key_func=key_func)

