"""
User Event Consumer
===================

Kafka consumer for customer user events.
"""

from typing import Optional, Dict, Any, List, Callable
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.customer.user_event import UserEvent, UserEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class UserEventConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for customer user events.
    
    Consumes user events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "user-event-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize user event consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topics: List of topics to consume from (defaults to ["customer-user-events"])
            group_id: Consumer group ID
            config: Additional consumer configuration
        """
        if topics is None:
            topics = ["customer-user-events"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.UserEventConsumer")
    
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
            validated = UserEventCreate.model_validate(message)
            
            # Convert to dict
            return validated.model_dump()
            
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid user event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating user event message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a user event message.
        
        Args:
            message: User event message to process
        """
        try:
            # Convert to Pydantic model
            user_event = UserEventCreate.model_validate(message)
            
            # Process the event
            self.logger.info(f"Processing user event: {user_event.event_id}")
            
            # Override in subclasses for specific processing logic
            # Example: Save to database, trigger workflows, etc.
            
        except Exception as e:
            self.logger.error(f"Error processing user event: {e}", exc_info=True)
            raise

