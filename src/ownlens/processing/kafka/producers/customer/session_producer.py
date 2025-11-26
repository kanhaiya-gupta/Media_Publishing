"""
Session Producer
================

Kafka producer for customer sessions.
"""

from typing import Optional, Dict, Any
from kafka.errors import KafkaError
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.customer.session import Session, SessionCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class SessionProducer(BaseKafkaProducer):
    """
    Kafka producer for customer sessions.
    
    Publishes session events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "customer-sessions",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize session producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            config: Additional producer configuration
        """
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.SessionProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate session message against Pydantic model.
        
        Args:
            message: Message to validate
            
        Returns:
            Validated message as dict
            
        Raises:
            ValueError: If message is invalid
        """
        try:
            # Validate using Pydantic model
            if isinstance(message, SessionCreate):
                validated = message
            else:
                validated = SessionCreate.model_validate(message)
            
            # Convert to dict
            return validated.model_dump()
            
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid session message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating session message: {str(e)}")
    
    def publish_session(
        self,
        session: SessionCreate,
        key: Optional[str] = None
    ) -> bool:
        """
        Publish session to Kafka topic.
        
        Args:
            session: Session to publish
            key: Message key for partitioning (defaults to session_id)
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Use session_id as key if not provided
            if key is None:
                key = str(session.session_id)
            
            # Send to Kafka
            return self.send(
                message=session.model_dump(),
                key=key
            )
            
        except Exception as e:
            self.logger.error(f"Error publishing session: {e}", exc_info=True)
            return False

