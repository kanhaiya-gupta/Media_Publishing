"""
Security Event Consumer
========================

Kafka consumer for security events.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.audit.security_event import SecurityEvent, SecurityEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class SecurityEventConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for security events.
    
    Consumes security events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "security-event-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize security event consumer."""
        if topics is None:
            topics = ["security-events"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.SecurityEventConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate security event message against Pydantic model."""
        try:
            validated = SecurityEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid security event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating security event message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a security event message."""
        try:
            security_event = SecurityEventCreate.model_validate(message)
            self.logger.info(f"Processing security event: {security_event.event_id}")
        except Exception as e:
            self.logger.error(f"Error processing security event: {e}", exc_info=True)
            raise

