"""
Security Event Producer
========================

Kafka producer for security events.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.audit.security_event import SecurityEvent, SecurityEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class SecurityEventProducer(BaseKafkaProducer):
    """
    Kafka producer for security events.
    
    Publishes security events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "security-events",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize security event producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.SecurityEventProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate security event message against Pydantic model."""
        try:
            if isinstance(message, SecurityEventCreate):
                validated = message
            else:
                validated = SecurityEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid security event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating security event message: {str(e)}")
    
    def publish_security_event(
        self,
        security_event: SecurityEventCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish security event to Kafka topic."""
        try:
            if key is None:
                key = str(security_event.event_id)
            return self.send(message=security_event.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing security event: {e}", exc_info=True)
            return False

