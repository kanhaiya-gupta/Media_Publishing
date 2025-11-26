"""
Internal Content Producer
=========================

Kafka producer for company internal content events.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.company.internal_content import InternalContent, InternalContentCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class InternalContentProducer(BaseKafkaProducer):
    """
    Kafka producer for company internal content events.
    
    Publishes internal content events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "company-internal-content-events",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize internal content producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.InternalContentProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate internal content message against Pydantic model."""
        try:
            if isinstance(message, InternalContentCreate):
                validated = message
            else:
                validated = InternalContentCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid internal content message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating internal content message: {str(e)}")
    
    def publish_internal_content(
        self,
        content: InternalContentCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish internal content to Kafka topic."""
        try:
            if key is None:
                key = str(content.content_id)
            return self.send(message=content.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing internal content: {e}", exc_info=True)
            return False

