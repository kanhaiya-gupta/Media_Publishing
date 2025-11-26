"""
Content Event Producer
======================

Kafka producer for editorial content events.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.editorial.content_event import ContentEvent, ContentEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ContentEventProducer(BaseKafkaProducer):
    """
    Kafka producer for editorial content events.
    
    Publishes content events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "editorial-content-events",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize content event producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.ContentEventProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate content event message against Pydantic model."""
        try:
            if isinstance(message, ContentEventCreate):
                validated = message
            else:
                validated = ContentEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid content event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating content event message: {str(e)}")
    
    def publish_content_event(
        self,
        content_event: ContentEventCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish content event to Kafka topic."""
        try:
            if key is None:
                key = str(content_event.article_id)
            return self.send(message=content_event.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing content event: {e}", exc_info=True)
            return False

