"""
Content Event Consumer
======================

Kafka consumer for editorial content events.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.editorial.content_event import ContentEvent, ContentEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ContentEventConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for editorial content events.
    
    Consumes content events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "content-event-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize content event consumer."""
        if topics is None:
            topics = ["editorial-content-events"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.ContentEventConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate content event message against Pydantic model."""
        try:
            validated = ContentEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid content event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating content event message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a content event message."""
        try:
            content_event = ContentEventCreate.model_validate(message)
            self.logger.info(f"Processing content event: {content_event.event_id}")
        except Exception as e:
            self.logger.error(f"Error processing content event: {e}", exc_info=True)
            raise

