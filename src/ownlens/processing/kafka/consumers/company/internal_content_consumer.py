"""
Internal Content Consumer
=========================

Kafka consumer for company internal content events.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.company.internal_content import InternalContent, InternalContentCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class InternalContentConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for company internal content events.
    
    Consumes internal content events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "internal-content-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize internal content consumer."""
        if topics is None:
            topics = ["company-internal-content-events"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.InternalContentConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate internal content message against Pydantic model."""
        try:
            validated = InternalContentCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid internal content message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating internal content message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process an internal content message."""
        try:
            content = InternalContentCreate.model_validate(message)
            self.logger.info(f"Processing internal content: {content.content_id}")
        except Exception as e:
            self.logger.error(f"Error processing internal content: {e}", exc_info=True)
            raise

