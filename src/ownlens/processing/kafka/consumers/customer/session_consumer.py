"""
Session Consumer
================

Kafka consumer for customer sessions.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.customer.session import Session, SessionCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class SessionConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for customer sessions.
    
    Consumes session events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "session-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize session consumer."""
        if topics is None:
            topics = ["customer-sessions"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.SessionConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate session message against Pydantic model."""
        try:
            validated = SessionCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid session message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating session message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a session message."""
        try:
            session = SessionCreate.model_validate(message)
            self.logger.info(f"Processing session: {session.session_id}")
        except Exception as e:
            self.logger.error(f"Error processing session: {e}", exc_info=True)
            raise

