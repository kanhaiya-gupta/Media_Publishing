"""
Compliance Event Consumer
==========================

Kafka consumer for compliance events.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.audit.compliance_event import ComplianceEvent, ComplianceEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ComplianceEventConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for compliance events.
    
    Consumes compliance events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "compliance-event-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize compliance event consumer."""
        if topics is None:
            topics = ["compliance-events"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.ComplianceEventConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate compliance event message against Pydantic model."""
        try:
            validated = ComplianceEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid compliance event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating compliance event message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a compliance event message."""
        try:
            compliance_event = ComplianceEventCreate.model_validate(message)
            self.logger.info(f"Processing compliance event: {compliance_event.event_id}")
        except Exception as e:
            self.logger.error(f"Error processing compliance event: {e}", exc_info=True)
            raise

