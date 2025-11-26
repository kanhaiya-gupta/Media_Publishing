"""
Compliance Event Producer
==========================

Kafka producer for compliance events.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.audit.compliance_event import ComplianceEvent, ComplianceEventCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ComplianceEventProducer(BaseKafkaProducer):
    """
    Kafka producer for compliance events.
    
    Publishes compliance events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "compliance-events",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize compliance event producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.ComplianceEventProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate compliance event message against Pydantic model."""
        try:
            if isinstance(message, ComplianceEventCreate):
                validated = message
            else:
                validated = ComplianceEventCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid compliance event message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating compliance event message: {str(e)}")
    
    def publish_compliance_event(
        self,
        compliance_event: ComplianceEventCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish compliance event to Kafka topic."""
        try:
            if key is None:
                key = str(compliance_event.event_id)
            return self.send(message=compliance_event.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing compliance event: {e}", exc_info=True)
            return False

