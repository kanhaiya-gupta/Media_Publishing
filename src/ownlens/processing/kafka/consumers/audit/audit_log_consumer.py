"""
Audit Log Consumer
===================

Kafka consumer for audit logs.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.audit.audit_log import AuditLog, AuditLogCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class AuditLogConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for audit logs.
    
    Consumes audit logs from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "audit-log-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize audit log consumer."""
        if topics is None:
            topics = ["audit-logs"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.AuditLogConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate audit log message against Pydantic model."""
        try:
            validated = AuditLogCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid audit log message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating audit log message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process an audit log message."""
        try:
            audit_log = AuditLogCreate.model_validate(message)
            self.logger.info(f"Processing audit log: {audit_log.log_id}")
        except Exception as e:
            self.logger.error(f"Error processing audit log: {e}", exc_info=True)
            raise

