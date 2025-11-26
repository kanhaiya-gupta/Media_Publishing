"""
Audit Log Producer
===================

Kafka producer for audit logs.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.audit.audit_log import AuditLog, AuditLogCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class AuditLogProducer(BaseKafkaProducer):
    """
    Kafka producer for audit logs.
    
    Publishes audit logs to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "audit-logs",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize audit log producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.AuditLogProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate audit log message against Pydantic model."""
        try:
            if isinstance(message, AuditLogCreate):
                validated = message
            else:
                validated = AuditLogCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid audit log message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating audit log message: {str(e)}")
    
    def publish_audit_log(
        self,
        audit_log: AuditLogCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish audit log to Kafka topic."""
        try:
            if key is None:
                key = str(audit_log.log_id)
            return self.send(message=audit_log.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing audit log: {e}", exc_info=True)
            return False

