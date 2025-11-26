"""
Audit Domain Kafka Consumers
=============================

Audit-specific event consumers.
"""

from .audit_log_consumer import AuditLogConsumer

__all__ = [
    "AuditLogConsumer",
]

