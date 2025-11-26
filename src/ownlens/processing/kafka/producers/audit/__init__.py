"""
Audit Domain Kafka Producers
=============================

Audit-specific event producers.
"""

from .audit_log_producer import AuditLogProducer

__all__ = [
    "AuditLogProducer",
]

