"""
Kafka Producers and Consumers
==============================

Kafka producers and consumers for event streaming.
"""

from .base import BaseKafkaProducer, BaseKafkaConsumer

# Customer Domain
from .producers.customer import UserEventProducer, SessionProducer
from .consumers.customer import UserEventConsumer, SessionConsumer

# Editorial Domain
from .producers.editorial import ContentEventProducer, ArticleProducer
from .consumers.editorial import ContentEventConsumer, ArticleConsumer

# Company Domain
from .producers.company import InternalContentProducer
from .consumers.company import InternalContentConsumer

# Security Domain
from .producers.security import SecurityEventProducer
from .consumers.security import SecurityEventConsumer

# Compliance Domain
from .producers.compliance import ComplianceEventProducer
from .consumers.compliance import ComplianceEventConsumer

# Audit Domain
from .producers.audit import AuditLogProducer
from .consumers.audit import AuditLogConsumer

# Data Quality Domain
from .producers.data_quality import QualityMetricProducer
from .consumers.data_quality import QualityMetricConsumer

# ML Models Domain
from .producers.ml_models import ModelPredictionProducer
from .consumers.ml_models import ModelPredictionConsumer

__all__ = [
    # Base classes
    "BaseKafkaProducer",
    "BaseKafkaConsumer",
    # Customer
    "UserEventProducer",
    "SessionProducer",
    "UserEventConsumer",
    "SessionConsumer",
    # Editorial
    "ContentEventProducer",
    "ArticleProducer",
    "ContentEventConsumer",
    "ArticleConsumer",
    # Company
    "InternalContentProducer",
    "InternalContentConsumer",
    # Security
    "SecurityEventProducer",
    "SecurityEventConsumer",
    # Compliance
    "ComplianceEventProducer",
    "ComplianceEventConsumer",
    # Audit
    "AuditLogProducer",
    "AuditLogConsumer",
    # Data Quality
    "QualityMetricProducer",
    "QualityMetricConsumer",
    # ML Models
    "ModelPredictionProducer",
    "ModelPredictionConsumer",
]

