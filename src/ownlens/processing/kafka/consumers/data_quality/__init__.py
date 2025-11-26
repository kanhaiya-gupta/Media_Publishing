"""
Data Quality Domain Kafka Consumers
====================================

Data quality-specific event consumers.
"""

from .quality_metric_consumer import QualityMetricConsumer

__all__ = [
    "QualityMetricConsumer",
]

