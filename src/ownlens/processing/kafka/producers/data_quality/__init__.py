"""
Data Quality Domain Kafka Producers
====================================

Data quality-specific event producers.
"""

from .quality_metric_producer import QualityMetricProducer

__all__ = [
    "QualityMetricProducer",
]

