"""
Base Kafka Classes
==================

Base classes for Kafka producers and consumers.
"""

from .producer import BaseKafkaProducer
from .consumer import BaseKafkaConsumer

__all__ = [
    "BaseKafkaProducer",
    "BaseKafkaConsumer",
]

