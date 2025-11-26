"""
Editorial Domain Kafka Producers
=================================

Editorial-specific event producers.
"""

from .content_event_producer import ContentEventProducer
from .article_producer import ArticleProducer

__all__ = [
    "ContentEventProducer",
    "ArticleProducer",
]

