"""
Editorial Domain Kafka Consumers
=================================

Editorial-specific event consumers.
"""

from .content_event_consumer import ContentEventConsumer
from .article_consumer import ArticleConsumer

__all__ = [
    "ContentEventConsumer",
    "ArticleConsumer",
]

