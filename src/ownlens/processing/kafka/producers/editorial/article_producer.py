"""
Article Producer
================

Kafka producer for editorial articles.
"""

from typing import Optional, Dict, Any
import logging

from ...base.producer import BaseKafkaProducer
from src.ownlens.models.editorial.article import Article, ArticleCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ArticleProducer(BaseKafkaProducer):
    """
    Kafka producer for editorial articles.
    
    Publishes article events to Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "editorial-articles",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize article producer."""
        super().__init__(bootstrap_servers, topic, config)
        self.logger = logging.getLogger(f"{__name__}.ArticleProducer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate article message against Pydantic model."""
        try:
            if isinstance(message, ArticleCreate):
                validated = message
            else:
                validated = ArticleCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid article message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating article message: {str(e)}")
    
    def publish_article(
        self,
        article: ArticleCreate,
        key: Optional[str] = None
    ) -> bool:
        """Publish article to Kafka topic."""
        try:
            if key is None:
                key = str(article.article_id)
            return self.send(message=article.model_dump(), key=key)
        except Exception as e:
            self.logger.error(f"Error publishing article: {e}", exc_info=True)
            return False

