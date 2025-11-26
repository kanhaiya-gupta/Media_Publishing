"""
Article Consumer
================

Kafka consumer for editorial articles.
"""

from typing import Optional, Dict, Any, List
import logging

from ...base.consumer import BaseKafkaConsumer
from src.ownlens.models.editorial.article import Article, ArticleCreate
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class ArticleConsumer(BaseKafkaConsumer):
    """
    Kafka consumer for editorial articles.
    
    Consumes article events from Kafka topic for real-time processing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topics: List[str] = None,
        group_id: str = "article-consumer-group",
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize article consumer."""
        if topics is None:
            topics = ["editorial-articles"]
        super().__init__(bootstrap_servers, topics, group_id, config)
        self.logger = logging.getLogger(f"{__name__}.ArticleConsumer")
    
    def validate_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Validate article message against Pydantic model."""
        try:
            validated = ArticleCreate.model_validate(message)
            return validated.model_dump()
        except ValidationError as e:
            error_messages = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
            error_str = "; ".join(error_messages)
            raise ValueError(f"Invalid article message: {error_str}")
        except Exception as e:
            raise ValueError(f"Error validating article message: {str(e)}")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process an article message."""
        try:
            article = ArticleCreate.model_validate(message)
            self.logger.info(f"Processing article: {article.article_id}")
        except Exception as e:
            self.logger.error(f"Error processing article: {e}", exc_info=True)
            raise

