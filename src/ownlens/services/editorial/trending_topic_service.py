"""
OwnLens - Editorial Domain: Trending Topic Service

Service for trending topic management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import TrendingTopicRepository
from ...models.editorial.trending_topic import TrendingTopic, TrendingTopicCreate, TrendingTopicUpdate

logger = logging.getLogger(__name__)


class TrendingTopicService(BaseService[TrendingTopic, TrendingTopicCreate, TrendingTopicUpdate, TrendingTopic]):
    """Service for trending topic management."""
    
    def __init__(self, repository: TrendingTopicRepository, service_name: str = None):
        """Initialize the trending topic service."""
        super().__init__(repository, service_name or "TrendingTopicService")
        self.repository: TrendingTopicRepository = repository
    
    def get_model_class(self):
        """Get the TrendingTopic model class."""
        return TrendingTopic
    
    def get_create_model_class(self):
        """Get the TrendingTopicCreate model class."""
        return TrendingTopicCreate
    
    def get_update_model_class(self):
        """Get the TrendingTopicUpdate model class."""
        return TrendingTopicUpdate
    
    def get_in_db_model_class(self):
        """Get the TrendingTopic model class."""
        return TrendingTopic
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for trending topic operations."""
        try:
            if operation == "create":
                # Validate topic name
                if hasattr(data, 'topic_name'):
                    name = data.topic_name
                else:
                    name = data.get('topic_name') if isinstance(data, dict) else None
                
                if not name or len(name.strip()) == 0:
                    raise ValidationError(
                        "Topic name is required",
                        "INVALID_NAME"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_trending_topic(self, topic_data: TrendingTopicCreate) -> TrendingTopic:
        """Create a new trending topic."""
        try:
            await self.validate_input(topic_data, "create")
            await self.validate_business_rules(topic_data, "create")
            
            result = await self.repository.create_trending_topic(topic_data)
            if not result:
                raise NotFoundError("Failed to create trending topic", "CREATE_FAILED")
            
            self.log_operation("create_trending_topic", {"topic_id": str(result.topic_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_trending_topic", "create")
            raise
    
    async def get_trending_topic_by_id(self, topic_id: UUID) -> TrendingTopic:
        """Get trending topic by ID."""
        try:
            result = await self.repository.get_trending_topic_by_id(topic_id)
            if not result:
                raise NotFoundError(f"Trending topic with ID {topic_id} not found", "TRENDING_TOPIC_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_trending_topic_by_id", "read")
            raise
    
    async def get_top_trending_topics(self, brand_id: UUID, limit: int = 100) -> List[TrendingTopic]:
        """Get top trending topics."""
        try:
            return await self.repository.get_top_trending_topics(brand_id, limit)
        except Exception as e:
            await self.handle_error(e, "get_top_trending_topics", "read")
            return []

