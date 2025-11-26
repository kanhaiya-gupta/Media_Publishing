"""
OwnLens - Editorial Domain: Content Recommendation Service

Service for content recommendation management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import datetime
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.editorial import ContentRecommendationRepository
from ...models.editorial.content_recommendation import ContentRecommendation, ContentRecommendationCreate, ContentRecommendationUpdate

logger = logging.getLogger(__name__)


class ContentRecommendationService(BaseService[ContentRecommendation, ContentRecommendationCreate, ContentRecommendationUpdate, ContentRecommendation]):
    """Service for content recommendation management."""
    
    def __init__(self, repository: ContentRecommendationRepository, service_name: str = None):
        """Initialize the content recommendation service."""
        super().__init__(repository, service_name or "ContentRecommendationService")
        self.repository: ContentRecommendationRepository = repository
    
    def get_model_class(self):
        """Get the ContentRecommendation model class."""
        return ContentRecommendation
    
    def get_create_model_class(self):
        """Get the ContentRecommendationCreate model class."""
        return ContentRecommendationCreate
    
    def get_update_model_class(self):
        """Get the ContentRecommendationUpdate model class."""
        return ContentRecommendationUpdate
    
    def get_in_db_model_class(self):
        """Get the ContentRecommendation model class."""
        return ContentRecommendation
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for content recommendation operations."""
        try:
            if operation == "create":
                # Validate recommendation score
                if hasattr(data, 'recommendation_score'):
                    score = data.recommendation_score
                else:
                    score = data.get('recommendation_score') if isinstance(data, dict) else None
                
                if score is not None and (score < 0 or score > 1):
                    raise ValidationError(
                        "Recommendation score must be between 0 and 1",
                        "INVALID_SCORE"
                    )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_content_recommendation(self, recommendation_data: ContentRecommendationCreate) -> ContentRecommendation:
        """Create a new content recommendation."""
        try:
            await self.validate_input(recommendation_data, "create")
            await self.validate_business_rules(recommendation_data, "create")
            
            result = await self.repository.create_content_recommendation(recommendation_data)
            if not result:
                raise NotFoundError("Failed to create content recommendation", "CREATE_FAILED")
            
            self.log_operation("create_content_recommendation", {"recommendation_id": str(result.recommendation_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_content_recommendation", "create")
            raise
    
    async def get_content_recommendation_by_id(self, recommendation_id: UUID) -> ContentRecommendation:
        """Get content recommendation by ID."""
        try:
            result = await self.repository.get_content_recommendation_by_id(recommendation_id)
            if not result:
                raise NotFoundError(f"Content recommendation with ID {recommendation_id} not found", "CONTENT_RECOMMENDATION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_content_recommendation_by_id", "read")
            raise
    
    async def get_content_recommendations_by_brand(self, brand_id: UUID, limit: int = 100) -> List[ContentRecommendation]:
        """Get content recommendations for a brand."""
        try:
            return await self.repository.get_content_recommendations_by_brand(brand_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_content_recommendations_by_brand", "read")
            return []

