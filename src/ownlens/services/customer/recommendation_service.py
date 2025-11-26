"""
OwnLens - Customer Domain: Recommendation Service

Service for recommendation management.
"""

from typing import List, Optional
from uuid import UUID
from datetime import date
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.customer import RecommendationRepository
from ...models.customer.recommendation import Recommendation, RecommendationCreate, RecommendationUpdate

logger = logging.getLogger(__name__)


class RecommendationService(BaseService[Recommendation, RecommendationCreate, RecommendationUpdate, Recommendation]):
    """Service for recommendation management."""
    
    def __init__(self, repository: RecommendationRepository, service_name: str = None):
        """Initialize the recommendation service."""
        super().__init__(repository, service_name or "RecommendationService")
        self.repository: RecommendationRepository = repository
    
    def get_model_class(self):
        """Get the Recommendation model class."""
        return Recommendation
    
    def get_create_model_class(self):
        """Get the RecommendationCreate model class."""
        return RecommendationCreate
    
    def get_update_model_class(self):
        """Get the RecommendationUpdate model class."""
        return RecommendationUpdate
    
    def get_in_db_model_class(self):
        """Get the Recommendation model class."""
        return Recommendation
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for recommendation operations."""
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
    
    async def create_recommendation(self, recommendation_data: RecommendationCreate) -> Recommendation:
        """Create a new recommendation."""
        try:
            await self.validate_input(recommendation_data, "create")
            await self.validate_business_rules(recommendation_data, "create")
            
            result = await self.repository.create_recommendation(recommendation_data)
            if not result:
                raise NotFoundError("Failed to create recommendation", "CREATE_FAILED")
            
            self.log_operation("create_recommendation", {"recommendation_id": str(result.recommendation_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_recommendation", "create")
            raise
    
    async def get_recommendation_by_id(self, recommendation_id: UUID) -> Recommendation:
        """Get recommendation by ID."""
        try:
            result = await self.repository.get_recommendation_by_id(recommendation_id)
            if not result:
                raise NotFoundError(f"Recommendation with ID {recommendation_id} not found", "RECOMMENDATION_NOT_FOUND")
            return result
        except NotFoundError:
            raise
        except Exception as e:
            await self.handle_error(e, "get_recommendation_by_id", "read")
            raise
    
    async def get_recommendations_by_user(self, user_id: UUID, limit: int = 100) -> List[Recommendation]:
        """Get recommendations for a user."""
        try:
            return await self.repository.get_recommendations_by_user(user_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_recommendations_by_user", "read")
            return []
    
    async def get_top_recommendations(self, brand_id: UUID, limit: int = 100) -> List[Recommendation]:
        """Get top recommendations by score."""
        try:
            return await self.repository.get_top_recommendations(brand_id, limit)
        except Exception as e:
            await self.handle_error(e, "get_top_recommendations", "read")
            return []








