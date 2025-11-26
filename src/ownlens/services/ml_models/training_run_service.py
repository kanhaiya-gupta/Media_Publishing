"""
OwnLens - ML Models Domain: Training Run Service

Service for training run management.
"""

from typing import List, Optional
from uuid import UUID
import logging

from ..shared import BaseService, NotFoundError, ValidationError
from ...repositories.ml_models import TrainingRunRepository
from ...models.ml_models.training_run import TrainingRun, TrainingRunCreate, TrainingRunUpdate

logger = logging.getLogger(__name__)


class TrainingRunService(BaseService[TrainingRun, TrainingRunCreate, TrainingRunUpdate, TrainingRun]):
    """Service for training run management."""
    
    def __init__(self, repository: TrainingRunRepository, service_name: str = None):
        """Initialize the training run service."""
        super().__init__(repository, service_name or "TrainingRunService")
        self.repository: TrainingRunRepository = repository
    
    def get_model_class(self):
        """Get the TrainingRun model class."""
        return TrainingRun
    
    def get_create_model_class(self):
        """Get the TrainingRunCreate model class."""
        return TrainingRunCreate
    
    def get_update_model_class(self):
        """Get the TrainingRunUpdate model class."""
        return TrainingRunUpdate
    
    def get_in_db_model_class(self):
        """Get the TrainingRun model class."""
        return TrainingRun
    
    async def validate_business_rules(self, data, operation: str = "create") -> bool:
        """Validate business rules for training run operations."""
        try:
            # Validate status
            if hasattr(data, 'status'):
                status = data.status
            else:
                status = data.get('status') if isinstance(data, dict) else None
            
            if status and status not in ['pending', 'running', 'completed', 'failed', 'cancelled']:
                raise ValidationError(
                    "Invalid training run status",
                    "INVALID_STATUS"
                )
            
            return True
        except ValidationError:
            raise
        except Exception as e:
            self.logger.error(f"Error validating business rules: {e}")
            raise ValidationError(f"Business rule validation failed: {str(e)}", "VALIDATION_ERROR")
    
    async def create_training_run(self, run_data: TrainingRunCreate) -> TrainingRun:
        """Create a new training run."""
        try:
            await self.validate_input(run_data, "create")
            await self.validate_business_rules(run_data, "create")
            
            result = await self.repository.create_training_run(run_data)
            if not result:
                raise NotFoundError("Failed to create training run", "CREATE_FAILED")
            
            self.log_operation("create_training_run", {"run_id": str(result.run_id)})
            return result
        except (ValidationError, NotFoundError):
            raise
        except Exception as e:
            await self.handle_error(e, "create_training_run", "create")
            raise
    
    async def get_training_runs_by_model(self, model_id: UUID, limit: int = 100) -> List[TrainingRun]:
        """Get training runs for a model."""
        try:
            return await self.repository.get_training_runs_by_model(model_id, limit=limit)
        except Exception as e:
            await self.handle_error(e, "get_training_runs_by_model", "read")
            return []
    
    async def get_latest_training_run(self, model_id: UUID) -> Optional[TrainingRun]:
        """Get the latest training run for a model."""
        try:
            return await self.repository.get_latest_training_run(model_id)
        except Exception as e:
            await self.handle_error(e, "get_latest_training_run", "read")
            raise

