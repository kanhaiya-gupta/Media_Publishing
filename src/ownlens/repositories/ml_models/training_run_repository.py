"""
OwnLens - ML Models Domain: Training Run Repository

Repository for ml_model_training_runs table.
"""

from typing import Dict, List, Optional, Any, Union
from uuid import UUID
from datetime import datetime, date
import logging

from ..shared import BaseRepository
from ...models.ml_models.training_run import TrainingRun, TrainingRunCreate, TrainingRunUpdate

logger = logging.getLogger(__name__)


class TrainingRunRepository(BaseRepository[TrainingRun]):
    """Repository for ml_model_training_runs table."""

    def __init__(self, connection_manager):
        """Initialize the training run repository."""
        super().__init__(connection_manager, table_name="ml_model_training_runs")
        self.training_runs_table = "ml_model_training_runs"

    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        return "run_id"

    def get_table_name(self) -> str:
        """Get the table name."""
        return "ml_model_training_runs"

    # ============================================================================
    # TRAINING RUN OPERATIONS
    # ============================================================================

    async def create_training_run(self, run_data: Union[TrainingRunCreate, Dict[str, Any]]) -> Optional[TrainingRun]:
        """
        Create a new training run record.
        
        Args:
            run_data: Training run creation data (Pydantic model or dict)
            
        Returns:
            Created training run with generated ID, or None if creation failed
        """
        try:
            # Base create() method already handles dict/Pydantic model conversion
            result = await self.create(run_data, table_name=self.training_runs_table)
            if result:
                result = self._convert_json_to_lists(result)
                return TrainingRun(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error creating training run: {e}")
            return None

    async def get_training_run_by_id(self, run_id: UUID) -> Optional[TrainingRun]:
        """Get training run by ID."""
        try:
            result = await self.get_by_id(run_id)
            if result:
                result = self._convert_json_to_lists(result)
                return TrainingRun(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting training run by ID {run_id}: {e}")
            return None

    async def get_training_runs_by_model(self, model_id: UUID, limit: int = 100, offset: int = 0) -> List[TrainingRun]:
        """Get all training runs for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="training_start_time",
                limit=limit,
                offset=offset
            )
            return [TrainingRun(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting training runs by model {model_id}: {e}")
            return []

    async def get_training_runs_by_status(self, status: str, limit: int = 100) -> List[TrainingRun]:
        """Get training runs by status."""
        try:
            results = await self.get_all(
                filters={"status": status},
                order_by="training_start_time",
                limit=limit
            )
            return [TrainingRun(**self._convert_json_to_lists(row)) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting training runs by status {status}: {e}")
            return []

    async def get_latest_training_run(self, model_id: UUID) -> Optional[TrainingRun]:
        """Get the latest training run for a model."""
        try:
            results = await self.get_all(
                filters={"model_id": model_id},
                order_by="training_start_time",
                limit=1
            )
            if results:
                result = self._convert_json_to_lists(results[0])
                return TrainingRun(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest training run for model {model_id}: {e}")
            return None

    async def update_training_run(self, run_id: UUID, run_data: TrainingRunUpdate) -> Optional[TrainingRun]:
        """Update training run data."""
        try:
            data = run_data.model_dump(exclude_unset=True)
            
            result = await self.update(run_id, data)
            if result:
                result = self._convert_json_to_lists(result)
                return TrainingRun(**result)
            return None
            
        except Exception as e:
            logger.error(f"Error updating training run {run_id}: {e}")
            return None

    async def delete_training_run(self, run_id: UUID) -> bool:
        """Delete training run and related data (CASCADE will handle related records)."""
        try:
            return await self.delete(run_id)
            
        except Exception as e:
            logger.error(f"Error deleting training run {run_id}: {e}")
            return False

