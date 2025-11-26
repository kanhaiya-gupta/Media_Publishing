"""
OwnLens - ML Module: Churn Training Pipeline

End-to-end training pipeline for churn prediction model.
"""

from typing import Optional, Dict, Any
from datetime import date
import logging
import json
import numpy as np
from pathlib import Path

from ...models.customer.churn.trainer import ChurnTrainer
from ...models.customer.churn.model import ChurnModel
from ...models.customer.churn.evaluator import ChurnEvaluator
from ...utils.config import get_ml_config

logger = logging.getLogger(__name__)


class ChurnTrainingPipeline:
    """
    End-to-end training pipeline for churn prediction.
    
    Handles complete workflow:
    1. Load data
    2. Train model
    3. Evaluate model
    4. Save model
    5. Register model (if registry enabled)
    """
    
    def __init__(self):
        """Initialize churn training pipeline."""
        self.config = get_ml_config()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.info("Initialized churn training pipeline")
    
    def run(
        self,
        feature_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        test_size: float = 0.2,
        save_model: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run complete training pipeline.
        
        Args:
            feature_date: Feature date (default: today)
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of users
            test_size: Test set size
            save_model: If True, save model to disk
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training results and metrics
        """
        self.logger.info("=" * 80)
        self.logger.info("CHURN PREDICTION TRAINING PIPELINE")
        self.logger.info("=" * 80)
        
        # Initialize trainer
        trainer = ChurnTrainer()
        
        # Train model
        self.logger.info("Starting model training...")
        metrics = trainer.train(
            feature_date=feature_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit,
            test_size=test_size,
            **kwargs
        )
        
        # Save model
        if save_model:
            model_path = self._save_model(trainer.model, metrics)
            metrics['model_path'] = model_path
        
        # Save metrics
        metrics_path = self._save_metrics(metrics)
        metrics['metrics_path'] = metrics_path
        
        self.logger.info("=" * 80)
        self.logger.info("TRAINING PIPELINE COMPLETED")
        self.logger.info("=" * 80)
        self.logger.info(f"Model saved to: {metrics.get('model_path', 'N/A')}")
        self.logger.info(f"Metrics saved to: {metrics.get('metrics_path', 'N/A')}")
        
        return metrics
    
    def _save_model(self, model: ChurnModel, metrics: Dict[str, Any]) -> str:
        """Save model to disk."""
        model_dir = Path(self.config.ml_models_dir) / "customer" / "churn"
        model_dir.mkdir(parents=True, exist_ok=True)
        
        model_filename = f"{model.model_id}.pkl"
        model_path = model_dir / model_filename
        
        if model.save(str(model_path)):
            self.logger.info(f"Model saved to {model_path}")
            return str(model_path)
        else:
            raise RuntimeError(f"Failed to save model to {model_path}")
    
    def _save_metrics(self, metrics: Dict[str, Any]) -> str:
        """Save metrics to JSON file."""
        results_dir = Path(self.config.ml_results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)
        
        metrics_filename = f"churn_training_metrics_{metrics.get('model_id', 'unknown')}.json"
        metrics_path = results_dir / metrics_filename
        
        # Convert numpy types to Python types for JSON serialization
        def convert_to_serializable(obj):
            if isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return obj
        
        serializable_metrics = convert_to_serializable(metrics)
        
        with open(metrics_path, 'w') as f:
            json.dump(serializable_metrics, f, indent=2)
        
        self.logger.info(f"Metrics saved to {metrics_path}")
        return str(metrics_path)

