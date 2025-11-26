"""
OwnLens - ML Module: Base Evaluator

Base class for model evaluators.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class BaseEvaluator(ABC):
    """
    Base class for model evaluators.
    
    Handles model evaluation including:
    - Metric calculation
    - Performance analysis
    - Result reporting
    """
    
    def __init__(self, metrics: Optional[list] = None):
        """
        Initialize the evaluator.
        
        Args:
            metrics: List of metrics to calculate
        """
        self.metrics = metrics or []
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def evaluate(
        self,
        model: Any,
        X: Any,
        y: Any,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Evaluate the model.
        
        Args:
            model: ML model instance
            X: Features
            y: True labels
            **kwargs: Additional evaluation parameters
        
        Returns:
            Dictionary with evaluation metrics
        """
        pass
    
    def calculate_metrics(
        self,
        y_true: Any,
        y_pred: Any,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Calculate evaluation metrics.
        
        Args:
            y_true: True labels
            y_pred: Predicted labels
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with metrics
        """
        metrics = {}
        
        for metric_name in self.metrics:
            try:
                metric_value = self._calculate_metric(
                    metric_name,
                    y_true,
                    y_pred,
                    **kwargs
                )
                metrics[metric_name] = metric_value
            except Exception as e:
                self.logger.warning(
                    f"Could not calculate metric {metric_name}: {e}"
                )
        
        return metrics
    
    def _calculate_metric(
        self,
        metric_name: str,
        y_true: Any,
        y_pred: Any,
        **kwargs
    ) -> float:
        """
        Calculate a specific metric.
        
        Args:
            metric_name: Name of the metric
            y_true: True labels
            y_pred: Predicted labels
            **kwargs: Additional parameters
        
        Returns:
            Metric value
        """
        from sklearn.metrics import (
            accuracy_score,
            precision_score,
            recall_score,
            f1_score,
            roc_auc_score,
            mean_squared_error,
            mean_absolute_error,
            r2_score
        )
        
        metric_functions = {
            'accuracy': accuracy_score,
            'precision': precision_score,
            'recall': recall_score,
            'f1': f1_score,
            'roc_auc': roc_auc_score,
            'mse': mean_squared_error,
            'mae': mean_absolute_error,
            'r2': r2_score
        }
        
        if metric_name not in metric_functions:
            raise ValueError(f"Unknown metric: {metric_name}")
        
        metric_func = metric_functions[metric_name]
        
        # Handle metrics that need different parameters
        if metric_name in ['precision', 'recall', 'f1']:
            return metric_func(y_true, y_pred, average='weighted', **kwargs)
        elif metric_name == 'roc_auc':
            # For binary classification
            if len(set(y_true)) == 2:
                return metric_func(y_true, y_pred, **kwargs)
            else:
                return metric_func(y_true, y_pred, multi_class='ovr', **kwargs)
        else:
            return metric_func(y_true, y_pred, **kwargs)

