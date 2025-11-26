"""
OwnLens - ML Module: Churn Evaluator

Evaluator for churn prediction model.
"""

from typing import Any, Dict
import pandas as pd
import numpy as np
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    roc_curve,
    confusion_matrix,
    classification_report
)

from ....base.evaluator import BaseEvaluator


class ChurnEvaluator(BaseEvaluator):
    """
    Evaluator for churn prediction model.
    
    Calculates classification metrics for churn prediction.
    """
    
    def __init__(self):
        """Initialize churn evaluator."""
        metrics = ['accuracy', 'precision', 'recall', 'f1', 'roc_auc']
        super().__init__(metrics)
        self.logger.info("Initialized churn evaluator")
    
    def evaluate(
        self,
        model: Any,
        X: pd.DataFrame,
        y: pd.Series,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Evaluate churn prediction model.
        
        Args:
            model: Churn model instance
            X: Test features
            y: True labels
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with evaluation metrics
        """
        self.logger.info("Evaluating churn model")
        
        # Make predictions
        y_pred = model.predict(X)
        y_proba = model.predict_proba(X)[:, 1]  # Probability of churn
        
        # Calculate metrics
        metrics = {
            'accuracy': accuracy_score(y, y_pred),
            'precision': precision_score(y, y_pred, average='weighted', zero_division=0),
            'recall': recall_score(y, y_pred, average='weighted', zero_division=0),
            'f1': f1_score(y, y_pred, average='weighted', zero_division=0),
            'roc_auc': roc_auc_score(y, y_proba) if len(set(y)) == 2 else 0.0
        }
        
        # Confusion matrix
        cm = confusion_matrix(y, y_pred)
        metrics['confusion_matrix'] = {
            'tn': int(cm[0, 0]),
            'fp': int(cm[0, 1]),
            'fn': int(cm[1, 0]),
            'tp': int(cm[1, 1])
        }
        
        # Classification report
        report = classification_report(y, y_pred, output_dict=True, zero_division=0)
        metrics['classification_report'] = report
        
        # ROC curve data
        if len(set(y)) == 2:
            fpr, tpr, thresholds = roc_curve(y, y_proba)
            metrics['roc_curve'] = {
                'fpr': fpr.tolist(),
                'tpr': tpr.tolist(),
                'thresholds': thresholds.tolist()
            }
        
        self.logger.info(f"Evaluation metrics: {metrics}")
        return metrics

