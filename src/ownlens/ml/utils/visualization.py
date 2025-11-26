"""
OwnLens - ML Module: Visualization

Visualization utilities for ML models.
"""

from typing import Optional, Dict, Any
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class MLVisualizer:
    """
    Visualization utilities for ML models.
    
    Creates visualizations for:
    - Model performance
    - Feature importance
    - ROC curves
    - Confusion matrices
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize visualizer.
        
        Args:
            output_dir: Directory to save visualizations
        """
        self.output_dir = Path(output_dir) if output_dir else None
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def plot_feature_importance(
        self,
        feature_importance: Dict[str, float],
        top_n: int = 20,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot feature importance.
        
        Args:
            feature_importance: Dictionary of feature names to importance scores
            top_n: Number of top features to show
            save_path: Path to save the plot
        
        Returns:
            Matplotlib figure
        """
        # Sort by importance
        sorted_features = sorted(
            feature_importance.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
        
        features, importances = zip(*sorted_features)
        
        # Create plot
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.barh(range(len(features)), importances)
        ax.set_yticks(range(len(features)))
        ax.set_yticklabels(features)
        ax.set_xlabel('Importance')
        ax.set_title(f'Top {top_n} Feature Importance')
        ax.invert_yaxis()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Feature importance plot saved to {save_path}")
        elif self.output_dir:
            save_path = self.output_dir / 'feature_importance.png'
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Feature importance plot saved to {save_path}")
        
        return fig
    
    def plot_roc_curve(
        self,
        fpr: np.ndarray,
        tpr: np.ndarray,
        auc: float,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot ROC curve.
        
        Args:
            fpr: False positive rates
            tpr: True positive rates
            auc: AUC score
            save_path: Path to save the plot
        
        Returns:
            Matplotlib figure
        """
        fig, ax = plt.subplots(figsize=(8, 8))
        
        ax.plot(fpr, tpr, label=f'ROC Curve (AUC = {auc:.3f})')
        ax.plot([0, 1], [0, 1], 'k--', label='Random')
        ax.set_xlabel('False Positive Rate')
        ax.set_ylabel('True Positive Rate')
        ax.set_title('ROC Curve')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"ROC curve saved to {save_path}")
        elif self.output_dir:
            save_path = self.output_dir / 'roc_curve.png'
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"ROC curve saved to {save_path}")
        
        return fig
    
    def plot_confusion_matrix(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        labels: Optional[list] = None,
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot confusion matrix.
        
        Args:
            y_true: True labels
            y_pred: Predicted labels
            labels: Class labels
            save_path: Path to save the plot
        
        Returns:
            Matplotlib figure
        """
        from sklearn.metrics import confusion_matrix
        
        cm = confusion_matrix(y_true, y_pred, labels=labels)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax)
        ax.set_xlabel('Predicted')
        ax.set_ylabel('Actual')
        ax.set_title('Confusion Matrix')
        
        if labels:
            ax.set_xticklabels(labels)
            ax.set_yticklabels(labels)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Confusion matrix saved to {save_path}")
        elif self.output_dir:
            save_path = self.output_dir / 'confusion_matrix.png'
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Confusion matrix saved to {save_path}")
        
        return fig
    
    def plot_training_metrics(
        self,
        metrics: Dict[str, Any],
        save_path: Optional[str] = None
    ) -> plt.Figure:
        """
        Plot training metrics including ROC curves, confusion matrices, and performance metrics.
        
        Args:
            metrics: Dictionary of training metrics
            save_path: Path to save the plot
        
        Returns:
            Matplotlib figure
        """
        # Extract metrics
        performance_metrics = metrics.get('performance_metrics', metrics)
        
        # Check what plots we need to create
        has_roc = 'roc_curve' in performance_metrics and performance_metrics['roc_curve'] is not None
        has_cm = 'confusion_matrix' in performance_metrics and performance_metrics['confusion_matrix'] is not None
        numeric_metrics = {k: v for k, v in performance_metrics.items() 
                          if isinstance(v, (int, float)) and k not in ['roc_curve', 'confusion_matrix', 'classification_report']}
        
        # Calculate number of subplots needed
        n_plots = len(numeric_metrics) + (1 if has_roc else 0) + (1 if has_cm else 0)
        
        if n_plots == 0:
            # Create a simple plot with available metrics
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, 'No numeric metrics available', 
                   ha='center', va='center', fontsize=14)
            ax.set_title('Training Metrics')
            ax.axis('off')
        else:
            # Create subplots: ROC curve, confusion matrix, then numeric metrics
            n_cols = 3
            n_rows = (n_plots + n_cols - 1) // n_cols
            fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 6 * n_rows))
            
            if n_plots == 1:
                axes = [axes]
            else:
                axes = axes.flatten()
            
            plot_idx = 0
            
            # Plot ROC curve if available
            if has_roc:
                roc_data = performance_metrics['roc_curve']
                fpr = np.array(roc_data['fpr'])
                tpr = np.array(roc_data['tpr'])
                auc = performance_metrics.get('roc_auc', 0.0)
                
                ax = axes[plot_idx]
                ax.plot(fpr, tpr, label=f'ROC Curve (AUC = {auc:.3f})', linewidth=2)
                ax.plot([0, 1], [0, 1], 'k--', label='Random', alpha=0.5)
                ax.set_xlabel('False Positive Rate')
                ax.set_ylabel('True Positive Rate')
                ax.set_title('ROC Curve')
                ax.legend()
                ax.grid(True, alpha=0.3)
                plot_idx += 1
            
            # Plot confusion matrix if available
            if has_cm:
                cm_data = performance_metrics['confusion_matrix']
                # Convert to 2x2 matrix format
                if isinstance(cm_data, dict):
                    cm = np.array([
                        [cm_data.get('tn', 0), cm_data.get('fp', 0)],
                        [cm_data.get('fn', 0), cm_data.get('tp', 0)]
                    ])
                else:
                    cm = np.array(cm_data)
                
                ax = axes[plot_idx]
                sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax, 
                           xticklabels=['Negative', 'Positive'],
                           yticklabels=['Negative', 'Positive'])
                ax.set_xlabel('Predicted')
                ax.set_ylabel('Actual')
                ax.set_title('Confusion Matrix')
                plot_idx += 1
            
            # Plot numeric metrics
            for metric_name, metric_value in list(numeric_metrics.items())[:len(axes) - plot_idx]:
                ax = axes[plot_idx]
                ax.bar([metric_name.replace('_', ' ').title()], [metric_value], color='steelblue')
                ax.set_ylabel('Value')
                ax.set_title(f'{metric_name.replace("_", " ").title()}')
                ax.grid(True, axis='y', alpha=0.3)
                if metric_value > 0:
                    ax.set_ylim(0, max(metric_value * 1.1, 0.1))
                plot_idx += 1
            
            # Hide unused subplots
            for idx in range(plot_idx, len(axes)):
                axes[idx].axis('off')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Training metrics plot saved to {save_path}")
        elif self.output_dir:
            save_path = self.output_dir / 'training_metrics.png'
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"Training metrics plot saved to {save_path}")
        
        return fig

