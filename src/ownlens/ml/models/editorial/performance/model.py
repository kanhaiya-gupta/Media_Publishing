"""
OwnLens - ML Module: Article Performance Model

XGBoost-based article performance prediction model.
"""

from typing import Any, Dict
import xgboost as xgb
import numpy as np
import pandas as pd

from ....base.model import BaseMLModel


class ArticlePerformanceModel(BaseMLModel):
    """
    Article performance prediction model using XGBoost.
    
    Predicts article engagement score based on article features and metadata.
    """
    
    def __init__(
        self,
        model_id: str = "article_performance_v1.0.0",
        model_config: Dict[str, Any] = None,
        model_name: str = "Article Performance Prediction Model",
        model_version: str = "1.0.0"
    ):
        """
        Initialize article performance prediction model.
        
        Args:
            model_id: Unique model identifier
            model_config: XGBoost hyperparameters
            model_name: Human-readable model name
            model_version: Model version
        """
        if model_config is None:
            model_config = {
                'objective': 'reg:squarederror',
                'eval_metric': 'rmse',
                'max_depth': 6,
                'learning_rate': 0.1,
                'n_estimators': 100,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42
            }
        
        super().__init__(model_id, model_config, model_name, model_version)
    
    def build_model(self) -> xgb.XGBRegressor:
        """Build XGBoost article performance prediction model."""
        self.model = xgb.XGBRegressor(**self.model_config)
        self.logger.info("Built XGBoost article performance prediction model")
        return self.model
    
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        validation_data: tuple = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the article performance prediction model.
        
        Args:
            X: Training features (article metadata, content features)
            y: Training labels (engagement_score or total_views)
            validation_data: Optional (X_val, y_val) for validation
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        if self.model is None:
            self.build_model()
        
        self.logger.info(f"Training article performance model on {len(X)} samples")
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Train model
        if validation_data:
            X_val, y_val = validation_data
            X_val = X_val.fillna(0)
            X_val = X_val.replace([np.inf, -np.inf], 0)
            
            self.model.fit(
                X, y,
                eval_set=[(X_val, y_val)],
                verbose=kwargs.get('verbose', False)
            )
        else:
            self.model.fit(X, y, verbose=kwargs.get('verbose', False))
        
        self.is_trained = True
        
        # Get feature importance
        feature_importance = dict(
            zip(X.columns, self.model.feature_importances_)
        )
        
        training_metrics = {
            'n_samples': len(X),
            'n_features': len(X.columns),
            'feature_importance': feature_importance
        }
        
        self.logger.info("Article performance model training completed")
        return training_metrics
    
    def predict(self, X: pd.DataFrame, **kwargs) -> np.ndarray:
        """
        Predict article performance (engagement score).
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Predicted engagement scores
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return self.model.predict(X, **kwargs)

