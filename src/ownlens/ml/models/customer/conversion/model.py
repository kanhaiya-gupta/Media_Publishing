"""
OwnLens - ML Module: Conversion Model

XGBoost-based conversion prediction model.
"""

from typing import Any, Dict
import xgboost as xgb
import numpy as np
import pandas as pd

from ....base.model import BaseMLModel


class ConversionModel(BaseMLModel):
    """
    Conversion prediction model using XGBoost.
    
    Predicts subscription conversion probability based on user features.
    """
    
    def __init__(
        self,
        model_id: str = "conversion_v1.0.0",
        model_config: Dict[str, Any] = None,
        model_name: str = "Conversion Prediction Model",
        model_version: str = "1.0.0"
    ):
        """
        Initialize conversion prediction model.
        
        Args:
            model_id: Unique model identifier
            model_config: XGBoost hyperparameters
            model_name: Human-readable model name
            model_version: Model version
        """
        if model_config is None:
            model_config = {
                'objective': 'binary:logistic',
                'eval_metric': 'auc',
                'max_depth': 6,
                'learning_rate': 0.1,
                'n_estimators': 100,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'random_state': 42
            }
        
        super().__init__(model_id, model_config, model_name, model_version)
    
    def build_model(self) -> xgb.XGBClassifier:
        """Build XGBoost conversion prediction model."""
        self.model = xgb.XGBClassifier(**self.model_config)
        self.logger.info("Built XGBoost conversion prediction model")
        return self.model
    
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        validation_data: tuple = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the conversion prediction model.
        
        Args:
            X: Training features
            y: Training labels (0 = not converted, 1 = converted)
            validation_data: Optional (X_val, y_val) for validation
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        if self.model is None:
            self.build_model()
        
        self.logger.info(f"Training conversion model on {len(X)} samples")
        
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
        
        self.logger.info("Conversion model training completed")
        return training_metrics
    
    def predict(self, X: pd.DataFrame, **kwargs) -> np.ndarray:
        """
        Predict conversion (binary classification).
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Binary predictions (0 = not converted, 1 = converted)
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return self.model.predict(X, **kwargs)
    
    def predict_proba(self, X: pd.DataFrame, **kwargs) -> np.ndarray:
        """
        Predict conversion probabilities.
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Conversion probabilities [prob_not_converted, prob_converted]
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        return self.model.predict_proba(X, **kwargs)

