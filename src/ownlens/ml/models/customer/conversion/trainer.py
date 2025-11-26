"""
OwnLens - ML Module: Conversion Trainer

Trainer for conversion prediction model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from ....base.trainer import BaseTrainer
from ....data.loaders.user_features_loader import UserFeaturesLoader
from ....features.customer.behavioral_features import BehavioralFeatureEngineer
from ....features.customer.engagement_features import EngagementFeatureEngineer
from ....features.customer.temporal_features import TemporalFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import ConversionModel


class ConversionTrainer(BaseTrainer):
    """
    Trainer for conversion prediction model.
    
    Handles the complete training workflow:
    1. Load user features from customer_user_features
    2. Prepare features and labels
    3. Train model
    4. Evaluate model
    """
    
    def __init__(
        self,
        model: Optional[ConversionModel] = None,
        use_feature_engineers: bool = True
    ):
        """
        Initialize conversion trainer.
        
        Args:
            model: Conversion model instance (if None, creates default)
            use_feature_engineers: If True, use feature engineering modules
        """
        if model is None:
            model = ConversionModel()
        
        # Initialize feature engineers
        feature_engineer = None
        if use_feature_engineers:
            behavioral_fe = BehavioralFeatureEngineer()
            engagement_fe = EngagementFeatureEngineer()
            temporal_fe = TemporalFeatureEngineer()
            feature_engineer = CompositeFeatureEngineer([
                behavioral_fe,
                engagement_fe,
                temporal_fe
            ])
        
        super().__init__(model, feature_engineer, None)
        self.logger.info("Initialized conversion trainer")
    
    def load_data(
        self,
        feature_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load user features from customer_user_features table.
        
        Args:
            feature_date: Feature date (default: today)
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of users
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with user features
        """
        loader = UserFeaturesLoader()
        data = loader.load(
            feature_date=feature_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit,
            include_references=False
        )
        
        self.logger.info(f"Loaded {len(data)} user features")
        return data
    
    def prepare_features(
        self,
        data: pd.DataFrame,
        test_size: float = 0.2,
        random_state: int = 42,
        **kwargs
    ) -> tuple:
        """
        Prepare features and labels for training.
        
        Creates conversion label based on subscription_tier.
        Users with premium/pro/enterprise tiers are considered converted.
        
        Args:
            data: User features DataFrame
            test_size: Test set size (0.0 to 1.0)
            random_state: Random seed
            **kwargs: Additional parameters
        
        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        self.logger.info("Preparing features and labels")
        
        # Apply feature engineering if available
        if self.feature_engineer:
            self.logger.info("Applying feature engineering...")
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (exclude metadata columns)
        exclude_cols = [
            'user_feature_id', 'user_id', 'company_id', 'brand_id',
            'feature_date', 'created_at', 'updated_at', 'ml_features',
            'first_session_date', 'last_session_date',  # Date columns
            'current_subscription_tier'  # Used to create label
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        
        # Handle missing values
        X = data[feature_cols].copy()
        
        # Handle categorical columns separately - convert to object type first
        categorical_cols = X.select_dtypes(include=['category']).columns
        if len(categorical_cols) > 0:
            X[categorical_cols] = X[categorical_cols].astype('object')
        
        # Drop object/string columns - XGBoost requires numeric features only
        # These columns can be encoded separately if needed, but for now we drop them
        object_cols = X.select_dtypes(include=['object']).columns
        if len(object_cols) > 0:
            self.logger.info(f"Dropping {len(object_cols)} object columns for XGBoost: {list(object_cols)}")
            X = X.drop(columns=object_cols)
        
        # Fill NaN values for numeric columns
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Create conversion label
        # Users with premium/pro/enterprise tiers are considered converted
        if 'current_subscription_tier' in data.columns:
            converted_tiers = ['premium', 'pro', 'enterprise']
            y = data['current_subscription_tier'].isin(converted_tiers).astype(int)
        elif 'subscription_tier' in data.columns:
            converted_tiers = ['premium', 'pro', 'enterprise']
            y = data['subscription_tier'].isin(converted_tiers).astype(int)
        else:
            # Fallback: use engagement score as proxy
            if 'avg_engagement_score' in data.columns:
                median_engagement = data['avg_engagement_score'].median()
                y = (data['avg_engagement_score'] > median_engagement).astype(int)
            else:
                raise ValueError("Cannot create conversion label: missing subscription_tier or engagement_score")
        
        self.logger.info(f"Conversion distribution: {(y == 0).sum()} not converted, {(y == 1).sum()} converted")
        
        # Check if we have both classes - XGBoost requires at least 2 classes
        unique_classes = y.unique()
        if len(unique_classes) < 2:
            self.logger.warning(f"Only one class found: {unique_classes}. Cannot train binary classification model.")
            raise ValueError(f"Cannot train conversion model: only one class found ({unique_classes[0]}). Need at least 2 classes.")
        
        # Split data
        if test_size > 0:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=test_size,
                random_state=random_state,
                stratify=y
            )
            
            self.logger.info(f"Train set: {len(X_train)} samples, Test set: {len(X_test)} samples")
            return X_train, X_test, y_train, y_test
        else:
            self.logger.info(f"Training on all {len(X)} samples (no test split)")
            return X, None, y, None
    
    def train(
        self,
        feature_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        test_size: float = 0.2,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            feature_date: Feature date (default: today)
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of users
            test_size: Test set size
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        # Load data
        data = self.load_data(
            feature_date=feature_date,
            brand_id=brand_id,
            company_id=company_id,
            limit=limit
        )
        
        if len(data) == 0:
            raise ValueError("No data loaded for training")
        
        # Prepare features
        split_result = self.prepare_features(data, test_size=test_size, **kwargs)
        
        if test_size > 0:
            X_train, X_test, y_train, y_test = split_result
            validation_data = (X_test, y_test)
        else:
            X_train, _, y_train, _ = split_result
            validation_data = None
        
        # Train model
        training_metrics = self.model.train(
            X_train,
            y_train,
            validation_data=validation_data,
            **kwargs
        )
        
        return training_metrics

