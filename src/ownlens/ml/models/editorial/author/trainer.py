"""
OwnLens - ML Module: Author Performance Trainer

Trainer for author performance prediction model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from ....base.trainer import BaseTrainer
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.author_features import AuthorFeatureEngineer
from .model import AuthorPerformanceModel


class AuthorPerformanceTrainer(BaseTrainer):
    """
    Trainer for author performance prediction model.
    
    Handles the complete training workflow:
    1. Load author performance data
    2. Prepare features and labels
    3. Train model
    4. Evaluate model
    """
    
    def __init__(
        self,
        model: Optional[AuthorPerformanceModel] = None,
        use_feature_engineers: bool = True
    ):
        """
        Initialize author performance trainer.
        
        Args:
            model: Author performance model instance (if None, creates default)
            use_feature_engineers: If True, use feature engineering modules
        """
        if model is None:
            model = AuthorPerformanceModel()
        
        # Initialize feature engineers
        feature_engineer = None
        if use_feature_engineers:
            author_fe = AuthorFeatureEngineer()
            feature_engineer = author_fe
        
        super().__init__(model, feature_engineer, None)
        self.logger.info("Initialized author performance trainer")
    
    def load_data(
        self,
        performance_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load author performance data.
        
        Args:
            performance_date: Performance date (default: today)
            brand_id: Filter by brand_id
            limit: Limit number of authors
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with author performance data
        """
        loader = EditorialLoader()
        data = loader.load_author_performance(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit
        )
        
        self.logger.info(f"Loaded {len(data)} author performance records")
        return data
    
    def prepare_features(
        self,
        data: pd.DataFrame,
        test_size: float = 0.2,
        random_state: int = 42,
        target_col: str = 'avg_engagement_score',
        **kwargs
    ) -> tuple:
        """
        Prepare features and labels for training.
        
        Args:
            data: Author performance DataFrame
            test_size: Test set size (0.0 to 1.0)
            random_state: Random seed
            target_col: Target column name
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
            'performance_id', 'author_id', 'brand_id', 'performance_date',
            'best_article_id', 'top_category_id',
            'created_at', 'updated_at',
            target_col, 'author_rank', 'engagement_rank'  # Target columns
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        
        # Handle missing values
        X = data[feature_cols].copy()
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Create target
        if target_col not in data.columns:
            raise ValueError(f"Target column '{target_col}' not found in data")
        
        y = data[target_col].fillna(0)
        
        self.logger.info(f"Target distribution: min={y.min():.2f}, max={y.max():.2f}, mean={y.mean():.2f}")
        
        # Split data
        if test_size > 0:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=test_size,
                random_state=random_state
            )
            
            self.logger.info(f"Train set: {len(X_train)} samples, Test set: {len(X_test)} samples")
            return X_train, X_test, y_train, y_test
        else:
            self.logger.info(f"Training on all {len(X)} samples (no test split)")
            return X, None, y, None
    
    def train(
        self,
        performance_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        test_size: float = 0.2,
        target_col: str = 'avg_engagement_score',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            performance_date: Performance date (default: today)
            brand_id: Filter by brand_id
            limit: Limit number of authors
            test_size: Test set size
            target_col: Target column name
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        # Load data
        data = self.load_data(
            performance_date=performance_date,
            brand_id=brand_id,
            limit=limit
        )
        
        if len(data) == 0:
            raise ValueError("No data loaded for training")
        
        # Prepare features
        split_result = self.prepare_features(
            data,
            test_size=test_size,
            target_col=target_col,
            **kwargs
        )
        
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

