"""
OwnLens - ML Module: Headline Optimization Trainer

Trainer for headline optimization model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

from ....base.trainer import BaseTrainer
from ....data.loaders.editorial_loader import EditorialLoader
from ....features.editorial.content_features import ContentFeatureEngineer
from .model import HeadlineOptimizationModel


class HeadlineOptimizationTrainer(BaseTrainer):
    """
    Trainer for headline optimization model.
    
    Handles the complete training workflow:
    1. Load headline test data and article performance
    2. Prepare features and labels (CTR)
    3. Train model
    4. Evaluate model
    """
    
    def __init__(
        self,
        model: Optional[HeadlineOptimizationModel] = None,
        use_feature_engineers: bool = True
    ):
        """
        Initialize headline optimization trainer.
        
        Args:
            model: Headline optimization model instance (if None, creates default)
            use_feature_engineers: If True, use feature engineering modules
        """
        if model is None:
            model = HeadlineOptimizationModel()
        
        # Initialize feature engineers
        feature_engineer = None
        if use_feature_engineers:
            content_fe = ContentFeatureEngineer()
            feature_engineer = content_fe
        
        super().__init__(model, feature_engineer, None)
        self.logger.info("Initialized headline optimization trainer")
    
    def load_data(
        self,
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Load headline test data.
        
        Note: This is a placeholder - headline test data would come from
        editorial_headline_tests table. For now, we'll use article performance
        data to simulate headline tests.
        
        Args:
            brand_id: Filter by brand_id
            limit: Limit number of records
            **kwargs: Additional parameters
        
        Returns:
            DataFrame with headline test data
        """
        loader = EditorialLoader()
        
        # Load article performance data (simulating headline tests)
        # In production, this would load from editorial_headline_tests
        articles = loader.load_article_performance(
            brand_id=brand_id,
            limit=limit
        )
        
        if len(articles) == 0:
            self.logger.warning("No article performance data found")
            return pd.DataFrame()
        
        # Load article metadata for headline features
        article_ids = articles['article_id'].unique().tolist()
        metadata = loader.load_articles_metadata(
            brand_id=brand_id,
            limit=len(article_ids)
        )
        
        if len(metadata) > 0:
            # Merge with performance data
            data = articles.merge(
                metadata[['article_id', 'headline', 'word_count', 'article_type']],
                on='article_id',
                how='left'
            )
        else:
            data = articles
        
        self.logger.info(f"Loaded {len(data)} headline test records")
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
        
        Creates CTR label from clicks/views ratio.
        
        Args:
            data: Headline test DataFrame
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
        
        # Create headline features
        if 'headline' in data.columns:
            # Headline length
            data['headline_length'] = data['headline'].str.len().fillna(0)
            data['headline_word_count'] = data['headline'].str.split().str.len().fillna(0)
            
            # Headline features
            data['headline_has_question'] = data['headline'].str.contains('?', na=False).astype(int)
            data['headline_has_number'] = data['headline'].str.contains(r'\d', na=False, regex=True).astype(int)
            data['headline_has_exclamation'] = data['headline'].str.contains('!', na=False).astype(int)
        
        # Select feature columns (exclude metadata columns)
        exclude_cols = [
            'performance_id', 'article_id', 'brand_id', 'performance_date',
            'headline', 'title', 'subtitle', 'summary',
            'created_at', 'updated_at',
            'total_clicks', 'total_views'  # Used to calculate CTR
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        
        # Handle missing values
        X = data[feature_cols].copy()
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Create CTR label (clicked = 1 if CTR > median, else 0)
        if 'total_views' in data.columns and 'total_clicks' in data.columns:
            ctr = data['total_clicks'] / (data['total_views'] + 1)
            median_ctr = ctr.median()
            y = (ctr > median_ctr).astype(int)
        else:
            raise ValueError("Cannot create CTR label: missing 'total_views' or 'total_clicks' columns")
        
        self.logger.info(f"CTR distribution: {(y == 0).sum()} low CTR, {(y == 1).sum()} high CTR")
        
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
        brand_id: Optional[str] = None,
        limit: Optional[int] = None,
        test_size: float = 0.2,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            brand_id: Filter by brand_id
            limit: Limit number of records
            test_size: Test set size
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        # Load data
        data = self.load_data(
            brand_id=brand_id,
            limit=limit
        )
        
        if len(data) == 0:
            raise ValueError("No data loaded for training")
        
        # Prepare features
        split_result = self.prepare_features(
            data,
            test_size=test_size,
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

