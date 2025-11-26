"""
OwnLens - ML Module: User Segmentation Trainer

Trainer for user segmentation model.
"""

from typing import Any, Dict, Optional
from datetime import date
import pandas as pd
import numpy as np

from ....base.trainer import BaseTrainer
from ....data.loaders.user_features_loader import UserFeaturesLoader
from ....features.customer.behavioral_features import BehavioralFeatureEngineer
from ....features.customer.engagement_features import EngagementFeatureEngineer
from ....features.customer.temporal_features import TemporalFeatureEngineer
from ....features.composite import CompositeFeatureEngineer
from .model import UserSegmentationModel


class UserSegmentationTrainer(BaseTrainer):
    """
    Trainer for user segmentation model.
    
    Handles the complete training workflow:
    1. Load user features from customer_user_features
    2. Prepare features
    3. Train clustering model
    4. Analyze segments
    """
    
    def __init__(
        self,
        model: Optional[UserSegmentationModel] = None,
        n_clusters: int = 5,
        use_feature_engineers: bool = True
    ):
        """
        Initialize user segmentation trainer.
        
        Args:
            model: User segmentation model instance (if None, creates default)
            n_clusters: Number of clusters/segments
            use_feature_engineers: If True, use feature engineering modules
        """
        if model is None:
            model_config = {'n_clusters': n_clusters}
            model = UserSegmentationModel(model_config=model_config)
        
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
        self.logger.info("Initialized user segmentation trainer")
    
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
        **kwargs
    ) -> pd.DataFrame:
        """
        Prepare features for clustering.
        
        Args:
            data: User features DataFrame
            **kwargs: Additional parameters
        
        Returns:
            Prepared features DataFrame
        """
        self.logger.info("Preparing features for segmentation")
        
        # Apply feature engineering if available
        if self.feature_engineer:
            self.logger.info("Applying feature engineering...")
            data = self.feature_engineer.engineer_features(data, **kwargs)
        
        # Select feature columns (exclude metadata columns)
        exclude_cols = [
            'user_feature_id', 'user_id', 'company_id', 'brand_id',
            'feature_date', 'created_at', 'updated_at', 'ml_features',
            'first_session_date', 'last_session_date'  # Date columns
        ]
        
        feature_cols = [col for col in data.columns if col not in exclude_cols]
        
        # Handle missing values
        X = data[feature_cols].copy()
        
        # Handle categorical columns separately - convert to object type first
        categorical_cols = X.select_dtypes(include=['category']).columns
        if len(categorical_cols) > 0:
            X[categorical_cols] = X[categorical_cols].astype('object')
        
        # Drop object/string columns - K-Means requires numeric features only
        # These columns can be encoded separately if needed, but for now we drop them
        object_cols = X.select_dtypes(include=['object']).columns
        if len(object_cols) > 0:
            self.logger.info(f"Dropping {len(object_cols)} object columns for K-Means: {list(object_cols)}")
            X = X.drop(columns=object_cols)
        
        # Fill NaN values for numeric columns
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        self.logger.info(f"Prepared {len(feature_cols)} features for {len(X)} users")
        return X
    
    def train(
        self,
        feature_date: Optional[date] = None,
        brand_id: Optional[str] = None,
        company_id: Optional[str] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute complete training workflow.
        
        Args:
            feature_date: Feature date (default: today)
            brand_id: Filter by brand_id
            company_id: Filter by company_id
            limit: Limit number of users
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics and segment analysis
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
        X = self.prepare_features(data, **kwargs)
        
        # Store user IDs for later
        user_ids = data['user_id'].values if 'user_id' in data.columns else None
        
        # Remove test_size from kwargs (KMeans doesn't accept it)
        model_kwargs = {k: v for k, v in kwargs.items() if k != 'test_size'}
        
        # Train model
        training_metrics = self.model.train(X, **model_kwargs)
        
        # Get segment assignments
        labels = self.model.predict(X)
        training_metrics['segment_assignments'] = labels.tolist()
        
        # Add user IDs if available
        if user_ids is not None:
            training_metrics['user_ids'] = user_ids.tolist()
        
        # Segment analysis
        segment_analysis = self._analyze_segments(data, X, labels)
        training_metrics['segment_analysis'] = segment_analysis
        
        return training_metrics
    
    def _analyze_segments(
        self,
        data: pd.DataFrame,
        X: pd.DataFrame,
        labels: np.ndarray
    ) -> Dict[str, Any]:
        """
        Analyze segments to understand their characteristics.
        
        Args:
            data: Original user features DataFrame
            X: Prepared features DataFrame
            labels: Cluster labels
        
        Returns:
            Dictionary with segment analysis
        """
        analysis = {}
        n_clusters = len(np.unique(labels))
        
        for i in range(n_clusters):
            cluster_mask = labels == i
            cluster_data = X[cluster_mask]
            
            segment_name = self.model.segment_names.get(i, f'segment_{i+1}')
            
            analysis[segment_name] = {
                'cluster_number': int(i),
                'size': int(cluster_mask.sum()),
                'percentage': float(cluster_mask.sum() / len(X) * 100),
                'avg_features': cluster_data.mean().to_dict()
            }
            
            # Add key metrics if available
            if 'avg_engagement_score' in cluster_data.columns:
                analysis[segment_name]['avg_engagement'] = float(cluster_data['avg_engagement_score'].mean())
            
            if 'total_sessions' in cluster_data.columns:
                analysis[segment_name]['avg_sessions'] = float(cluster_data['total_sessions'].mean())
            
            if 'days_since_last_session' in cluster_data.columns:
                analysis[segment_name]['avg_days_since_last_session'] = float(cluster_data['days_since_last_session'].mean())
        
        return analysis

