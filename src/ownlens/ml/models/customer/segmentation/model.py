"""
OwnLens - ML Module: User Segmentation Model

K-Means-based user segmentation model.
"""

from typing import Any, Dict, List
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ....base.model import BaseMLModel


class UserSegmentationModel(BaseMLModel):
    """
    User segmentation model using K-Means clustering.
    
    Segments users into groups based on behavioral and engagement features.
    """
    
    def __init__(
        self,
        model_id: str = "user_segmentation_v1.0.0",
        model_config: Dict[str, Any] = None,
        model_name: str = "User Segmentation Model",
        model_version: str = "1.0.0"
    ):
        """
        Initialize user segmentation model.
        
        Args:
            model_id: Unique model identifier
            model_config: K-Means hyperparameters
            model_name: Human-readable model name
            model_version: Model version
        """
        if model_config is None:
            model_config = {
                'n_clusters': 5,
                'init': 'k-means++',
                'n_init': 10,
                'max_iter': 300,
                'random_state': 42
            }
        
        super().__init__(model_id, model_config, model_name, model_version)
        self.scaler = StandardScaler()
        self.segment_names = None
    
    def build_model(self) -> KMeans:
        """Build K-Means user segmentation model."""
        self.model = KMeans(**self.model_config)
        self.logger.info("Built K-Means user segmentation model")
        return self.model
    
    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series = None,
        validation_data: tuple = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the user segmentation model.
        
        Args:
            X: Training features (user behavioral and engagement features)
            y: Not used (for compatibility)
            validation_data: Not used (for compatibility)
            **kwargs: Additional training parameters
        
        Returns:
            Dictionary with training metrics
        """
        if self.model is None:
            self.build_model()
        
        self.logger.info(f"Training user segmentation model on {len(X)} samples")
        
        # Adjust n_clusters if we have fewer samples than clusters
        n_clusters = self.model_config.get('n_clusters', 5)
        n_samples = len(X)
        if n_samples < n_clusters:
            self.logger.warning(f"Only {n_samples} samples but {n_clusters} clusters requested. Reducing to {n_samples} clusters.")
            n_clusters = max(1, n_samples)  # At least 1 cluster, but not more than samples
            # Update model config and rebuild model
            self.model_config['n_clusters'] = n_clusters
            self.model = KMeans(**self.model_config)
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model
        self.model.fit(X_scaled, **kwargs)
        
        self.is_trained = True
        
        # Get cluster labels
        labels = self.model.labels_
        
        # Calculate cluster statistics
        n_clusters = self.model_config.get('n_clusters', len(np.unique(labels)))
        cluster_stats = {}
        
        for i in range(n_clusters):
            cluster_mask = labels == i
            cluster_size = cluster_mask.sum()
            cluster_stats[f'cluster_{i}'] = {
                'size': int(cluster_size),
                'percentage': float(cluster_size / len(X) * 100)
            }
        
        # Calculate inertia (within-cluster sum of squares)
        inertia = self.model.inertia_
        
        # Generate segment names if not provided
        if self.segment_names is None:
            self.segment_names = self._generate_segment_names(X, labels)
        
        training_metrics = {
            'n_samples': len(X),
            'n_features': len(X.columns),
            'n_clusters': n_clusters,
            'inertia': float(inertia),
            'cluster_stats': cluster_stats,
            'segment_names': self.segment_names
        }
        
        self.logger.info("User segmentation model training completed")
        return training_metrics
    
    def predict(self, X: pd.DataFrame, **kwargs) -> np.ndarray:
        """
        Predict user segments.
        
        Args:
            X: Features for prediction
            **kwargs: Additional prediction parameters
        
        Returns:
            Cluster labels (segment assignments)
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Handle missing values
        X = X.fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        # Scale features
        X_scaled = self.scaler.transform(X)
        
        return self.model.predict(X_scaled, **kwargs)
    
    def _generate_segment_names(
        self,
        X: pd.DataFrame,
        labels: np.ndarray
    ) -> Dict[int, str]:
        """
        Generate segment names based on cluster characteristics.
        
        Args:
            X: Feature DataFrame
            labels: Cluster labels
        
        Returns:
            Dictionary mapping cluster number to segment name
        """
        segment_names = {}
        n_clusters = len(np.unique(labels))
        
        # Calculate average features per cluster
        for i in range(n_clusters):
            cluster_mask = labels == i
            cluster_data = X[cluster_mask]
            
            # Determine segment name based on key features
            if 'avg_engagement_score' in X.columns:
                avg_engagement = cluster_data['avg_engagement_score'].mean()
                total_sessions = cluster_data['total_sessions'].mean() if 'total_sessions' in X.columns else 0
                
                if avg_engagement > X['avg_engagement_score'].quantile(0.75):
                    if total_sessions > X['total_sessions'].quantile(0.75) if 'total_sessions' in X.columns else 0:
                        segment_names[i] = 'power_user'
                    else:
                        segment_names[i] = 'highly_engaged'
                elif avg_engagement > X['avg_engagement_score'].quantile(0.5):
                    segment_names[i] = 'engaged'
                elif avg_engagement > X['avg_engagement_score'].quantile(0.25):
                    segment_names[i] = 'casual'
                else:
                    segment_names[i] = 'low_engagement'
            else:
                # Default naming
                segment_names[i] = f'segment_{i+1}'
        
        return segment_names
    
    def get_cluster_centers(self) -> pd.DataFrame:
        """
        Get cluster centers (representative feature values for each segment).
        
        Returns:
            DataFrame with cluster centers
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before getting cluster centers")
        
        centers = self.model.cluster_centers_
        # Inverse transform to get original scale
        centers_original = self.scaler.inverse_transform(centers)
        
        return pd.DataFrame(centers_original, columns=self.scaler.feature_names_in_)

