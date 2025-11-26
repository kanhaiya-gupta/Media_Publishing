"""
Unit Tests for UserSegmentationTrainer
======================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.customer.segmentation.trainer import UserSegmentationTrainer


class TestUserSegmentationTrainer:
    """Test UserSegmentationTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = UserSegmentationTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    def test_init_with_n_clusters(self):
        """Test trainer initialization with custom n_clusters."""
        trainer = UserSegmentationTrainer(n_clusters=5)
        
        assert trainer is not None

    @patch('src.ownlens.ml.models.customer.segmentation.trainer.UserFeaturesLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "user_id": ["1", "2", "3", "4", "5"],
            "feature1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "feature2": [1.0, 2.0, 3.0, 4.0, 5.0],
        })
        mock_loader.load.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = UserSegmentationTrainer(n_clusters=3)
        trainer.model.train = Mock(return_value={"n_clusters": 3, "inertia": 0.5})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

