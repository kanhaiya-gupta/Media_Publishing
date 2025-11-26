"""
Unit Tests for UserRecommendationTrainer
=========================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.customer.recommendation.trainer import UserRecommendationTrainer


class TestUserRecommendationTrainer:
    """Test UserRecommendationTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = UserRecommendationTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    def test_init_with_n_components(self):
        """Test trainer initialization with custom n_components."""
        trainer = UserRecommendationTrainer(n_components=100)
        
        assert trainer is not None

    @patch('src.ownlens.ml.models.customer.recommendation.trainer.EditorialLoader')
    def test_load_data_success(self, mock_loader_class):
        """Test successful data loading."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "user_id": ["1", "2", "3"],
            "article_id": ["a1", "a2", "a3"],
            "event_type": ["article_view", "article_click", "article_share"],
        })
        mock_loader.load_content_events.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = UserRecommendationTrainer()
        data = trainer.load_data(limit=10)
        
        assert data is not None
        assert len(data) > 0

    @patch('src.ownlens.ml.models.customer.recommendation.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "user_id": ["1", "2", "3", "4", "5"],
            "article_id": ["a1", "a2", "a3", "a4", "a5"],
            "interaction_score": [1.0, 3.0, 5.0, 4.0, 2.0],
        })
        mock_loader.load_content_events.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = UserRecommendationTrainer(n_components=5)
        trainer.model.train = Mock(return_value={"n_components": 5})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

