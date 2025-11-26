"""
Unit Tests for ContentRecommendationTrainer
============================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.editorial.recommendation.trainer import ContentRecommendationTrainer


class TestContentRecommendationTrainer:
    """Test ContentRecommendationTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = ContentRecommendationTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    @patch('src.ownlens.ml.models.editorial.recommendation.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "user_id": ["u1", "u2", "u3"],
            "interaction_score": [1.0, 3.0, 2.0],
        })
        mock_loader.load_content_events.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = ContentRecommendationTrainer()
        trainer.model.train = Mock(return_value={"n_components": 50})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

