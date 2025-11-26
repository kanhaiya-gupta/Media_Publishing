"""
Unit Tests for TrendingTopicsTrainer
====================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.editorial.trending.trainer import TrendingTopicsTrainer


class TestTrendingTopicsTrainer:
    """Test TrendingTopicsTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = TrendingTopicsTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    @patch('src.ownlens.ml.models.editorial.trending.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "content": ["text1", "text2", "text3"],
            "trending_score": [0.8, 0.9, 0.7],
        })
        mock_loader.load_articles_metadata.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = TrendingTopicsTrainer()
        trainer.model.train = Mock(return_value={"topics": 10})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

