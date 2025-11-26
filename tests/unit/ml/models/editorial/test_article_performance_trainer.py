"""
Unit Tests for ArticlePerformanceTrainer
=========================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.editorial.performance.trainer import ArticlePerformanceTrainer


class TestArticlePerformanceTrainer:
    """Test ArticlePerformanceTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = ArticlePerformanceTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    @patch('src.ownlens.ml.models.editorial.performance.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "feature1": [1.0, 2.0, 3.0],
            "performance_score": [0.8, 0.9, 0.7],
        })
        mock_loader.load_articles_metadata.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = ArticlePerformanceTrainer()
        trainer.model.train = Mock(return_value={"r2_score": 0.85})
        trainer.evaluator.evaluate = Mock(return_value={"r2_score": 0.85, "mae": 0.1})
        
        metrics = trainer.train(limit=10, test_size=0.2)
        
        assert metrics is not None

