"""
Unit Tests for AuthorPerformanceTrainer
=======================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.editorial.author.trainer import AuthorPerformanceTrainer


class TestAuthorPerformanceTrainer:
    """Test AuthorPerformanceTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = AuthorPerformanceTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    @patch('src.ownlens.ml.models.editorial.author.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "author_id": ["auth1", "auth2", "auth3"],
            "feature1": [1.0, 2.0, 3.0],
            "performance_score": [0.8, 0.9, 0.7],
        })
        mock_loader.load_author_performance.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = AuthorPerformanceTrainer()
        trainer.model.train = Mock(return_value={"r2_score": 0.80})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

