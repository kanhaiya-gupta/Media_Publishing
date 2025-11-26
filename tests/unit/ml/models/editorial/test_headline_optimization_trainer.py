"""
Unit Tests for HeadlineOptimizationTrainer
===========================================
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.ownlens.ml.models.editorial.headline.trainer import HeadlineOptimizationTrainer


class TestHeadlineOptimizationTrainer:
    """Test HeadlineOptimizationTrainer class."""

    def test_init(self):
        """Test trainer initialization."""
        trainer = HeadlineOptimizationTrainer()
        
        assert trainer is not None
        assert trainer.model is not None

    @patch('src.ownlens.ml.models.editorial.headline.trainer.EditorialLoader')
    def test_train_success(self, mock_loader_class):
        """Test successful model training."""
        mock_loader = Mock()
        mock_data = pd.DataFrame({
            "article_id": ["a1", "a2", "a3"],
            "headline": ["headline1", "headline2", "headline3"],
            "click_rate": [0.05, 0.08, 0.06],
        })
        mock_loader.load_headline_tests.return_value = mock_data
        mock_loader_class.return_value = mock_loader
        
        trainer = HeadlineOptimizationTrainer()
        trainer.model.train = Mock(return_value={"accuracy": 0.75})
        
        metrics = trainer.train(limit=10)
        
        assert metrics is not None

