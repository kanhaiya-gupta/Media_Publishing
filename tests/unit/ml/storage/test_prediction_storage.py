"""
Unit Tests for PredictionStorage
================================
"""

import pytest
from unittest.mock import Mock, patch
import uuid

from src.ownlens.ml.storage.prediction_storage import PredictionStorage


class TestPredictionStorage:
    """Test PredictionStorage class."""

    def test_init(self):
        """Test storage initialization."""
        storage = PredictionStorage()
        
        assert storage is not None
        assert storage.config is not None

    def test_init_with_client(self):
        """Test storage initialization with custom client."""
        mock_client = Mock()
        storage = PredictionStorage(client=mock_client)
        
        assert storage.client is mock_client

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_save_prediction_success(self, mock_client_class):
        """Test successful prediction saving."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        prediction_id = storage.save_prediction(
            model_id="model_id",
            entity_id="user_id",
            entity_type="user",
            prediction_type="churn",
            prediction_probability=0.85,
            prediction_class="churn"
        )
        
        assert prediction_id is not None
        assert isinstance(prediction_id, str)

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions(self, mock_client_class):
        """Test retrieving predictions."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id", "model_id", "user_id", "churn", 0.85)
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions(
            model_id="model_id",
            entity_id="user_id"
        )
        
        assert predictions is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_save_prediction_with_metadata(self, mock_client_class):
        """Test saving prediction with additional metadata."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        prediction_id = storage.save_prediction(
            model_id="model_id",
            entity_id="user_id",
            entity_type="user",
            prediction_type="churn",
            prediction_probability=0.85,
            prediction_class="churn",
            metadata={"feature1": 1.0, "feature2": 2.0},
            timestamp="2024-01-01T00:00:00"
        )
        
        assert prediction_id is not None
        assert isinstance(prediction_id, str)

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_save_prediction_batch(self, mock_client_class):
        """Test saving multiple predictions in batch."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = [
            {
                "model_id": "model_id",
                "entity_id": "user_1",
                "entity_type": "user",
                "prediction_type": "churn",
                "prediction_probability": 0.85,
                "prediction_class": "churn"
            },
            {
                "model_id": "model_id",
                "entity_id": "user_2",
                "entity_type": "user",
                "prediction_type": "churn",
                "prediction_probability": 0.75,
                "prediction_class": "no_churn"
            }
        ]
        
        result = storage.save_predictions_batch(predictions)
        
        assert result is not None
        assert len(result) == 2

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_with_filters(self, mock_client_class):
        """Test retrieving predictions with filters."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id_1", "model_id", "user_id", "churn", 0.85, "2024-01-01"),
            ("prediction_id_2", "model_id", "user_id", "churn", 0.80, "2024-01-02")
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions(
            model_id="model_id",
            entity_id="user_id",
            prediction_type="churn",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert predictions is not None
        assert len(predictions) > 0

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_empty_result(self, mock_client_class):
        """Test retrieving predictions with empty result."""
        mock_client = Mock()
        mock_client.execute.return_value = []
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions(
            model_id="non_existent",
            entity_id="user_id"
        )
        
        assert predictions is not None
        assert len(predictions) == 0

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_latest_prediction(self, mock_client_class):
        """Test getting latest prediction for entity."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id", "model_id", "user_id", "churn", 0.85, "2024-01-15")
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        prediction = storage.get_latest_prediction(
            model_id="model_id",
            entity_id="user_id"
        )
        
        assert prediction is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_prediction_by_id(self, mock_client_class):
        """Test getting prediction by ID."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id", "model_id", "user_id", "churn", 0.85, "2024-01-01")
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        prediction = storage.get_prediction_by_id("prediction_id")
        
        assert prediction is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_update_prediction(self, mock_client_class):
        """Test updating a prediction."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        result = storage.update_prediction(
            prediction_id="prediction_id",
            actual_value=1,
            actual_class="churn"
        )
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_delete_prediction(self, mock_client_class):
        """Test deleting a prediction."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        result = storage.delete_prediction("prediction_id")
        
        assert result is True
        mock_client.execute.assert_called()

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_by_model(self, mock_client_class):
        """Test getting all predictions for a model."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id_1", "model_id", "user_1", "churn", 0.85),
            ("prediction_id_2", "model_id", "user_2", "churn", 0.75)
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions_by_model("model_id")
        
        assert predictions is not None
        assert len(predictions) > 0

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_by_entity_type(self, mock_client_class):
        """Test getting predictions by entity type."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id", "model_id", "user_id", "churn", 0.85)
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions_by_entity_type(
            model_id="model_id",
            entity_type="user"
        )
        
        assert predictions is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_prediction_statistics(self, mock_client_class):
        """Test getting prediction statistics."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            (100, 0.85, 0.75, 0.95)  # count, avg_probability, min_probability, max_probability
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        stats = storage.get_prediction_statistics(
            model_id="model_id",
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
        
        assert stats is not None
        assert "count" in stats or isinstance(stats, dict)

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_save_prediction_error_handling(self, mock_client_class):
        """Test prediction saving error handling."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        with pytest.raises(Exception, match="Database error"):
            storage.save_prediction(
                model_id="model_id",
                entity_id="user_id",
                entity_type="user",
                prediction_type="churn",
                prediction_probability=0.85,
                prediction_class="churn"
            )

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_error_handling(self, mock_client_class):
        """Test prediction retrieval error handling."""
        mock_client = Mock()
        mock_client.execute.side_effect = Exception("Database error")
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        with pytest.raises(Exception, match="Database error"):
            storage.get_predictions(
                model_id="model_id",
                entity_id="user_id"
            )

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_save_prediction_with_features(self, mock_client_class):
        """Test saving prediction with feature values."""
        mock_client = Mock()
        mock_client.execute.return_value = None
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        prediction_id = storage.save_prediction(
            model_id="model_id",
            entity_id="user_id",
            entity_type="user",
            prediction_type="churn",
            prediction_probability=0.85,
            prediction_class="churn",
            features={"feature1": 1.0, "feature2": 2.0, "feature3": 3.0}
        )
        
        assert prediction_id is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_with_limit(self, mock_client_class):
        """Test retrieving predictions with limit."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id_1", "model_id", "user_1", "churn", 0.85),
            ("prediction_id_2", "model_id", "user_2", "churn", 0.75)
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions(
            model_id="model_id",
            entity_id="user_id",
            limit=10
        )
        
        assert predictions is not None

    @patch('src.ownlens.ml.storage.prediction_storage.Client')
    def test_get_predictions_with_ordering(self, mock_client_class):
        """Test retrieving predictions with ordering."""
        mock_client = Mock()
        mock_client.execute.return_value = [
            ("prediction_id_2", "model_id", "user_id", "churn", 0.75, "2024-01-02"),
            ("prediction_id_1", "model_id", "user_id", "churn", 0.85, "2024-01-01")
        ]
        mock_client_class.return_value = mock_client
        
        storage = PredictionStorage(client=mock_client)
        
        predictions = storage.get_predictions(
            model_id="model_id",
            entity_id="user_id",
            order_by="timestamp",
            order_direction="desc"
        )
        
        assert predictions is not None
        assert len(predictions) > 0

