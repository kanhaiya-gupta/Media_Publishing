"""
OwnLens - ML Module: Serialization

Model serialization utilities.
"""

import pickle
import json
from pathlib import Path
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)


def save_model(model: Any, path: str, metadata: Dict[str, Any] = None) -> bool:
    """
    Save model to disk with metadata.
    
    Args:
        model: Model object to save
        path: Path to save the model
        metadata: Additional metadata to save
    
    Returns:
        True if successful, False otherwise
    """
    try:
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        save_data = {
            'model': model,
            'metadata': metadata or {}
        }
        
        with open(path, 'wb') as f:
            pickle.dump(save_data, f)
        
        logger.info(f"Model saved to {path}")
        return True
    except Exception as e:
        logger.error(f"Error saving model: {e}")
        return False


def load_model(path: str) -> tuple:
    """
    Load model from disk.
    
    Args:
        path: Path to load the model from
    
    Returns:
        Tuple of (model, metadata)
    """
    try:
        with open(path, 'rb') as f:
            save_data = pickle.load(f)
        
        model = save_data.get('model')
        metadata = save_data.get('metadata', {})
        
        logger.info(f"Model loaded from {path}")
        return model, metadata
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        return None, {}


def save_metrics(metrics: Dict[str, Any], path: str) -> bool:
    """
    Save metrics to JSON file.
    
    Args:
        metrics: Dictionary with metrics
        path: Path to save metrics
    
    Returns:
        True if successful, False otherwise
    """
    try:
        import numpy as np
        
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert numpy types to Python types
        def convert_to_serializable(obj):
            if isinstance(obj, dict):
                return {k: convert_to_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, (np.integer, np.floating)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (np.bool_, bool)):
                return bool(obj)
            else:
                return obj
        
        serializable_metrics = convert_to_serializable(metrics)
        
        with open(path, 'w') as f:
            json.dump(serializable_metrics, f, indent=2)
        
        logger.info(f"Metrics saved to {path}")
        return True
    except Exception as e:
        logger.error(f"Error saving metrics: {e}")
        return False

