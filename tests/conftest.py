"""
Pytest configuration and fixtures
"""
import pytest
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture(scope="session")
def test_data_dir():
    """Return path to test data directory"""
    return os.path.join(os.path.dirname(__file__), 'data')

@pytest.fixture(scope="session")
def ml_models_dir():
    """Return path to ML models directory"""
    return os.path.join(os.path.dirname(__file__), '..', 'ml', 'models')

