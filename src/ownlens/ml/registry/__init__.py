"""
OwnLens - ML Module: Model Registry

Model registry integration for saving and loading models from ClickHouse.
"""

from .model_registry import ModelRegistry
from .version_manager import VersionManager
from .metadata_manager import MetadataManager

__all__ = [
    "ModelRegistry",
    "VersionManager",
    "MetadataManager",
]

