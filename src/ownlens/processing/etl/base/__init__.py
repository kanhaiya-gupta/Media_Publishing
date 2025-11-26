"""
ETL Base Classes
================

Base classes for ETL operations (Extractor, Transformer, Loader, Pipeline).
"""

from .extractor import BaseExtractor
from .transformer import BaseTransformer
from .loader import BaseLoader
from .pipeline import ETLPipeline

__all__ = [
    "BaseExtractor",
    "BaseTransformer",
    "BaseLoader",
    "ETLPipeline",
]

