"""
Loader Factory
==============

Automatically selects the appropriate loader based on destination.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
import logging

from ..loaders import (
    S3Loader,
    PostgreSQLLoader,
    ClickHouseLoader,
)
from ..base.loader import BaseLoader

logger = logging.getLogger(__name__)


class LoaderFactory:
    """
    Factory for automatically selecting loaders based on destination.
    
    Maps destination types to their appropriate loader classes.
    """
    
    # Mapping of destination types to loader classes
    LOADER_MAP: Dict[str, type] = {
        "s3": S3Loader,
        "minio": S3Loader,  # Alias for S3
        "postgresql": PostgreSQLLoader,
        "postgres": PostgreSQLLoader,  # Alias
        "clickhouse": ClickHouseLoader,
        "ch": ClickHouseLoader,  # Alias
    }
    
    @classmethod
    def get_loader(
        cls,
        destination: str,
        spark: SparkSession,
        config: Optional[Any] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ) -> BaseLoader:
        """
        Get loader instance for destination.
        
        Args:
            destination: Destination type ("s3", "postgresql", "clickhouse")
            spark: SparkSession instance
            config: ETLConfig instance (optional)
            loader_config: Loader-specific configuration (optional)
            
        Returns:
            Loader instance
            
        Raises:
            ValueError: If destination is not supported
        """
        destination_lower = destination.lower().strip()
        loader_class = cls.LOADER_MAP.get(destination_lower)
        
        if not loader_class:
            raise ValueError(
                f"Unknown destination: {destination}. "
                f"Supported destinations: {', '.join(cls.LOADER_MAP.keys())}"
            )
        
        logger.info(f"Using {loader_class.__name__} for destination: {destination}")
        
        # Create loader instance with config
        return loader_class(spark, config, loader_config)

