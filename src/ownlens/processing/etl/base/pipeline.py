"""
ETL Pipeline
============

Base class for ETL pipeline orchestration.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
import logging

from .extractor import BaseExtractor
from .transformer import BaseTransformer
from .loader import BaseLoader

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    ETL Pipeline orchestrator.
    
    Coordinates Extract, Transform, and Load operations.
    Supports multiple extractors, transformers, and loaders.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        extractor: BaseExtractor,
        transformer: Optional[BaseTransformer] = None,
        loader: Optional[BaseLoader] = None,
        name: str = "ETLPipeline"
    ):
        """
        Initialize the ETL pipeline.
        
        Args:
            spark: SparkSession instance
            extractor: Data extractor
            transformer: Data transformer (optional)
            loader: Data loader (optional)
            name: Pipeline name
        """
        self.spark = spark
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
    
    def run(
        self,
        extract_kwargs: Optional[Dict[str, Any]] = None,
        transform_kwargs: Optional[Dict[str, Any]] = None,
        load_kwargs: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Run the complete ETL pipeline.
        
        Args:
            extract_kwargs: Arguments for extractor
            transform_kwargs: Arguments for transformer
            load_kwargs: Arguments for loader
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        try:
            self.logger.info(f"Starting ETL pipeline: {self.name}")
            
            # Extract
            self.logger.info("Extracting data...")
            df = self.extractor.extract(**(extract_kwargs or {}))
            
            if df is None or df.count() == 0:
                self.logger.warning("No data extracted, skipping pipeline")
                return False
            
            # Transform
            if self.transformer:
                self.logger.info("Transforming data...")
                df = self.transformer.transform(df, **(transform_kwargs or {}))
                
                if df is None:
                    self.logger.error("Transformation returned None")
                    return False
            
            # Load
            if self.loader:
                self.logger.info("Loading data...")
                success = self.loader.load(df, **(load_kwargs or {}))
                
                if not success:
                    self.logger.error("Load operation failed")
                    return False
            
            self.logger.info(f"ETL pipeline completed successfully: {self.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            return False
    
    def extract_only(self, **kwargs) -> Optional[DataFrame]:
        """
        Run only the extract step.
        
        Args:
            **kwargs: Arguments for extractor
            
        Returns:
            Extracted DataFrame or None if failed
        """
        try:
            self.logger.info(f"Running extract step: {self.name}")
            return self.extractor.extract(**kwargs)
        except Exception as e:
            self.logger.error(f"Extract step failed: {e}", exc_info=True)
            return None
    
    def transform_only(self, df: DataFrame, **kwargs) -> Optional[DataFrame]:
        """
        Run only the transform step.
        
        Args:
            df: Input DataFrame
            **kwargs: Arguments for transformer
            
        Returns:
            Transformed DataFrame or None if failed
        """
        if not self.transformer:
            self.logger.error("No transformer configured")
            return None
        
        try:
            self.logger.info(f"Running transform step: {self.name}")
            return self.transformer.transform(df, **kwargs)
        except Exception as e:
            self.logger.error(f"Transform step failed: {e}", exc_info=True)
            return None
    
    def load_only(self, df: DataFrame, **kwargs) -> bool:
        """
        Run only the load step.
        
        Args:
            df: DataFrame to load
            **kwargs: Arguments for loader
            
        Returns:
            True if load successful, False otherwise
        """
        if not self.loader:
            self.logger.error("No loader configured")
            return False
        
        try:
            self.logger.info(f"Running load step: {self.name}")
            return self.loader.load(df, **kwargs)
        except Exception as e:
            self.logger.error(f"Load step failed: {e}", exc_info=True)
            return False

