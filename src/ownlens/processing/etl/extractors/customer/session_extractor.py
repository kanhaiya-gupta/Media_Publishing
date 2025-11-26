"""
Session Extractor
=================

Extract customer sessions from PostgreSQL.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from ...postgresql_extractor import PostgreSQLExtractor
from ...utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


class SessionExtractor(PostgreSQLExtractor):
    """
    Extract customer sessions.
    
    Supports extraction from PostgreSQL (customer_sessions table).
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize session extractor."""
        super().__init__(spark, config, extractor_config)
        self.table_name = "customer_sessions"
    
    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        brand_id: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract sessions from PostgreSQL.
        
        Args:
            start_date: Start date filter (ISO format)
            end_date: End date filter (ISO format)
            brand_id: Brand ID filter (optional)
            **kwargs: Additional extraction parameters
            
        Returns:
            DataFrame with sessions
        """
        query = f"SELECT * FROM {self.table_name} WHERE 1=1"
        
        if start_date:
            query += f" AND session_date >= '{start_date}'"
        if end_date:
            query += f" AND session_date <= '{end_date}'"
        if brand_id:
            query += f" AND brand_id = '{brand_id}'"
        
        query += " ORDER BY session_date, session_start_time"
        
        self.logger.info(f"Extracting sessions from PostgreSQL: {query[:100]}...")
        return super().extract(query=query, **kwargs)

