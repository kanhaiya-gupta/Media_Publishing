"""
PostgreSQL Extractor
====================

Extract data from PostgreSQL database.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from ..base.extractor import BaseExtractor
from ..utils.config import ETLConfig, get_etl_config

import logging

logger = logging.getLogger(__name__)


# Comprehensive list of PostgreSQL tables organized by domain
# This matches the complete schema definition
POSTGRESQL_TABLES = [
    # Base Domain
    "companies",
    "brands",
    "brand_countries",
    "countries",
    "cities",
    "categories",
    "users",
    "user_accounts",
    "device_types",
    "operating_systems",
    "browsers",
    # Customer Domain
    "customer_sessions",
    "customer_events",
    "customer_user_features",
    "customer_user_segments",
    "customer_user_segment_assignments",
    "customer_churn_predictions",
    "customer_recommendations",
    "customer_conversion_predictions",
    # Editorial Core Domain
    "editorial_authors",
    "editorial_articles",
    "editorial_article_performance",
    "editorial_author_performance",
    "editorial_category_performance",
    "editorial_content_events",
    "editorial_headline_tests",
    "editorial_trending_topics",
    "editorial_content_recommendations",
    # Editorial Content Domain
    "editorial_article_content",
    "editorial_content_versions",
    # Editorial Media Domain
    "editorial_media_assets",
    "editorial_media_variants",
    "editorial_content_media",
    "editorial_media_collections",
    "editorial_media_collection_items",
    "editorial_media_usage",
    # Company Domain
    "company_departments",
    "company_employees",
    "company_internal_content",
    "company_content_performance",
    "company_department_performance",
    "company_content_events",
    "company_employee_engagement",
    "company_communications_analytics",
    # Security Domain
    "security_roles",
    "security_permissions",
    "security_role_permissions",
    "security_user_roles",
    "security_api_keys",
    "security_api_key_usage",
    "security_user_sessions",
    # Audit Domain
    "audit_logs",
    "audit_data_changes",
    "audit_data_access",
    "audit_security_events",
    "audit_data_lineage",
    "audit_compliance_events",
    # Compliance Domain
    "compliance_user_consent",
    "compliance_data_subject_requests",
    "compliance_retention_policies",
    "compliance_retention_executions",
    "compliance_anonymized_data",
    "compliance_privacy_assessments",
    "compliance_breach_incidents",
    # ML Models Domain
    "ml_model_registry",
    "ml_model_features",
    "ml_model_training_runs",
    "ml_model_predictions",
    "ml_model_monitoring",
    "ml_model_ab_tests",
    # Data Quality Domain
    "data_quality_rules",
    "data_quality_checks",
    "data_quality_metrics",
    "data_quality_alerts",
    "data_validation_results",
    # Configuration Domain
    "configuration_feature_flags",
    "configuration_feature_flag_history",
    "configuration_system_settings",
    "configuration_system_settings_history",
]


class PostgreSQLExtractor(BaseExtractor):
    """
    Extract data from PostgreSQL database.
    
    Supports:
    - Full table extraction
    - Query-based extraction
    - Incremental extraction (with timestamp/ID column)
    - Partitioned extraction
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize PostgreSQL extractor.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            extractor_config: Extractor-specific configuration
        """
        super().__init__(spark, extractor_config)
        self.etl_config = config or get_etl_config()
        # Extractors read from raw database (where data is generated)
        # Loaders write to production database (where transformed data goes)
        self.jdbc_url = self.etl_config.get_postgresql_url(use_raw=True)
        self.jdbc_properties = self.etl_config.get_postgresql_properties()
        logger.info(f"PostgreSQL Extractor configured to read from raw database: {self.etl_config.postgresql_raw_database}")
    
    def extract(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        schema: Optional[str] = None,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 1,
        **kwargs
    ) -> DataFrame:
        """
        Extract data from PostgreSQL.
        
        Args:
            table: Table name (required if query not provided)
            query: SQL query (required if table not provided)
            schema: Schema name (optional)
            partition_column: Column to use for partitioning (optional)
            lower_bound: Lower bound for partitioning (optional)
            upper_bound: Upper bound for partitioning (optional)
            num_partitions: Number of partitions (default: 1)
            **kwargs: Additional JDBC options
            
        Returns:
            Spark DataFrame with extracted data
        """
        try:
            if not table and not query:
                raise ValueError("Either 'table' or 'query' must be provided")
            
            jdbc_options = {
                "url": self.jdbc_url,
                **self.jdbc_properties,
                **kwargs
            }
            
            if query:
                # Use query for extraction
                jdbc_options["query"] = query
                self.logger.info(f"Extracting data using query: {query[:100]}...")
                df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            
            elif table:
                # Use table for extraction
                full_table_name = f"{schema}.{table}" if schema else table
                jdbc_options["dbtable"] = full_table_name
                
                # Add partitioning if specified
                if partition_column and lower_bound is not None and upper_bound is not None:
                    jdbc_options["partitionColumn"] = partition_column
                    jdbc_options["lowerBound"] = str(lower_bound)
                    jdbc_options["upperBound"] = str(upper_bound)
                    jdbc_options["numPartitions"] = str(num_partitions)
                    self.logger.info(
                        f"Extracting data from {full_table_name} with partitioning "
                        f"(column: {partition_column}, partitions: {num_partitions})"
                    )
                else:
                    self.logger.info(f"Extracting data from {full_table_name}")
                
                df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            
            self.log_extraction_stats(df, f"PostgreSQL: {table or 'query'}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from PostgreSQL: {e}", exc_info=True)
            raise
    
    def extract_incremental(
        self,
        table: str,
        timestamp_column: str,
        last_timestamp: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract incremental data based on timestamp column.
        
        Args:
            table: Table name
            timestamp_column: Timestamp column name
            last_timestamp: Last extracted timestamp (ISO format string)
            schema: Schema name (optional)
            **kwargs: Additional JDBC options
            
        Returns:
            Spark DataFrame with incremental data
        """
        full_table_name = f"{schema}.{table}" if schema else table
        
        if last_timestamp:
            query = f"""
                SELECT * FROM {full_table_name}
                WHERE {timestamp_column} > '{last_timestamp}'
                ORDER BY {timestamp_column}
            """
        else:
            query = f"SELECT * FROM {full_table_name} ORDER BY {timestamp_column}"
        
        self.logger.info(f"Extracting incremental data from {full_table_name} since {last_timestamp or 'beginning'}")
        return self.extract(query=query, **kwargs)

