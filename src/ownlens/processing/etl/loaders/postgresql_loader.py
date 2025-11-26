"""
PostgreSQL Loader
=================

Load data to PostgreSQL database.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import BinaryType, StringType

from ..base.loader import BaseLoader
from ..utils.config import ETLConfig, get_etl_config

import logging
import uuid

logger = logging.getLogger(__name__)


class PostgreSQLLoader(BaseLoader):
    """
    Load data to PostgreSQL database.
    
    Supports:
    - Append mode
    - Overwrite mode
    - Upsert mode (using merge)
    - Batch loading
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize PostgreSQL loader.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            loader_config: Loader-specific configuration
        """
        super().__init__(spark, loader_config)
        self.etl_config = config or get_etl_config()
        # Loaders write to production database (where transformed data goes)
        # Extractors read from raw database (where data is generated)
        self.jdbc_url = self.etl_config.get_postgresql_url(use_raw=False)
        self.jdbc_properties = self.etl_config.get_postgresql_properties()
        self.logger.info(f"PostgreSQL Loader configured to write to production database: {self.etl_config.postgresql_database}")
    
    def load(
        self,
        df: DataFrame,
        destination: str,
        mode: str = "append",
        batch_size: int = 10000,
        primary_key: Optional[List[str]] = None,
        conflict_resolution: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Load data to PostgreSQL table.
        
        Args:
            df: DataFrame to load
            destination: Table name (can include schema.table)
            mode: Write mode ("append", "overwrite", "upsert", "ignore", "error")
            batch_size: Batch size for JDBC writes
            primary_key: Primary key columns for upsert operations
            conflict_resolution: Conflict resolution strategy ("update", "skip", "error")
            **kwargs: Additional JDBC options
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            if not self.validate_input(df):
                return False
            
            self.log_load_stats(df, f"PostgreSQL: {destination}")
            
            # Since data was generated per schema and validated, we can write all columns directly
            # Try to get table columns for validation, but don't fail if we can't
            table_columns = self._get_table_columns(destination)
            
            if table_columns:
                # Filter DataFrame to only include columns that exist in the table
                df_columns = set(df.columns)
                common_columns = df_columns.intersection(table_columns)
                
                if not common_columns:
                    self.logger.error(f"No common columns between DataFrame and table {destination}")
                    self.logger.error(f"DataFrame columns: {sorted(df_columns)}")
                    self.logger.error(f"Table columns: {sorted(table_columns)}")
                    return False
                
                # Select only common columns if there's a mismatch
                if common_columns != df_columns:
                    missing_cols = df_columns - common_columns
                    extra_cols = table_columns - df_columns
                    self.logger.warning(
                        f"DataFrame has columns not in table {destination}: {missing_cols}. "
                        f"These will be skipped."
                    )
                    if extra_cols:
                        self.logger.info(
                            f"Table {destination} has columns not in DataFrame: {extra_cols}. "
                            f"These will use their default values."
                        )
                    df = df.select(*sorted(common_columns))
            # If we can't get table columns, trust that the DataFrame matches the schema
            # (since data was generated per schema and validated)
            
            # Map foreign keys from raw database UUIDs to production database UUIDs
            # This is necessary because UUIDs differ between raw and production databases
            # We map based on business keys (e.g., company_code, brand_code)
            df = self._map_foreign_keys(df, destination)
            
            # Cast columns to proper types for PostgreSQL
            df = self._cast_columns_for_postgresql(df, destination)
            
            # Handle upsert mode
            if mode == "upsert":
                upsert_success = self._upsert(
                    df, destination, primary_key, conflict_resolution, batch_size, **kwargs
                )
                if not upsert_success:
                    # ON CONFLICT failed - fall back to append mode with explicit casting
                    self.logger.warning(
                        f"Upsert failed for {destination}. Falling back to append mode with explicit casting."
                    )
                    # Use the same INSERT logic with explicit casting, but without ON CONFLICT
                    return self._insert_with_casting(df, destination, batch_size, **kwargs)
                else:
                    return True
            
            # For append mode, use _insert_with_casting to handle NULL UUIDs and type conversions correctly
            # This ensures proper type casting for all columns, including NULL handling
            if mode == "append":
                # Get primary key for filtering existing rows
                # Try to get from config first, then from table constraints
                if not primary_key:
                    constraints = self._get_unique_constraints(destination)
                    if constraints:
                        # Use primary key if available, otherwise use first unique constraint
                        pk_constraint = next((c for c in constraints if c["type"] == "p"), None)
                        if pk_constraint:
                            primary_key = pk_constraint["columns"]
                return self._insert_with_casting(df, destination, batch_size, primary_key=primary_key, **kwargs)
            
            # For overwrite, ignore, error modes, use standard JDBC write
            # (These modes are less common and may not need explicit casting)
            jdbc_options = {
                "url": self.jdbc_url,
                "dbtable": destination,
                "batchsize": str(batch_size),
                **self.jdbc_properties,
                **kwargs
            }
            
            df.write.format("jdbc").mode(mode).options(**jdbc_options).save()
            
            self.logger.info(f"Successfully loaded data to PostgreSQL table: {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to PostgreSQL: {e}", exc_info=True)
            return False
    
    def _cast_columns_for_postgresql(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Cast columns to proper types for PostgreSQL compatibility.
        
        With ?stringtype=unspecified in the JDBC URL, PostgreSQL automatically converts:
        - StringType UUID strings → UUID type
        - StringType ISO timestamp strings (with offsets) → timestamp with time zone
        - StringType date strings → date type
        
        Therefore, we keep everything as StringType and let PostgreSQL handle the conversion.
        This preserves original time zone offsets for timestamps.
        
        Args:
            df: DataFrame to cast
            table_name: Table name (for logging)
            
        Returns:
            DataFrame (unchanged - all columns remain as StringType for auto-conversion)
        """
        # With stringtype=unspecified in JDBC URL, PostgreSQL automatically converts:
        # - UUID strings → UUID type
        # - ISO timestamp strings → timestamp with time zone (preserves offsets)
        # - Date strings → date type
        
        # No casting needed - keep everything as StringType
        # PostgreSQL will infer and convert types based on column definitions
        
        self.logger.debug(f"Keeping all columns as StringType for PostgreSQL auto-conversion (table: {table_name})")
        return df
    
    def _get_table_columns(self, table_name: str) -> set:
        """
        Get column names from a PostgreSQL table.
        
        Args:
            table_name: Table name (can include schema.table)
            
        Returns:
            Set of column names (empty set if unable to retrieve)
        """
        try:
            # Extract schema and table name
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name
            
            # Try reading the table with LIMIT 0 to get schema (simpler and more reliable)
            try:
                jdbc_options_schema = {
                    "url": self.jdbc_url,
                    "dbtable": f"(SELECT * FROM {table_name} LIMIT 0) AS schema_query",
                    **self.jdbc_properties
                }
                schema_df = self.spark.read.format("jdbc").options(**jdbc_options_schema).load()
                columns = set(schema_df.columns)
                self.logger.debug(f"Table {table_name} has columns: {sorted(columns)}")
                return columns
            except Exception as schema_error:
                # If that fails, try querying information_schema
                self.logger.debug(f"Schema method failed for {table_name}, trying information_schema: {schema_error}")
                try:
                    # Query information_schema - use dbtable with subquery
                    query = f"(SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position) AS columns_query"
                    jdbc_options = {
                        "url": self.jdbc_url,
                        "dbtable": query,
                        **self.jdbc_properties
                    }
                    columns_df = self.spark.read.format("jdbc").options(**jdbc_options).load()
                    if columns_df.count() > 0:
                        columns = [row["column_name"] for row in columns_df.collect()]
                        self.logger.debug(f"Table {table_name} has columns: {columns}")
                        return set(columns)
                    else:
                        self.logger.warning(f"Table {table_name} not found or has no columns")
                        return set()
                except Exception as query_error:
                    self.logger.warning(f"Could not get columns for table {table_name}: {query_error}")
                    return set()
                
        except Exception as e:
            self.logger.warning(f"Could not get columns for table {table_name}: {e}. Will use DataFrame columns only")
            return set()
    
    def _get_primary_key_column(self, table_name: str) -> str:
        """
        Get the primary key column name for a table.
        
        Uses convention: {table_name}_id for most tables.
        Special cases: users -> user_id, companies -> company_id, brands -> brand_id, etc.
        
        Args:
            table_name: Table name
            
        Returns:
            Primary key column name
        """
        # Special cases
        special_cases = {
            "users": "user_id",
            "companies": "company_id",
            "brands": "brand_id",
            "cities": "city_id",
            "countries": "country_code",  # countries uses country_code as PK
            "categories": "category_id",
            "device_types": "device_type_id",
            "operating_systems": "os_id",
            "browsers": "browser_id",
            "security_roles": "role_id",
            "security_permissions": "permission_id",
            "security_api_keys": "api_key_id",
            "customer_user_segments": "segment_id",
            "data_quality_rules": "rule_id",
            "configuration_feature_flags": "flag_id",
            "configuration_system_settings": "setting_id",
            "ml_model_registry": "model_id",
            "editorial_article_content": "content_id",  # Uses content_id, not editorial_article_content_id
            "editorial_articles": "article_id",  # Uses article_id, not editorial_article_id
            "editorial_authors": "author_id",  # Uses author_id, not editorial_author_id
        }
        
        if table_name in special_cases:
            return special_cases[table_name]
        
        # Default convention: {table_name}_id
        # Convert table_name to singular and add _id
        # Simple approach: just add _id
        return f"{table_name.rstrip('s')}_id" if table_name.endswith('s') else f"{table_name}_id"
    
    def _map_foreign_keys(self, df: DataFrame, destination: str) -> DataFrame:
        """
        Map foreign key UUIDs from raw database to production database.
        
        When extracting from raw and loading to production, UUIDs don't match.
        We need to map foreign keys based on business keys (e.g., company_code, brand_code).
        
        Args:
            df: DataFrame with foreign key columns that need mapping
            destination: Destination table name
            
        Returns:
            DataFrame with foreign keys mapped to production UUIDs
        """
        # Foreign key mappings: (fk_column, referenced_table, business_key_column)
        # This maps foreign key columns to their referenced tables and business keys
        fk_mappings = {
            "brands": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
            ],
            "brand_countries": [
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "categories": [
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("parent_category_id", "categories", "category_code"),  # Map parent_category_id based on category_code (self-reference, composite key with brand_id, not fully supported)
            ],
            "user_accounts": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "customer_events": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "security_role_permissions": [
                ("role_id", "security_roles", "role_code"),  # Map role_id based on role_code
                ("permission_id", "security_permissions", "permission_code"),  # Map permission_id based on permission_code
                ("granted_by", "users", "email"),  # Map granted_by based on email
            ],
            "security_user_roles": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("role_id", "security_roles", "role_code"),  # Map role_id based on role_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("granted_by", "users", "email"),  # Map granted_by based on email
            ],
            "editorial_authors": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional, can be NULL)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("primary_city_id", "cities", "city_name"),  # Map primary_city_id based on city_name (composite key with country_code, not fully supported)
            ],
            "editorial_articles": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("primary_author_id", "editorial_authors", "author_code"),  # Map primary_author_id based on author_code (composite key with brand_id, not fully supported)
                ("primary_category_id", "categories", "category_code"),  # Map primary_category_id based on category_code (composite key with brand_id, not fully supported)
            ],
            "company_departments": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("parent_department_id", "company_departments", "department_code"),  # Map parent_department_id based on department_code (self-reference, composite key with company_id, not fully supported)
                ("primary_city_id", "cities", "city_name"),  # Map primary_city_id based on city_name (composite key with country_code, not fully supported)
            ],
            "company_employees": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("department_id", "company_departments", "department_code"),  # Map department_id based on department_code (composite key with company_id, not fully supported)
                ("manager_id", "company_employees", "employee_number"),  # Map manager_id based on employee_number (self-reference)
            ],
            "customer_sessions": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("city_id", "cities", "city_name"),  # Map city_id based on city_name (composite key with country_code, not fully supported)
                ("device_type_id", "device_types", "device_type_code"),  # Map device_type_id based on device_type_code
                ("os_id", "operating_systems", "os_code"),  # Map os_id based on os_code
                ("browser_id", "browsers", "browser_code"),  # Map browser_id based on browser_code
            ],
            "customer_events": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key with brand_id, not fully supported)
                ("city_id", "cities", "city_name"),  # Map city_id based on city_name (composite key with country_code, not fully supported)
                ("device_type_id", "device_types", "device_type_code"),  # Map device_type_id based on device_type_code
                ("os_id", "operating_systems", "os_code"),  # Map os_id based on os_code
                ("browser_id", "browsers", "browser_code"),  # Map browser_id based on browser_code
            ],
            "customer_user_features": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("preferred_brand_id", "brands", "brand_code"),  # Map preferred_brand_id based on brand_code
                ("preferred_category_id", "categories", "category_code"),  # Map preferred_category_id based on category_code (composite key with brand_id, not fully supported)
                ("primary_city_id", "cities", "city_name"),  # Map primary_city_id based on city_name (composite key with country_code, not fully supported)
                ("preferred_device_type_id", "device_types", "device_type_code"),  # Map preferred_device_type_id based on device_type_code
                ("preferred_os_id", "operating_systems", "os_code"),  # Map preferred_os_id based on os_code
                ("preferred_browser_id", "browsers", "browser_code"),  # Map preferred_browser_id based on browser_code
            ],
            "customer_user_segments": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "customer_user_segment_assignments": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("segment_id", "customer_user_segments", "segment_code"),  # Map segment_id based on segment_code
            ],
            "customer_churn_predictions": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "customer_recommendations": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key with brand_id, not fully supported)
            ],
            "customer_conversion_predictions": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "security_api_keys": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("created_by", "users", "email"),  # Map created_by based on email
            ],
            "security_api_key_usage": [
                ("api_key_id", "security_api_keys", "api_key_hash"),  # Map api_key_id based on api_key_hash
            ],
            "security_user_sessions": [
                ("user_id", "users", "email"),  # Map user_id based on email
            ],
            "compliance_user_consent": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "compliance_data_subject_requests": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("rejected_by", "users", "email"),  # Map rejected_by based on email
            ],
            "compliance_retention_policies": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("created_by", "users", "email"),  # Map created_by based on email
            ],
            "compliance_retention_executions": [
                ("policy_id", "compliance_retention_policies", "policy_id"),  # policy_id is UUID, but we can map if there's a business key
            ],
            "compliance_anonymized_data": [
                ("original_user_id", "users", "email"),  # Map original_user_id based on email (optional)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("anonymized_by", "users", "email"),  # Map anonymized_by based on email
            ],
            "compliance_privacy_assessments": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("assessed_by", "users", "email"),  # Map assessed_by based on email
                ("reviewed_by", "users", "email"),  # Map reviewed_by based on email
                ("approved_by", "users", "email"),  # Map approved_by based on email
            ],
            "compliance_breach_incidents": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("discovered_by", "users", "email"),  # Map discovered_by based on email
                ("investigated_by", "users", "email"),  # Map investigated_by based on email
                ("resolved_by", "users", "email"),  # Map resolved_by based on email
            ],
            "editorial_article_performance": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "editorial_author_performance": [
                ("author_id", "editorial_authors", "author_code"),  # Map author_id based on author_code (composite key, not fully supported)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("top_category_id", "categories", "category_code"),  # Map top_category_id based on category_code (composite key with brand_id, not fully supported)
            ],
            "editorial_category_performance": [
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key, not fully supported)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "editorial_content_events": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key with brand_id, not fully supported)
                ("author_id", "editorial_authors", "author_code"),  # Map author_id based on author_code (composite key with brand_id, not fully supported)
                ("city_id", "cities", "city_name"),  # Map city_id based on city_name (composite key with country_code, not fully supported)
                ("device_type_id", "device_types", "device_type_code"),  # Map device_type_id based on device_type_code
                ("os_id", "operating_systems", "os_code"),  # Map os_id based on os_code
                ("browser_id", "browsers", "browser_code"),  # Map browser_id based on browser_code
            ],
            "editorial_headline_tests": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "editorial_trending_topics": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "editorial_content_recommendations": [
                ("user_id", "users", "email"),  # Map user_id based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("recommended_category_id", "categories", "category_code"),  # Map recommended_category_id based on category_code (composite key with brand_id, not fully supported)
                ("recommended_author_id", "editorial_authors", "author_code"),  # Map recommended_author_id based on author_code (composite key with brand_id, not fully supported)
            ],
            "editorial_article_content": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("version_created_by", "users", "email"),  # Map version_created_by based on email
                # Note: previous_version_id is a self-referencing FK that references content_id
                # Since content_id doesn't have a business key, we can't map it using the standard approach
                # It will be set to NULL if it can't be mapped (since it's nullable)
                # Special handling would be needed to map it based on (article_id, content_version) composite key
            ],
            "editorial_content_versions": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("content_id", "editorial_article_content", "content_id"),  # content_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("created_by", "users", "email"),  # Map created_by based on email
                ("reviewed_by", "users", "email"),  # Map reviewed_by based on email
                ("approved_by", "users", "email"),  # Map approved_by based on email
                ("published_by", "users", "email"),  # Map published_by based on email
            ],
            "editorial_media_assets": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("uploaded_by", "users", "email"),  # Map uploaded_by based on email
                ("author_id", "editorial_authors", "author_code"),  # Map author_id based on author_code (composite key with brand_id, not fully supported)
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key with brand_id, not fully supported)
            ],
            "editorial_media_variants": [
                ("media_id", "editorial_media_assets", "media_code"),  # Map media_id based on media_code
            ],
            "editorial_content_media": [
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("media_id", "editorial_media_assets", "media_code"),  # Map media_id based on media_code
                ("content_id", "editorial_article_content", "content_id"),  # content_id is UUID, no business key
            ],
            "editorial_media_collections": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("cover_media_id", "editorial_media_assets", "media_code"),  # Map cover_media_id based on media_code
                ("created_by", "users", "email"),  # Map created_by based on email
            ],
            "editorial_media_collection_items": [
                ("collection_id", "editorial_media_collections", "collection_id"),  # collection_id is UUID, no business key
                ("media_id", "editorial_media_assets", "media_code"),  # Map media_id based on media_code
            ],
            "editorial_media_usage": [
                ("media_id", "editorial_media_assets", "media_code"),  # Map media_id based on media_code
                ("article_id", "editorial_articles", "article_id"),  # article_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "company_internal_content": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("department_id", "company_departments", "department_code"),  # Map department_id based on department_code (composite key with company_id, not fully supported)
                ("author_employee_id", "company_employees", "employee_number"),  # Map author_employee_id based on employee_number
                ("author_department_id", "company_departments", "department_code"),  # Map author_department_id based on department_code (composite key with company_id, not fully supported)
                ("category_id", "categories", "category_code"),  # Map category_id based on category_code (composite key with brand_id, not fully supported)
            ],
            "company_content_performance": [
                ("content_id", "company_internal_content", "content_id"),  # content_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "company_department_performance": [
                ("department_id", "company_departments", "department_code"),  # Map department_id based on department_code (composite key, not fully supported)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "company_content_events": [
                ("content_id", "company_internal_content", "content_id"),  # content_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("employee_id", "company_employees", "employee_number"),  # Map employee_id based on employee_number
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("department_id", "company_departments", "department_code"),  # Map department_id based on department_code (composite key with company_id, not fully supported)
                ("city_id", "cities", "city_name"),  # Map city_id based on city_name (composite key with country_code, not fully supported)
                ("device_type_id", "device_types", "device_type_code"),  # Map device_type_id based on device_type_code
                ("os_id", "operating_systems", "os_code"),  # Map os_id based on os_code
                ("browser_id", "browsers", "browser_code"),  # Map browser_id based on browser_code
            ],
            "company_employee_engagement": [
                ("employee_id", "company_employees", "employee_number"),  # Map employee_id based on employee_number
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("department_id", "company_departments", "department_code"),  # Map department_id based on department_code (composite key with company_id, not fully supported)
            ],
            "company_communications_analytics": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("top_department_id", "company_departments", "department_code"),  # Map top_department_id based on department_code (composite key with company_id, not fully supported)
            ],
            "audit_logs": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("api_key_id", "security_api_keys", "api_key_hash"),  # Map api_key_id based on api_key_hash
                ("session_id", "security_user_sessions", "session_id"),  # session_id is UUID, no business key
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "audit_data_changes": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("api_key_id", "security_api_keys", "api_key_hash"),  # Map api_key_id based on api_key_hash
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "audit_data_access": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("api_key_id", "security_api_keys", "api_key_hash"),  # Map api_key_id based on api_key_hash
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "audit_security_events": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("session_id", "security_user_sessions", "session_id"),  # session_id is UUID, no business key
            ],
            "audit_data_lineage": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "audit_compliance_events": [
                ("user_id", "users", "email"),  # Map user_id based on email (optional)
                ("requested_by", "users", "email"),  # Map requested_by based on email
                ("processed_by", "users", "email"),  # Map processed_by based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "ml_model_registry": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("deployed_by", "users", "email"),  # Map deployed_by based on email
                ("created_by", "users", "email"),  # Map created_by based on email
                ("parent_model_id", "ml_model_registry", "model_code"),  # Map parent_model_id based on model_code (self-reference, composite key with model_version, not fully supported)
            ],
            "ml_model_features": [
                ("model_id", "ml_model_registry", "model_code"),  # Map model_id based on model_code
            ],
            "ml_model_training_runs": [
                ("model_id", "ml_model_registry", "model_code"),  # Map model_id based on model_code
                ("trained_by", "users", "email"),  # Map trained_by based on email
            ],
            "ml_model_predictions": [
                ("model_id", "ml_model_registry", "model_code"),  # Map model_id based on model_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "ml_model_monitoring": [
                ("model_id", "ml_model_registry", "model_code"),  # Map model_id based on model_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "ml_model_ab_tests": [
                ("model_a_id", "ml_model_registry", "model_code"),  # Map model_a_id based on model_code
                ("model_b_id", "ml_model_registry", "model_code"),  # Map model_b_id based on model_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("created_by", "users", "email"),  # Map created_by based on email
                ("winning_model_id", "ml_model_registry", "model_code"),  # Map winning_model_id based on model_code
            ],
            "data_quality_rules": [
                ("created_by", "users", "email"),  # Map created_by based on email
            ],
            "data_quality_checks": [
                ("rule_id", "data_quality_rules", "rule_code"),  # Map rule_id based on rule_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "data_quality_metrics": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "data_quality_alerts": [
                ("rule_id", "data_quality_rules", "rule_code"),  # Map rule_id based on rule_code
                ("acknowledged_by", "users", "email"),  # Map acknowledged_by based on email
                ("resolved_by", "users", "email"),  # Map resolved_by based on email
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "data_validation_results": [
                ("rule_id", "data_quality_rules", "rule_code"),  # Map rule_id based on rule_code
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
            ],
            "configuration_feature_flags": [
                ("created_by", "users", "email"),  # Map created_by based on email
                ("updated_by", "users", "email"),  # Map updated_by based on email
            ],
            "configuration_feature_flag_history": [
                ("flag_id", "configuration_feature_flags", "flag_code"),  # Map flag_id based on flag_code
                ("changed_by", "users", "email"),  # Map changed_by based on email
            ],
            "configuration_system_settings": [
                ("company_id", "companies", "company_code"),  # Map company_id based on company_code
                ("brand_id", "brands", "brand_code"),  # Map brand_id based on brand_code
                ("created_by", "users", "email"),  # Map created_by based on email
                ("updated_by", "users", "email"),  # Map updated_by based on email
            ],
            "configuration_system_settings_history": [
                ("setting_id", "configuration_system_settings", "setting_code"),  # Map setting_id based on setting_code
                ("changed_by", "users", "email"),  # Map changed_by based on email
            ],
            # Note: Composite key mappings (e.g., author_code + brand_id, category_code + brand_id, 
            # department_code + company_id) are not yet supported and will need special handling
            # Note: country_code is already a business key (CHAR(2)), no mapping needed
            # Note: UUID primary keys without business keys (e.g., article_id, session_id, account_id, 
            # content_id, collection_id) don't need mapping - they are already UUIDs
            # Note: Some foreign keys reference tables that don't have single-column business keys
            # (e.g., policy_id, segment_id) - these may need special handling
        }
        
        # Get mappings for this table
        mappings = fk_mappings.get(destination, [])
        if not mappings:
            return df  # No foreign keys to map
        
        mapped_df = df
        
        for fk_column, ref_table, business_key in mappings:
            # Check if the foreign key column exists in the DataFrame
            if fk_column not in df.columns:
                continue
            
            try:
                # Read raw table to get raw UUID -> business_key mapping
                raw_url = self.etl_config.get_postgresql_url(use_raw=True)
                raw_props = self.etl_config.get_postgresql_properties()
                
                # Get the primary key column name for the referenced table
                ref_pk_column = self._get_primary_key_column(ref_table)
                
                # Read raw referenced table to get raw UUID -> business_key mapping
                # Use the primary key column, not the foreign key column
                raw_query = f'SELECT "{ref_pk_column}" as raw_id, "{business_key}" as business_key FROM {ref_table}'
                raw_mapping_df = self.spark.read.format("jdbc").options(
                    url=raw_url,
                    query=raw_query,
                    **raw_props
                ).load()
                
                # Read production table to get production UUID -> business_key mapping
                # Use the primary key column, not the foreign key column
                prod_query = f'SELECT "{ref_pk_column}" as prod_id, "{business_key}" as business_key FROM {ref_table}'
                prod_mapping_df = self.spark.read.format("jdbc").options(
                    url=self.jdbc_url,
                    query=prod_query,
                    **self.jdbc_properties
                ).load()
                
                # Join raw and production mappings on business_key to create UUID mapping
                # raw_id -> prod_id mapping
                uuid_mapping_df = raw_mapping_df.join(
                    prod_mapping_df,
                    on="business_key",
                    how="inner"
                ).select("raw_id", "prod_id")
                
                if uuid_mapping_df.count() == 0:
                    self.logger.warning(f"No UUID mapping found for {fk_column} in {destination}. Skipping mapping.")
                    continue
                
                # Now join with the main DataFrame to replace raw UUIDs with production UUIDs
                # We need to join on the foreign key column
                mapped_df = mapped_df.join(
                    uuid_mapping_df,
                    mapped_df[fk_column] == uuid_mapping_df["raw_id"],
                    how="left"
                ).withColumn(
                    fk_column,
                    when(col("prod_id").isNotNull(), col("prod_id")).otherwise(col(fk_column))
                ).drop("raw_id", "prod_id")
                
                # Count how many rows were actually mapped
                # We need to compare the original df with the mapped_df
                # But we can't directly compare DataFrames, so we'll just log that mapping was attempted
                self.logger.info(f"Attempted to map {fk_column} foreign keys for {destination} based on {business_key}")
                
            except Exception as e:
                self.logger.warning(f"Could not map foreign key {fk_column} for {destination}: {e}. Continuing with original UUIDs.")
                continue
        
        # Special handling for foreign keys that use composite keys
        # For editorial_article_content.previous_version_id, map based on (article_id, content_version)
        if destination == "editorial_article_content" and "previous_version_id" in mapped_df.columns:
            try:
                # Read raw table to get raw content_id -> (article_id, content_version) mapping
                raw_url = self.etl_config.get_postgresql_url(use_raw=True)
                raw_props = self.etl_config.get_postgresql_properties()
                
                raw_query = 'SELECT content_id as raw_id, article_id, content_version FROM editorial_article_content'
                raw_mapping_df = self.spark.read.format("jdbc").options(
                    url=raw_url,
                    query=raw_query,
                    **raw_props
                ).load()
                
                # Read production table to get production content_id -> (article_id, content_version) mapping
                prod_query = 'SELECT content_id as prod_id, article_id, content_version FROM editorial_article_content'
                prod_mapping_df = self.spark.read.format("jdbc").options(
                    url=self.jdbc_url,
                    query=prod_query,
                    **self.jdbc_properties
                ).load()
                
                # Join raw and production mappings on (article_id, content_version) to create UUID mapping
                uuid_mapping_df = raw_mapping_df.join(
                    prod_mapping_df,
                    on=["article_id", "content_version"],
                    how="inner"
                ).select("raw_id", "prod_id")
                
                if uuid_mapping_df.count() > 0:
                    # Join with the main DataFrame to replace raw UUIDs with production UUIDs
                    mapped_df = mapped_df.join(
                        uuid_mapping_df,
                        mapped_df["previous_version_id"] == uuid_mapping_df["raw_id"],
                        how="left"
                    ).withColumn(
                        "previous_version_id",
                        when(col("prod_id").isNotNull(), col("prod_id")).otherwise(None)
                    ).drop("raw_id", "prod_id")
                    
                    # Verify that all mapped previous_version_id values exist in production
                    # Note: We only check production, not current batch, because PostgreSQL
                    # checks foreign key constraints during INSERT, and we can't guarantee
                    # insertion order. If previous_version_id points to a row in the same batch,
                    # it will cause a foreign key violation. So we only keep previous_version_id
                    # if it points to a row that already exists in production.
                    prod_content_ids_query = 'SELECT content_id FROM editorial_article_content'
                    prod_content_ids_df = self.spark.read.format("jdbc").options(
                        url=self.jdbc_url,
                        query=prod_content_ids_query,
                        **self.jdbc_properties
                    ).load()
                    
                    # Join to verify previous_version_id exists in production
                    verified_df = mapped_df.join(
                        prod_content_ids_df.select(col("content_id").alias("verified_prev_id")),
                        mapped_df["previous_version_id"] == prod_content_ids_df["content_id"],
                        how="left"
                    )
                    
                    # Set previous_version_id to NULL if it doesn't exist in production
                    mapped_df = verified_df.withColumn(
                        "previous_version_id",
                        when(
                            (col("previous_version_id").isNotNull()) & 
                            col("verified_prev_id").isNotNull(),
                            col("previous_version_id")
                        ).otherwise(None)
                    ).drop("verified_prev_id")
                    
                    mapped_count = mapped_df.filter(col("previous_version_id").isNotNull()).count()
                    self.logger.info(f"Mapped {mapped_count} previous_version_id foreign keys for {destination} based on (article_id, content_version) (only keeping references to existing production rows to avoid FK violations)")
                else:
                    self.logger.warning(f"No UUID mapping found for previous_version_id in {destination}. Setting ALL to NULL to avoid FK violations.")
                    # Set ALL previous_version_id to NULL since we can't map them
                    # This prevents foreign key violations from unmapped raw UUIDs
                    # Cast lit(None) to StringType to preserve the column type (lit(None) creates void type that Spark can't write to JDBC)
                    # UUID columns are typically stored as strings in Spark DataFrames
                    mapped_df = mapped_df.withColumn(
                        "previous_version_id",
                        lit(None).cast(StringType()))
            except Exception as e:
                self.logger.warning(f"Could not map previous_version_id for {destination}: {e}. Setting ALL to NULL to avoid FK violations.")
                # Set ALL previous_version_id to NULL since we can't map them
                # This prevents foreign key violations from unmapped raw UUIDs
                # Cast lit(None) to StringType to preserve the column type (lit(None) creates void type that Spark can't write to JDBC)
                # UUID columns are typically stored as strings in Spark DataFrames
                mapped_df = mapped_df.withColumn(
                    "previous_version_id",
                    lit(None).cast(StringType()))
        
        # Special handling for editorial_content_versions
        # The relationship:
        # - editorial_article_content has UNIQUE (article_id, content_version)
        # - editorial_content_versions has UNIQUE (article_id, version_number)
        # - version_number in editorial_content_versions should match content_version in editorial_article_content
        # - content_id in editorial_content_versions references editorial_article_content.content_id
        #
        # The challenge: article_id cannot be mapped directly (no business key in editorial_articles)
        # Solution: Use raw article_id from editorial_content_versions to find raw content_id from editorial_article_content
        # using (raw_article_id, version_number) = (raw_article_id, content_version), then use that raw content_id's
        # (brand_id, content_version) to find production content_id. Since brand_id is already mapped, we can use
        # (mapped_brand_id, content_version) to find production content_id.
        #
        # Steps:
        # 1. Read raw editorial_article_content to get raw content_id -> (raw_article_id, brand_id, content_version)
        # 2. Match raw editorial_content_versions with raw editorial_article_content on (raw_article_id, version_number) = (raw_article_id, content_version)
        # 3. This gives us raw content_id and (raw_brand_id, content_version)
        # 4. Read production editorial_article_content to get production content_id -> (brand_id, content_version, article_id)
        # 5. Match on (mapped_brand_id, content_version) to get production content_id
        # 6. Extract production article_id from the matched production content_id
        if destination == "editorial_content_versions" and "content_id" in mapped_df.columns and "version_number" in mapped_df.columns and "article_id" in mapped_df.columns:
            try:
                # Step 1: Read raw editorial_article_content to get raw content_id -> (raw_article_id, brand_id, content_version)
                # We need to read from the raw database
                raw_jdbc_url = self.etl_config.get_postgresql_url(use_raw=True)
                raw_jdbc_properties = self.etl_config.get_postgresql_properties()
                
                if not raw_jdbc_url:
                    self.logger.warning(f"Could not get raw database connection for {destination}. Skipping content_id mapping.")
                else:
                    # Read raw editorial_article_content
                    raw_query = 'SELECT content_id as raw_content_id, article_id as raw_article_id, brand_id as raw_brand_id, content_version as raw_content_version FROM editorial_article_content'
                    raw_content_df = self.spark.read.format("jdbc").options(
                        url=raw_jdbc_url,
                        query=raw_query,
                        **raw_jdbc_properties
                    ).load()
                    
                    # Step 2: Match raw editorial_content_versions with raw editorial_article_content
                    # on (raw_article_id, version_number) = (raw_article_id, content_version)
                    # We need the raw article_id from mapped_df (before it gets mapped)
                    # Actually, mapped_df already has article_id, but it's the raw article_id at this point
                    raw_content_select = raw_content_df.select(
                        col("raw_content_id").alias("raw_content_id_matched"),
                        col("raw_article_id").alias("raw_article_id_match"),
                        col("raw_brand_id").alias("raw_brand_id_match"),
                        col("raw_content_version").alias("raw_content_version_match")
                    )
                    
                    # Join on raw article_id and version_number = content_version
                    mapped_df = mapped_df.join(
                        raw_content_select,
                        (mapped_df["article_id"] == raw_content_select["raw_article_id_match"]) &
                        (mapped_df["version_number"] == raw_content_select["raw_content_version_match"]),
                        how="left"
                    )
                    
                    # Step 3: Find production article_id first by matching raw and production articles
                    # Since editorial_articles doesn't have a business key, we'll use (company_id, brand_id, title, content_url)
                    # to match articles. This is more specific than just (brand_id, title, content_url).
                    # We need to join editorial_content_versions with editorial_articles to get
                    # title and content_url, but we're in the mapping phase, so we need to read from the raw database.
                    
                    # Read raw editorial_articles to get raw article_id -> (company_id, brand_id, title, content_url)
                    raw_articles_query = 'SELECT article_id as raw_article_id, company_id as raw_company_id, brand_id as raw_brand_id, title as raw_title, content_url as raw_content_url FROM editorial_articles'
                    raw_articles_df = self.spark.read.format("jdbc").options(
                        url=raw_jdbc_url,
                        query=raw_articles_query,
                        **raw_jdbc_properties
                    ).load()
                    
                    # Join mapped_df with raw_articles_df to get title and content_url
                    # Use fully qualified column references to avoid ambiguity
                    raw_articles_select = raw_articles_df.select(
                        col("raw_article_id").alias("raw_article_id_for_join"),
                        col("raw_company_id").alias("raw_company_id"),
                        col("raw_title").alias("raw_title"),
                        col("raw_content_url").alias("raw_content_url")
                    )
                    mapped_df = mapped_df.join(
                        raw_articles_select,
                        mapped_df["article_id"] == raw_articles_select["raw_article_id_for_join"],
                        how="left"
                    ).drop("raw_article_id_for_join")
                    
                    # Read production editorial_articles to get production article_id -> (company_id, brand_id, title, content_url)
                    prod_articles_query = 'SELECT article_id as prod_article_id, company_id, brand_id, title, content_url FROM editorial_articles'
                    prod_articles_df = self.spark.read.format("jdbc").options(
                        url=self.jdbc_url,
                        query=prod_articles_query,
                        **self.jdbc_properties
                    ).load()
                    
                    # Match on (company_id, brand_id, title, content_url) to get production article_id
                    # Use the mapped company_id and brand_id (which are already mapped) and raw_title, raw_content_url
                    prod_articles_select = prod_articles_df.select(
                        col("prod_article_id").alias("prod_article_id_mapped"),
                        col("company_id").alias("prod_company_id"),
                        col("brand_id").alias("prod_brand_id"),
                        col("title").alias("prod_title"),
                        col("content_url").alias("prod_content_url")
                    )
                    
                    # Join on mapped company_id, mapped brand_id, raw_title = prod_title, and raw_content_url = prod_content_url
                    mapped_df = mapped_df.join(
                        prod_articles_select,
                        (mapped_df["company_id"] == prod_articles_select["prod_company_id"]) &
                        (mapped_df["brand_id"] == prod_articles_select["prod_brand_id"]) &
                        (mapped_df["raw_title"] == prod_articles_select["prod_title"]) &
                        (mapped_df["raw_content_url"] == prod_articles_select["prod_content_url"]),
                        how="left"
                    )
                    
                    # Update article_id with production article_id
                    mapped_df = mapped_df.withColumn(
                        "article_id",
                        when(col("prod_article_id_mapped").isNotNull(), col("prod_article_id_mapped")).otherwise(col("article_id"))
                    )
                    
                    # Step 4: Now use (production article_id, content_version) to find production content_id
                    # Since (article_id, content_version) is unique in editorial_article_content, this should be unique
                    prod_query = 'SELECT content_id as prod_content_id, article_id, content_version FROM editorial_article_content'
                    prod_content_df = self.spark.read.format("jdbc").options(
                        url=self.jdbc_url,
                        query=prod_query,
                        **self.jdbc_properties
                    ).load()
                    
                    # Match on (production article_id, content_version) = (article_id, content_version)
                    prod_content_select = prod_content_df.select(
                        col("prod_content_id").alias("prod_content_id_mapped"),
                        col("article_id").alias("prod_article_id_for_content"),
                        col("content_version").alias("prod_content_version")
                    )
                    
                    # Join on production article_id and raw_content_version_match = prod_content_version
                    mapped_df = mapped_df.join(
                        prod_content_select,
                        (mapped_df["article_id"] == prod_content_select["prod_article_id_for_content"]) &
                        (mapped_df["raw_content_version_match"] == prod_content_select["prod_content_version"]),
                        how="left"
                    )
                    
                    # Verify that the mapped content_id actually exists in production editorial_article_content
                    # Read production editorial_article_content to verify content_id exists
                    verify_query = 'SELECT content_id as valid_content_id FROM editorial_article_content'
                    verify_content_df = self.spark.read.format("jdbc").options(
                        url=self.jdbc_url,
                        query=verify_query,
                        **self.jdbc_properties
                    ).load()
                    
                    # Join with verification DataFrame to check if content_id exists
                    # Only keep rows where prod_content_id_mapped exists in production
                    mapped_df = mapped_df.join(
                        verify_content_df.select(col("valid_content_id")),
                        mapped_df["prod_content_id_mapped"] == verify_content_df["valid_content_id"],
                        how="left"
                    )
                    
                    # Update content_id with production content_id, but only if it exists in production
                    mapped_df = mapped_df.withColumn(
                        "content_id",
                        when(
                            col("prod_content_id_mapped").isNotNull() & 
                            col("valid_content_id").isNotNull(),
                            col("prod_content_id_mapped")
                        ).otherwise(None)
                    ).drop("valid_content_id")
                    
                    # Drop temporary columns
                    mapped_df = mapped_df.drop(
                        "raw_content_id_matched", "raw_article_id_match", "raw_brand_id_match", "raw_content_version_match",
                        "raw_company_id", "raw_title", "raw_content_url",
                        "prod_article_id_mapped", "prod_company_id", "prod_brand_id", "prod_title", "prod_content_url",
                        "prod_content_id_mapped", "prod_article_id_for_content", "prod_content_version"
                    )
                    
                    mapped_content_count = mapped_df.filter(col("content_id").isNotNull()).count()
                    mapped_article_count = mapped_df.filter(col("article_id").isNotNull()).count()
                    
                    if mapped_content_count > 0:
                        self.logger.info(f"Mapped {mapped_content_count} content_id foreign keys for {destination} using raw article_id -> raw content_id -> production content_id (verified in production)")
                    if mapped_article_count > 0:
                        self.logger.info(f"Mapped {mapped_article_count} article_id foreign keys for {destination} from mapped content_id")
                    
                    if mapped_content_count == 0:
                        self.logger.warning(f"No valid UUID mapping found for content_id in {destination} (mapped content_id doesn't exist in production). Setting to NULL where unmapped.")
                        mapped_df = mapped_df.withColumn(
                            "content_id",
                            when(col("content_id").isNotNull(), col("content_id")).otherwise(None)
                        )
                    else:
                        # Log warning if some content_ids couldn't be mapped
                        total_count = mapped_df.count()
                        unmapped_count = total_count - mapped_content_count
                        if unmapped_count > 0:
                            self.logger.warning(f"{unmapped_count} out of {total_count} content_id foreign keys for {destination} could not be mapped (mapped content_id doesn't exist in production). Setting to NULL.")
            except Exception as e:
                self.logger.warning(f"Could not map content_id and article_id for {destination}: {e}. Setting to NULL where unmapped.")
                # Set unmapped content_id to NULL (since it's nullable)
                mapped_df = mapped_df.withColumn(
                    "content_id",
                    when(col("content_id").isNotNull(), col("content_id")).otherwise(None)
                )
        
        return mapped_df
    
    def _get_table_columns_with_types(self, table_name: str) -> Dict[str, str]:
        """
        Get column names and types from a PostgreSQL table.
        
        Args:
            table_name: Table name (can include schema.table)
            
        Returns:
            Dictionary mapping column names to their PostgreSQL types
        """
        try:
            # Extract schema and table name
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name
            
            # Query information_schema to get column types
            query = f"""
                (SELECT column_name, data_type, udt_name
                 FROM information_schema.columns
                 WHERE table_schema = '{schema}' AND table_name = '{table}'
                 ORDER BY ordinal_position) AS columns_query
            """
            jdbc_options = {
                "url": self.jdbc_url,
                "dbtable": query,
                **self.jdbc_properties
            }
            columns_df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            
            if columns_df.count() > 0:
                columns_info = {}
                for row in columns_df.collect():
                    col_name = row["column_name"]
                    data_type = row["data_type"]
                    udt_name = row["udt_name"]
                    
                    # Handle array types: data_type is "ARRAY", udt_name is like "_text" or "_varchar"
                    if data_type == "ARRAY":
                        # Convert udt_name from "_text" to "text[]"
                        if udt_name.startswith("_"):
                            base_type = udt_name[1:]  # Remove underscore
                            col_type = f"{base_type}[]"
                        else:
                            # Fallback: use udt_name as-is with []
                            col_type = f"{udt_name}[]"
                    # Use udt_name for UUID (it will be 'uuid')
                    elif udt_name == "uuid":
                        col_type = "uuid"
                    # Use data_type for other types
                    else:
                        col_type = data_type
                    
                    columns_info[col_name] = col_type
                self.logger.debug(f"Table {table_name} column types: {columns_info}")
                return columns_info
            else:
                self.logger.warning(f"Table {table_name} not found or has no columns")
                return {}
        except Exception as e:
            self.logger.warning(f"Could not get column types for table {table_name}: {e}")
            return {}
    
    def _get_unique_constraints(self, table_name: str) -> List[Dict]:
        """
        Get all unique constraints (including primary key) from a PostgreSQL table.
        
        Args:
            table_name: Table name (can include schema.table)
            
        Returns:
            List of dicts with structure:
            [
                {"name": "operating_systems_pkey", "columns": ["os_id"], "type": "p"},
                {"name": "operating_systems_os_code_key", "columns": ["os_code"], "type": "u"}
            ]
            Ordered: primary key first, then unique constraints.
        """
        try:
            # Use psycopg2 directly since Spark JDBC can't handle ARRAY types
            import psycopg2
            from urllib.parse import urlparse, parse_qs
            
            # Parse JDBC URL to get connection parameters
            # Format: jdbc:postgresql://host:port/database?user=...&password=...
            jdbc_url = self.jdbc_url.replace("jdbc:postgresql://", "")
            if "?" in jdbc_url:
                url_part, query_part = jdbc_url.split("?", 1)
                params = parse_qs(query_part)
            else:
                url_part = jdbc_url
                params = {}
            
            # Parse host:port/database
            if "/" in url_part:
                host_port, database = url_part.split("/", 1)
            else:
                host_port = url_part
                database = params.get("database", [None])[0]
            
            if ":" in host_port:
                host, port = host_port.split(":", 1)
            else:
                host = host_port
                port = "5432"
            
            # Get connection parameters
            user = params.get("user", [None])[0] or self.jdbc_properties.get("user")
            password = params.get("password", [None])[0] or self.jdbc_properties.get("password")
            
            # Extract schema and table name
            if "." in table_name:
                schema, table = table_name.split(".", 1)
            else:
                schema = "public"
                table = table_name
            
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=host,
                port=int(port),
                database=database,
                user=user,
                password=password
            )
            cursor = conn.cursor()
            
            # Query pg_constraint to get all unique constraints
            # This includes primary keys and unique constraints
            # CRITICAL FIX: Order by con.contype = 'p' DESC to get PRIMARY KEY FIRST
            query = """
                SELECT 
                    con.conname AS constraint_name,
                    con.contype,
                    array_agg(att.attname ORDER BY u.ord) AS columns
                 FROM pg_constraint con
                 JOIN pg_class rel ON rel.oid = con.conrelid
                 JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                 JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord) ON true
                 JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = u.attnum
                 WHERE nsp.nspname = %s
                   AND rel.relname = %s
                   AND con.contype IN ('p', 'u')  -- 'p' = primary key, 'u' = unique constraint
                 GROUP BY con.conname, con.contype
                 ORDER BY 
                     con.contype = 'p' DESC,  -- Primary key FIRST (TRUE > FALSE)
                     con.conname
            """
            cursor.execute(query, (schema, table))
            results = cursor.fetchall()
            
            self.logger.debug(f"Raw query results for {table_name}: {results}")
            
            constraints = []
            for row in results:
                constraint_name, ctype, columns_array = row
                self.logger.debug(f"Processing constraint: name={constraint_name}, type={ctype}, columns_array={columns_array}, type={type(columns_array)}")
                
                if columns_array:
                    # PostgreSQL array is returned as a list by psycopg2
                    if isinstance(columns_array, list):
                        columns = [str(col).strip().strip('"') for col in columns_array]
                    else:
                        # Fallback: parse as string if it's not a list
                        columns = [col.strip().strip('"') for col in str(columns_array).strip('{}').split(',')]
                    
                    constraint_dict = {
                        "name": constraint_name,
                        "type": "p" if ctype == 'p' else "u",
                        "columns": columns
                    }
                    self.logger.debug(f"Parsed constraint: {constraint_dict}")
                    constraints.append(constraint_dict)
            
            cursor.close()
            conn.close()
            
            self.logger.info(f"Table {table_name} unique constraints: {constraints}")
            if constraints:
                pk_constraints = [c for c in constraints if c["type"] == "p"]
                uq_constraints = [c for c in constraints if c["type"] == "u"]
                self.logger.info(f"  - Primary keys: {pk_constraints}")
                self.logger.info(f"  - Unique constraints: {uq_constraints}")
            else:
                self.logger.warning(f"No constraints found for table {table_name}")
            return constraints
        except ImportError:
            self.logger.warning("psycopg2 not installed. Cannot get unique constraints.")
            return []
        except Exception as e:
            self.logger.warning(f"Could not get unique constraints for table {table_name}: {e}")
            return []
    
    def _select_conflict_columns(self, df_columns: List[str], all_constraints: List[Dict], primary_key: Optional[List[str]]) -> tuple:
        """
        Select the best constraint for ON CONFLICT clause.
        Always prefers primary key, then unique constraints.
        
        Args:
            df_columns: List of column names in the DataFrame
            all_constraints: List of constraint dicts from _get_unique_constraints
            primary_key: Primary key columns from config
            
        Returns:
            Tuple of (conflict_columns, constraint_name, constraint_type)
        """
        df_cols_set = set(df_columns)
        
        # Debug logging
        self.logger.debug(f"Selecting conflict columns. DataFrame columns: {df_columns}")
        self.logger.debug(f"Available constraints: {all_constraints}")
        self.logger.debug(f"Primary key from config: {primary_key}")
        
        # 0. FIRST PRIORITY: Use primary key from config if it's in DataFrame
        # This is the most reliable approach - we trust the config
        if primary_key and all(col in df_cols_set for col in primary_key):
            # Try to find the matching constraint (PRIMARY KEY or UNIQUE) to get the constraint name
            for constr in all_constraints:
                if set(constr["columns"]) == set(primary_key):
                    # Found matching constraint (could be PRIMARY KEY or UNIQUE)
                    constraint_type_str = "PRIMARY KEY" if constr["type"] == "p" else "UNIQUE"
                    self.logger.info(f"✅ Using {constraint_type_str} from config: {primary_key} (constraint: {constr['name']})")
                    return primary_key, constr["name"], constr["type"]
            
            # If no matching constraint found, use primary key directly
            # This might happen if the constraint doesn't exist or wasn't discovered
            self.logger.warning(f"⚠️ Using primary key from config: {primary_key} (no matching constraint found - will use raw columns)")
            return primary_key, None, "p"
        
        # 2. Try to find PRIMARY KEY constraint from database
        for constr in all_constraints:
            if constr["type"] == "p":
                cols = constr["columns"]
                self.logger.debug(f"Checking PRIMARY KEY constraint: {cols}, columns in DataFrame: {[col in df_cols_set for col in cols]}")
                if all(col in df_cols_set for col in cols):
                    self.logger.info(f"✅ Using PRIMARY KEY from database: {cols} (constraint: {constr['name']})")
                    return cols, constr["name"], "p"
                else:
                    missing = [col for col in cols if col not in df_cols_set]
                    self.logger.warning(f"PRIMARY KEY constraint {cols} not usable - missing columns in DataFrame: {missing}")
        
        # 3. Fall back to UNIQUE constraints
        for constr in all_constraints:
            if constr["type"] == "u":
                cols = constr["columns"]
                self.logger.debug(f"Checking UNIQUE constraint: {cols}, columns in DataFrame: {[col in df_cols_set for col in cols]}")
                if all(col in df_cols_set for col in cols):
                    self.logger.warning(f"⚠️ Using UNIQUE constraint for ON CONFLICT: {cols} (constraint: {constr['name']}, no PK available)")
                    return cols, constr["name"], "u"
        
        # 3. Last resort: use primary_key list directly (if columns exist)
        if primary_key and all(col in df_cols_set for col in primary_key):
            self.logger.warning(f"Using raw primary key columns (no constraint match): {primary_key}")
            return primary_key, None, "p"
        
        # 4. If nothing works, raise error
        raise ValueError(f"No suitable constraint found for ON CONFLICT. DataFrame columns: {df_columns}, Constraints: {all_constraints}")
    
    def _upsert(
        self,
        df: DataFrame,
        destination: str,
        primary_key: Optional[List[str]],
        conflict_resolution: Optional[str],
        batch_size: int,
        **kwargs
    ) -> bool:
        """
        Perform upsert operation using temporary table and ON CONFLICT.
        
        Args:
            df: DataFrame to upsert
            destination: Target table name
            primary_key: Primary key columns
            conflict_resolution: Conflict resolution strategy
            batch_size: Batch size for JDBC writes
            **kwargs: Additional JDBC options
            
        Returns:
            True if upsert successful, False otherwise
        """
        if not primary_key:
            self.logger.error("Primary key is required for upsert mode")
            return False
        
        conflict_resolution = conflict_resolution or "update"
        
        try:
            # Get table columns to ensure we only write columns that exist in the table
            table_columns = self._get_table_columns(destination)
            
            # Filter DataFrame to only include columns that exist in the table
            df_columns = set(df.columns)
            common_columns = df_columns.intersection(table_columns) if table_columns else df_columns
            
            if not common_columns:
                self.logger.error(f"No common columns between DataFrame and table {destination}")
                return False
            
            # Select only common columns
            if common_columns != df_columns:
                missing_cols = df_columns - common_columns
                self.logger.warning(
                    f"DataFrame has columns not in table {destination}: {missing_cols}. "
                    f"These will be skipped."
                )
                df = df.select(*sorted(common_columns))
            
            # Cast columns to proper types for PostgreSQL
            df = self._cast_columns_for_postgresql(df, destination)
            
            # Get column names from filtered DataFrame
            columns = df.columns
            
            # Get all unique constraints (including primary key) - now returns structured dicts
            all_unique_constraints = self._get_unique_constraints(destination)
            
            # If we can't get constraints or they're empty, use primary key directly
            if not all_unique_constraints:
                self.logger.warning(f"Could not get constraints for {destination}, using primary key directly: {primary_key}")
                if primary_key and all(col in columns for col in primary_key):
                    conflict_columns = primary_key
                    constraint_name = None
                    constraint_type = "p"
                else:
                    self.logger.error(f"Cannot perform upsert: primary key {primary_key} not in DataFrame columns {columns}")
                    return False
            else:
                # Select the best constraint for ON CONFLICT (always prefers primary key)
                try:
                    conflict_columns, constraint_name, constraint_type = self._select_conflict_columns(
                        columns, all_unique_constraints, primary_key
                    )
                except ValueError as e:
                    self.logger.error(f"Cannot perform upsert: {e}")
                    return False
            
            # CRITICAL: Deduplicate data based on conflict columns before upsert
            # PostgreSQL doesn't allow the same row to be updated twice in one command
            # If we have duplicate conflict column values, we need to keep only one row per conflict key
            initial_count = df.count()
            if conflict_columns and all(col in columns for col in conflict_columns):
                # Use dropDuplicates to keep only the last row for each conflict key combination
                # This ensures we don't try to update the same row twice
                df = df.dropDuplicates(subset=conflict_columns)
                final_count = df.count()
                if initial_count != final_count:
                    self.logger.warning(
                        f"Deduplicated {destination} data: {initial_count} rows -> {final_count} rows "
                        f"(removed {initial_count - final_count} duplicates based on {conflict_columns})"
                    )
            
            # Check if rows already exist in the table and filter them out
            # This is more efficient than using ON CONFLICT when extracting from the same table
            if conflict_columns and all(col in columns for col in conflict_columns):
                try:
                    # Read existing conflict key values from the table
                    column_list = ", ".join([f'"{c}"' for c in conflict_columns])
                    existing_query = f'SELECT {column_list} FROM {destination}'
                    existing_df = self.spark.read.format("jdbc").options(
                        url=self.jdbc_url,
                        query=existing_query,
                        **self.jdbc_properties
                    ).load()
                    
                    if existing_df.count() > 0:
                        # Anti-join: keep only rows that don't exist in the table
                        # This filters out rows that already exist
                        df = df.join(existing_df, on=conflict_columns, how="left_anti")
                        
                        filtered_count = df.count()
                        if filtered_count < final_count:
                            self.logger.info(
                                f"Filtered out existing rows from {destination}: "
                                f"{final_count} rows -> {filtered_count} rows "
                                f"(removed {final_count - filtered_count} rows that already exist in table)"
                            )
                        
                        # If no new rows to insert, skip the upsert
                        if filtered_count == 0:
                            self.logger.info(f"No new rows to insert for {destination} - all rows already exist")
                            return True
                except Exception as e:
                    # If we can't check existing rows, fall back to ON CONFLICT
                    self.logger.warning(f"Could not check existing rows for {destination}: {e}. Using ON CONFLICT instead.")
            
            # Create temporary table name
            temp_table = f"{destination}_temp_{uuid.uuid4().hex[:8]}"
            
            self.logger.info(f"Upserting to {destination} using temporary table {temp_table}")
            
            # Write to temporary table (after deduplication)
            jdbc_options = {
                "url": self.jdbc_url,
                "dbtable": temp_table,
                "batchsize": str(batch_size),
                **self.jdbc_properties,
                **kwargs
            }
            
            # Write to temporary table (after deduplication)
            # Note: Spark JDBC writes NULL values correctly, but we need to ensure
            # the temp table schema matches what we expect (all text columns)
            df.write.format("jdbc").mode("overwrite").options(**jdbc_options).save()
            
            update_columns = [col for col in columns if col not in conflict_columns]
            
            # When using ON CONFLICT on a non-primary key constraint (e.g., os_code),
            # we need to exclude the primary key from the INSERT to avoid primary key violations
            # The primary key will be preserved from the existing row when ON CONFLICT matches
            insert_columns = columns.copy()
            if constraint_type == "u":  # Using unique constraint, not primary key
                # Find the primary key columns
                pk_columns = []
                for constr in all_unique_constraints:
                    if constr["type"] == "p":
                        pk_columns = constr["columns"]
                        break
                
                # Exclude primary key from INSERT if it's not the conflict target
                if pk_columns and set(pk_columns) != set(conflict_columns):
                    for pk_col in pk_columns:
                        if pk_col in insert_columns:
                            insert_columns.remove(pk_col)
                            self.logger.info(f"Excluding primary key column '{pk_col}' from INSERT when using ON CONFLICT on '{conflict_columns}'")
            
            # Build ON CONFLICT SQL
            conflict_cols_str = ", ".join([f'"{c}"' for c in conflict_columns])
            if conflict_resolution == "update":
                # Update all non-conflict columns
                update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                conflict_clause = f'ON CONFLICT ({conflict_cols_str}) DO UPDATE SET {update_set}'
            elif conflict_resolution == "skip":
                conflict_clause = f'ON CONFLICT ({conflict_cols_str}) DO NOTHING'
            else:
                # Default to update
                update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                conflict_clause = f'ON CONFLICT ({conflict_cols_str}) DO UPDATE SET {update_set}'
            
            # Execute MERGE using INSERT ... ON CONFLICT
            # Note: This requires a JDBC connection, which we'll need to establish
            # For now, we'll use a simpler approach: read from temp table and write with mode
            # In production, you might want to use a JDBC connection pool
            
            # Alternative approach: Read temp table and use JDBC batch insert with conflict handling
            # For simplicity, we'll use Spark's JDBC with a custom query
            # This is a simplified version - in production, you'd want to use a proper JDBC connection
            
            self.logger.warning(
                "Upsert using temporary table. In production, consider using a JDBC connection "
                "pool for better performance and proper ON CONFLICT handling."
            )
            
            # For now, we'll use a workaround: read temp table, then use append mode
            # This is not a true upsert, but works for most cases
            # TODO: Implement proper ON CONFLICT using JDBC connection
            
            # Read from temp table
            temp_df = self.spark.read.format("jdbc").options(
                url=self.jdbc_url,
                dbtable=temp_table,
                **self.jdbc_properties
            ).load()
            
            # Get table columns to identify UUID columns
            # We need to cast UUID columns explicitly in SQL when selecting from temp table
            table_columns_info = self._get_table_columns_with_types(destination)
            
            # Build column list with explicit type casting for SELECT
            # The temp table has text columns, but destination expects specific types
            # We need to cast all non-text types to match PostgreSQL column types
            # Use insert_columns (which may exclude primary key when using ON CONFLICT on unique constraint)
            # Handle NULL values: use NULLIF to convert empty strings or "NULL" strings to NULL
            column_list = []
            for col_name in insert_columns:
                if col_name in table_columns_info:
                    col_type = table_columns_info[col_name].lower()
                    
                    # UUID types - handle NULL values
                    if col_type == "uuid":
                        # NULLIF converts empty string or "NULL" string to NULL, then cast to UUID
                        column_list.append(f'NULLIF(NULLIF("{col_name}", \'\'), \'NULL\')::uuid')
                    
                    # Date/Time types
                    elif col_type in ["timestamp with time zone", "timestamptz"]:
                        column_list.append(f'"{col_name}"::timestamp with time zone')
                    elif col_type in ["timestamp without time zone", "timestamp"]:
                        column_list.append(f'"{col_name}"::timestamp without time zone')
                    elif col_type == "date":
                        column_list.append(f'"{col_name}"::date')
                    elif col_type in ["time with time zone", "timetz"]:
                        column_list.append(f'"{col_name}"::time with time zone')
                    elif col_type in ["time without time zone", "time"]:
                        column_list.append(f'"{col_name}"::time without time zone')
                    elif col_type == "interval":
                        column_list.append(f'"{col_name}"::interval')
                    
                    # Integer types
                    elif col_type in ["integer", "int", "int4"]:
                        column_list.append(f'"{col_name}"::integer')
                    elif col_type in ["bigint", "int8"]:
                        column_list.append(f'"{col_name}"::bigint')
                    elif col_type in ["smallint", "int2"]:
                        column_list.append(f'"{col_name}"::smallint')
                    elif col_type in ["smallserial", "serial2"]:
                        column_list.append(f'"{col_name}"::smallint')
                    elif col_type in ["serial", "serial4"]:
                        column_list.append(f'"{col_name}"::integer')
                    elif col_type in ["bigserial", "serial8"]:
                        column_list.append(f'"{col_name}"::bigint')
                    
                    # Numeric types
                    elif col_type in ["numeric", "decimal"]:
                        column_list.append(f'"{col_name}"::numeric')
                    elif col_type in ["real", "float4"]:
                        column_list.append(f'"{col_name}"::real')
                    elif col_type in ["double precision", "float8", "double"]:
                        column_list.append(f'"{col_name}"::double precision')
                    elif col_type == "money":
                        column_list.append(f'"{col_name}"::money')
                    
                    # Boolean type
                    elif col_type == "boolean" or col_type == "bool":
                        column_list.append(f'"{col_name}"::boolean')
                    
                    # JSON types
                    # JSON strings from Spark may have single quotes instead of double quotes
                    # We need to convert them: {'key': 'value'} -> {"key": "value"}
                    # Use TRANSLATE to convert single quotes to double quotes
                    # This is simpler and works for most cases
                    # Note: This will replace ALL single quotes, which should be fine for Python dict format
                    # If the JSON already has double quotes, they will remain double quotes (no change)
                    elif col_type == "json":
                        # Use TRANSLATE to convert single quotes to double quotes
                        # This handles: {'key': 'value'} -> {"key": "value"}
                        # For arrays of JSON: [{'key': 'value'}] -> [{"key": "value"}]
                        # TRANSLATE replaces each character in the 'from' string with the corresponding character in 'to'
                        # TRANSLATE(string, '''', '"') replaces all single quotes with double quotes
                        # In SQL: ''' is a single quote (two single quotes = one single quote character)
                        # Use double quotes for outer string to avoid escaping issues
                        column_list.append(
                            f"TRANSLATE(\"{col_name}\", '''', '\"')::json"
                        )
                    elif col_type == "jsonb":
                        # Same conversion for JSONB
                        column_list.append(
                            f"TRANSLATE(\"{col_name}\", '''', '\"')::jsonb"
                        )
                    
                    # Binary types
                    elif col_type == "bytea":
                        column_list.append(f'"{col_name}"::bytea')
                    
                    # Network types
                    elif col_type == "inet":
                        column_list.append(f'"{col_name}"::inet')
                    elif col_type == "cidr":
                        column_list.append(f'"{col_name}"::cidr')
                    elif col_type == "macaddr":
                        column_list.append(f'"{col_name}"::macaddr')
                    elif col_type == "macaddr8":
                        column_list.append(f'"{col_name}"::macaddr8')
                    
                    # Geometric types
                    elif col_type == "point":
                        column_list.append(f'"{col_name}"::point')
                    elif col_type == "line":
                        column_list.append(f'"{col_name}"::line')
                    elif col_type == "lseg":
                        column_list.append(f'"{col_name}"::lseg')
                    elif col_type == "box":
                        column_list.append(f'"{col_name}"::box')
                    elif col_type == "path":
                        column_list.append(f'"{col_name}"::path')
                    elif col_type == "polygon":
                        column_list.append(f'"{col_name}"::polygon')
                    elif col_type == "circle":
                        column_list.append(f'"{col_name}"::circle')
                    
                    # Range types
                    elif col_type == "int4range":
                        column_list.append(f'"{col_name}"::int4range')
                    elif col_type == "int8range":
                        column_list.append(f'"{col_name}"::int8range')
                    elif col_type == "numrange":
                        column_list.append(f'"{col_name}"::numrange')
                    elif col_type == "tsrange":
                        column_list.append(f'"{col_name}"::tsrange')
                    elif col_type == "tstzrange":
                        column_list.append(f'"{col_name}"::tstzrange')
                    elif col_type == "daterange":
                        column_list.append(f'"{col_name}"::daterange')
                    
                    # XML type
                    elif col_type == "xml":
                        column_list.append(f'"{col_name}"::xml')
                    
                    # Array types (handle common array types)
                    # Arrays from Spark are in Python list format: ['a', 'b'] or []
                    # PostgreSQL needs: {a,b} or {}
                    # We need to convert: ['a', 'b'] -> {a,b}
                    # Strategy: 
                    # 1. Remove outer double quotes if present (string might be "['a', 'b']")
                    # 2. Replace [ with { and ] with }
                    # 3. Remove all single quotes
                    # 4. Remove spaces around commas for cleaner output
                    elif col_type.endswith("[]"):
                        # Convert Python list format to PostgreSQL array format
                        # ['a', 'b'] -> {a,b}
                        # [] -> {}
                        # "['a', 'b']" -> {a,b} (handle case where string has outer double quotes)
                        # ['4c03ee67-8d00-4053-9301-3868dbf2d5a1', '97db4886-34c1-4909-a495-4befbccbd5a1'] -> {4c03ee67-8d00-4053-9301-3868dbf2d5a1,97db4886-34c1-4909-a495-4befbccbd5a1}
                        # Use TRIM to remove outer double quotes, then do the conversion
                        # Use double quotes for outer string to avoid escaping issues
                        column_list.append(
                            f"REPLACE(REPLACE(REPLACE(TRIM(BOTH '\"' FROM \"{col_name}\"), '[', '{{'), ']', '}}'), '''', '')::{col_type}"
                        )
                    elif col_type.lower() == "array":
                        # Fallback: if type is just "array" without element type info
                        # Try to cast as text[] (most common array type)
                        # This shouldn't happen if _get_table_columns_with_types works correctly
                        self.logger.warning(
                            f"Column {col_name} has type 'array' without element type. "
                            f"Assuming text[] for casting."
                        )
                        column_list.append(
                            f"REPLACE(REPLACE(REPLACE(TRIM(BOTH '\"' FROM \"{col_name}\"), '[', '{{'), ']', '}}'), '''', '')::text[]"
                        )
                    
                    # Text types (no casting needed)
                    elif col_type in ["text", "varchar", "character varying", "char", "character", "bpchar"]:
                        column_list.append(f'"{col_name}"')
                    
                    # Unknown types - try to cast using the type name directly
                    # This handles custom types and any types we might have missed
                    else:
                        # For unknown types, try casting directly
                        # PostgreSQL will handle the conversion if possible
                        column_list.append(f'"{col_name}"::{col_type}')
                else:
                    # Column not in table info - no casting
                    column_list.append(f'"{col_name}"')
            
            # Execute custom SQL INSERT with ON CONFLICT using direct JDBC connection
            # Build the INSERT query with ON CONFLICT clause
            # Note: stringtype=unspecified only works for parameterized INSERTs, not SELECTs
            # So we need explicit casting in the SELECT statement
            insert_query = f"""
                INSERT INTO {destination} ({', '.join([f'"{c}"' for c in insert_columns])})
                SELECT {', '.join(column_list)}
                FROM {temp_table}
                {conflict_clause}
            """
            
            self.logger.info(f"Executing custom INSERT with ON CONFLICT for {destination}")
            self.logger.debug(f"INSERT query: {insert_query[:300]}...")
            
            # Execute the INSERT query using direct JDBC connection
            # Spark JDBC doesn't support custom SQL for writes, so we use psycopg2
            try:
                import psycopg2
                
                # Parse JDBC URL to get connection parameters
                # jdbc:postgresql://host:port/db?stringtype=unspecified
                url = self.jdbc_url.replace("jdbc:postgresql://", "").split("/")
                host_port = url[0].split(":")
                host = host_port[0]
                port = int(host_port[1]) if len(host_port) > 1 else 5432
                database = url[1].split("?")[0] if len(url) > 1 else "postgres"
                
                # Get connection properties
                user = self.jdbc_properties.get("user", "postgres")
                password = self.jdbc_properties.get("password", "")
                
                # Connect to PostgreSQL and execute the INSERT query
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )
                
                try:
                    cursor = conn.cursor()
                    cursor.execute(insert_query)
                    conn.commit()
                    self.logger.info(f"Successfully executed INSERT with ON CONFLICT for {destination}")
                    
                    # Drop temporary table
                    try:
                        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                        cursor.execute(drop_sql)
                        conn.commit()
                        self.logger.debug(f"Dropped temporary table {temp_table}")
                    except Exception as drop_error:
                        self.logger.warning(f"Could not drop temporary table {temp_table}: {drop_error}")
                        conn.rollback()
                        
                except psycopg2.errors.UniqueViolation as e:
                    # Unique constraint violation - handle different constraint violations
                    conn.rollback()
                    error_msg = str(e)
                    self.logger.warning(f"ON CONFLICT failed: {error_msg}")
                    self.logger.debug(f"Current constraint_type: {constraint_type}, primary_key: {primary_key}, conflict_columns: {conflict_columns}")
                    
                    # Extract the constraint name from the error message
                    violated_constraint_name = None
                    if 'constraint "' in error_msg:
                        start = error_msg.find('constraint "') + len('constraint "')
                        end = error_msg.find('"', start)
                        if end > start:
                            violated_constraint_name = error_msg[start:end]
                    
                    # Find which constraint was violated
                    violated_constraint = None
                    if violated_constraint_name:
                        for constr in all_unique_constraints:
                            if constr["name"] == violated_constraint_name:
                                violated_constraint = constr
                                break
                    
                    # If we used primary key but hit a unique constraint violation, 
                    # it means the data violates a different unique constraint (e.g., os_code)
                    # In this case, we should use the violated constraint for ON CONFLICT
                    if violated_constraint and violated_constraint["type"] == "u":
                        self.logger.info(
                            f"Data violates unique constraint {violated_constraint_name} ({violated_constraint['columns']}). "
                            f"Retrying with this constraint instead of {conflict_columns}."
                        )
                        conflict_columns = violated_constraint["columns"]
                        constraint_name = violated_constraint["name"]
                        constraint_type = "u"
                        
                        # Rebuild insert_columns: exclude primary key when using unique constraint
                        insert_columns_retry = columns.copy()
                        pk_columns = []
                        for constr in all_unique_constraints:
                            if constr["type"] == "p":
                                pk_columns = constr["columns"]
                                break
                        
                        if pk_columns and set(pk_columns) != set(conflict_columns):
                            for pk_col in pk_columns:
                                if pk_col in insert_columns_retry:
                                    insert_columns_retry.remove(pk_col)
                                    self.logger.info(f"Excluding primary key column '{pk_col}' from INSERT when using ON CONFLICT on '{conflict_columns}'")
                        
                        # Rebuild conflict clause with the violated constraint
                        update_columns = [col for col in columns if col not in conflict_columns]
                        conflict_cols_str_retry = ", ".join([f'"{c}"' for c in conflict_columns])
                        if conflict_resolution == "update":
                            update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                            conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO UPDATE SET {update_set}'
                        elif conflict_resolution == "skip":
                            conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO NOTHING'
                        else:
                            update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                            conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO UPDATE SET {update_set}'
                        
                        # Rebuild column_list for retry (only include columns in insert_columns_retry)
                        column_list_retry = []
                        for col_name in insert_columns_retry:
                            if col_name in table_columns_info:
                                col_type = table_columns_info[col_name].lower()
                                # Use the same casting logic as before (simplified - reuse the same logic)
                                # For now, just use the same casting approach
                                if col_type == "uuid":
                                    # Handle NULL values: NULLIF converts empty string or "NULL" string to NULL
                                    column_list_retry.append(f'NULLIF(NULLIF("{col_name}", \'\'), \'NULL\')::uuid')
                                elif col_type in ["timestamp with time zone", "timestamptz"]:
                                    column_list_retry.append(f'"{col_name}"::timestamp with time zone')
                                elif col_type in ["timestamp without time zone", "timestamp"]:
                                    column_list_retry.append(f'"{col_name}"::timestamp without time zone')
                                elif col_type == "date":
                                    column_list_retry.append(f'"{col_name}"::date')
                                elif col_type in ["integer", "int", "int4"]:
                                    column_list_retry.append(f'"{col_name}"::integer')
                                elif col_type in ["bigint", "int8"]:
                                    column_list_retry.append(f'"{col_name}"::bigint')
                                elif col_type in ["numeric", "decimal"]:
                                    column_list_retry.append(f'"{col_name}"::numeric')
                                elif col_type == "boolean" or col_type == "bool":
                                    column_list_retry.append(f'"{col_name}"::boolean')
                                elif col_type == "json":
                                    column_list_retry.append(f"TRANSLATE(\"{col_name}\", '''', '\"')::json")
                                elif col_type == "jsonb":
                                    column_list_retry.append(f"TRANSLATE(\"{col_name}\", '''', '\"')::jsonb")
                                elif col_type.endswith("[]"):
                                    column_list_retry.append(f"REPLACE(REPLACE(REPLACE(TRIM(BOTH '\"' FROM \"{col_name}\"), '[', '{{'), ']', '}}'), '''', '')::{col_type}")
                                elif col_type in ["text", "varchar", "character varying", "char", "character", "bpchar"]:
                                    column_list_retry.append(f'"{col_name}"')
                                else:
                                    column_list_retry.append(f'"{col_name}"::{col_type}')
                            else:
                                column_list_retry.append(f'"{col_name}"')
                        
                        # Rebuild insert query
                        insert_query = f"""
                            INSERT INTO {destination} ({', '.join([f'"{c}"' for c in insert_columns_retry])})
                            SELECT {', '.join(column_list_retry)}
                            FROM {temp_table}
                            {conflict_clause}
                        """
                        
                        # Retry with the violated constraint
                        try:
                            cursor.execute(insert_query)
                            conn.commit()
                            self.logger.info(f"Successfully executed INSERT with ON CONFLICT (retry with violated constraint) for {destination}")
                            
                            # Drop temporary table
                            try:
                                drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                                cursor.execute(drop_sql)
                                conn.commit()
                                self.logger.debug(f"Dropped temporary table {temp_table}")
                            except Exception as drop_error:
                                self.logger.warning(f"Could not drop temporary table {temp_table}: {drop_error}")
                                conn.rollback()
                            
                            return True
                        except Exception as retry_error:
                            conn.rollback()
                            self.logger.warning(
                                f"Retry with violated constraint also failed: {retry_error}. "
                                f"Falling back to append mode."
                            )
                    
                    # If we used a unique constraint but hit primary key violation, retry with primary key
                    elif constraint_type == "u" and primary_key:
                        self.logger.debug(f"Attempting retry with primary key. constraint_type={constraint_type}, primary_key={primary_key}")
                        # Check if primary key columns are in DataFrame
                        if all(col in columns for col in primary_key):
                            # Find the primary key constraint
                            pk_constraint = None
                            for constr in all_unique_constraints:
                                if constr["type"] == "p" and set(constr["columns"]) == set(primary_key):
                                    pk_constraint = constr
                                    break
                            
                            if pk_constraint:
                                # Retry with primary key
                                self.logger.info(f"Retrying upsert with PRIMARY KEY: {pk_constraint['columns']} (original constraint was {conflict_columns})")
                                conflict_columns = pk_constraint["columns"]
                                constraint_name = pk_constraint["name"]
                                constraint_type = "p"
                                
                                # Rebuild conflict clause with primary key
                                update_columns = [col for col in columns if col not in conflict_columns]
                                conflict_cols_str_retry = ", ".join([f'"{c}"' for c in conflict_columns])
                                if conflict_resolution == "update":
                                    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                                    conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO UPDATE SET {update_set}'
                                elif conflict_resolution == "skip":
                                    conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO NOTHING'
                                else:
                                    update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                                    conflict_clause = f'ON CONFLICT ({conflict_cols_str_retry}) DO UPDATE SET {update_set}'
                                
                                # Rebuild insert query
                                insert_query = f"""
                                    INSERT INTO {destination} ({', '.join([f'"{c}"' for c in columns])})
                                    SELECT {', '.join(column_list)}
                                    FROM {temp_table}
                                    {conflict_clause}
                                """
                                
                                # Retry with primary key ONCE
                                try:
                                    cursor.execute(insert_query)
                                    conn.commit()
                                    self.logger.info(f"Successfully executed INSERT with ON CONFLICT (retry with PK) for {destination}")
                                    
                                    # Drop temporary table
                                    try:
                                        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                                        cursor.execute(drop_sql)
                                        conn.commit()
                                        self.logger.debug(f"Dropped temporary table {temp_table}")
                                    except Exception as drop_error:
                                        self.logger.warning(f"Could not drop temporary table {temp_table}: {drop_error}")
                                        conn.rollback()
                                    
                                    return True
                                except Exception as retry_error:
                                    conn.rollback()
                                    self.logger.warning(
                                        f"Retry with PRIMARY KEY also failed: {retry_error}. "
                                        f"Falling back to append mode."
                                    )
                    
                    # If we already tried PK or no PK available → fall back to append mode
                    self.logger.warning(
                        f"ON CONFLICT failed for {destination}: {e}. "
                        f"Falling back to append mode."
                    )
                    # Drop temp table and use append mode instead
                    try:
                        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                        cursor.execute(drop_sql)
                        conn.commit()
                    except Exception as drop_error:
                        self.logger.warning(f"Could not drop temp table {temp_table}: {drop_error}")
                    
                    # Return False to trigger fallback to append mode
                    return False
                except psycopg2.errors.InvalidColumnReference as e:
                    # ON CONFLICT failed - likely no primary key or unique constraint
                    # Fall back to append mode
                    conn.rollback()
                    self.logger.warning(
                        f"ON CONFLICT failed for {destination}: {e}. "
                        f"Falling back to append mode."
                    )
                    # Drop temp table and use append mode instead
                    try:
                        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                        cursor.execute(drop_sql)
                        conn.commit()
                    except Exception as drop_error:
                        self.logger.warning(f"Could not drop temp table {temp_table}: {drop_error}")
                    
                    # Return False to trigger fallback to append mode
                    return False
                except Exception as sql_error:
                    conn.rollback()
                    self.logger.error(f"Error during upsert to PostgreSQL: {sql_error}", exc_info=True)
                    # Return False to trigger fallback to append mode with explicit casting
                    return False
                finally:
                    conn.close()
                    
            except ImportError:
                # psycopg2 not available - fall back to simpler approach
                self.logger.warning(
                    "psycopg2 not available. Using append mode (will fail on duplicates). "
                    "Install psycopg2 for proper ON CONFLICT handling: pip install psycopg2-binary"
                )
                
                # Fallback: use append mode (will fail on duplicates)
                jdbc_options_dest = {
                    "url": self.jdbc_url,
                    "dbtable": destination,
                    "batchsize": str(batch_size),
                    **self.jdbc_properties,
                    **kwargs
                }
                temp_df.write.format("jdbc").mode("append").options(**jdbc_options_dest).save()
                
                # Note: Temporary table cleanup skipped when psycopg2 is not available
                self.logger.warning(f"Temporary table {temp_table} should be dropped manually")
            
            self.logger.info(f"Successfully upserted data to PostgreSQL table: {destination}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during upsert to PostgreSQL: {e}", exc_info=True)
            return False
    
    def _ensure_safe_array_function(self, conn) -> None:
        """
        Ensure the safe_to_text_array function exists in PostgreSQL.
        
        This function safely converts text to text[] arrays, catching malformed
        array literal errors and returning NULL instead of aborting the query.
        
        Args:
            conn: psycopg2 connection object
        """
        try:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE OR REPLACE FUNCTION safe_to_text_array(input text)
                RETURNS text[] AS $$
                DECLARE
                    converted text;
                BEGIN
                    IF input IS NULL THEN
                        RETURN NULL;
                    END IF;
                    
                    BEGIN
                        -- First, try to convert Python list format to PostgreSQL array format
                        -- Convert ['a', 'b'] to {a,b}
                        converted := REPLACE(REPLACE(REPLACE(TRIM(BOTH '"' FROM input), '[', '{'), ']', '}'), '''', '');
                        RETURN converted::text[];
                    EXCEPTION WHEN OTHERS THEN
                        RETURN NULL;
                    END;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE;
            """)
            conn.commit()
            cursor.close()
            self.logger.debug("✅ safe_to_text_array function created/verified")
        except Exception as e:
            self.logger.warning(f"Could not create safe_to_text_array function: {e}. Will use direct casting.")
            conn.rollback()
    
    def _insert_with_casting(
        self,
        df: DataFrame,
        destination: str,
        batch_size: int,
        primary_key: Optional[List[str]] = None,
        **kwargs
    ) -> bool:
        """
        Insert data to PostgreSQL with explicit type casting.
        
        This method uses a temporary table and explicit casting in SQL to handle
        type conversions (UUID, timestamp, etc.) that Spark JDBC doesn't handle well.
        
        Args:
            df: DataFrame to insert
            destination: Destination table name
            batch_size: Batch size for JDBC writes
            primary_key: Primary key columns for filtering existing rows
            **kwargs: Additional JDBC options
            
        Returns:
            True if insert successful, False otherwise
        """
        try:
            import psycopg2
            
            # Get table columns to filter DataFrame
            table_columns = self._get_table_columns(destination)
            if table_columns:
                df_columns = set(df.columns)
                common_columns = df_columns.intersection(table_columns)
                if common_columns != df_columns:
                    df = df.select(*sorted(common_columns))
            
            # Filter out existing rows based on all unique constraints
            # This prevents duplicate key errors when extracting from the same table
            # We check all unique constraints (not just primary key) because business keys
            # (like company_name, company_code) are more likely to be duplicated
            constraints = self._get_unique_constraints(destination)
            if constraints:
                try:
                    # Try to filter based on all unique constraints
                    # Start with the most specific (unique constraints) before primary key
                    unique_constraints = [c for c in constraints if c["type"] == "u"]
                    pk_constraint = next((c for c in constraints if c["type"] == "p"), None)
                    
                    # Build list of constraints to check (unique constraints first, then primary key)
                    constraints_to_check = unique_constraints + ([pk_constraint] if pk_constraint else [])
                    
                    original_count = df.count()
                    filtered_df = df
                    for constr in constraints_to_check:
                        constr_cols = constr["columns"]
                        # Check if all constraint columns exist in DataFrame
                        if all(col in filtered_df.columns for col in constr_cols):
                            try:
                                # Read existing constraint key values from the table
                                column_list = ", ".join([f'"{c}"' for c in constr_cols])
                                existing_query = f'SELECT {column_list} FROM {destination}'
                                existing_df = self.spark.read.format("jdbc").options(
                                    url=self.jdbc_url,
                                    query=existing_query,
                                    **self.jdbc_properties
                                ).load()
                                
                                if existing_df.count() > 0:
                                    # Anti-join: keep only rows that don't exist in the table
                                    before_count = filtered_df.count()
                                    filtered_df = filtered_df.join(existing_df, on=constr_cols, how="left_anti")
                                    
                                    filtered_count = filtered_df.count()
                                    if filtered_count == 0:
                                        self.logger.info(f"No new rows to insert for {destination} - all rows already exist (checked constraint: {constr['name']})")
                                        return {"success": True, "rows_inserted": 0}
                                    
                                    # If we filtered some rows, log it and continue checking other constraints
                                    if filtered_count < before_count:
                                        self.logger.info(
                                            f"Filtered out existing rows from {destination} using constraint {constr['name']}: "
                                            f"{before_count} rows -> {filtered_count} rows"
                                        )
                            except Exception as constr_filter_error:
                                # If filtering on this constraint fails, continue with next constraint
                                self.logger.debug(f"Could not filter using constraint {constr['name']}: {constr_filter_error}. Trying next constraint.")
                                continue
                    
                    # Final check after all constraint filtering
                    final_count = filtered_df.count()
                    if final_count == 0:
                        self.logger.info(f"No new rows to insert for {destination} - all rows already exist")
                        return {"success": True, "rows_inserted": 0}
                    elif final_count < original_count:
                        self.logger.info(
                            f"Filtered out existing rows from {destination}: "
                            f"{original_count} rows -> {final_count} rows "
                            f"(removed rows that already exist in table)"
                        )
                    
                    # Use the filtered DataFrame for insertion
                    df = filtered_df
                except Exception as filter_error:
                    # If filtering fails, continue with all rows (may cause duplicates)
                    self.logger.warning(f"Could not filter existing rows for {destination}: {filter_error}. Continuing with all rows.")
            
            # Create temporary table name
            temp_table = f"{destination}_temp_{uuid.uuid4().hex[:8]}"
            
            # Get table column types to identify TEXT columns
            table_columns_info = self._get_table_columns_with_types(destination)
            
            # For TEXT columns, cast to binary before writing to temp table
            # This prevents PostgreSQL from parsing {, as array literals
            # The root cause: Spark JDBC writes TEXT columns with {} braces, which PostgreSQL tries to parse as arrays
            # Solution: Write TEXT columns as binary, then use convert_from() in PostgreSQL
            df_to_write = df
            text_columns_to_convert = []
            if table_columns_info:
                for col_name in df.columns:
                    if col_name in table_columns_info:
                        col_type = table_columns_info[col_name].lower()
                        if col_type == "text":
                            # Cast TEXT column to binary before writing
                            # This prevents PostgreSQL from parsing {, as array literals
                            df_to_write = df_to_write.withColumn(col_name, col(col_name).cast(BinaryType()))
                            text_columns_to_convert.append(col_name)
            
            # Write to temporary table
            jdbc_options = {
                "url": self.jdbc_url,
                "dbtable": temp_table,
                "batchsize": str(batch_size),
                **self.jdbc_properties,
                **kwargs
            }
            
            df_to_write.write.format("jdbc").mode("overwrite").options(**jdbc_options).save()
            
            # Get column names from DataFrame
            columns = df.columns
            
            # Get table columns to identify types
            table_columns_info = self._get_table_columns_with_types(destination)
            
            # Build column list with explicit type casting for SELECT
            column_list = []
            missing_columns = []
            for col_name in columns:
                if col_name in table_columns_info:
                    col_type = table_columns_info[col_name].lower()
                    
                    # UUID types - handle NULL values
                    if col_type == "uuid":
                        # NULLIF converts empty string or "NULL" string to NULL, then cast to UUID
                        column_list.append(f'NULLIF(NULLIF("{col_name}", \'\'), \'NULL\')::uuid')
                    # Date/Time types
                    elif col_type in ["timestamp with time zone", "timestamptz"]:
                        column_list.append(f'"{col_name}"::timestamp with time zone')
                    elif col_type in ["timestamp without time zone", "timestamp"]:
                        column_list.append(f'"{col_name}"::timestamp without time zone')
                    elif col_type == "date":
                        column_list.append(f'"{col_name}"::date')
                    # Integer types
                    elif col_type in ["integer", "int", "int4"]:
                        column_list.append(f'"{col_name}"::integer')
                    elif col_type in ["bigint", "int8"]:
                        column_list.append(f'"{col_name}"::bigint')
                    elif col_type in ["smallint", "int2"]:
                        column_list.append(f'"{col_name}"::smallint')
                    # Numeric types
                    elif col_type in ["numeric", "decimal"]:
                        column_list.append(f'"{col_name}"::numeric')
                    elif col_type in ["real", "float4"]:
                        column_list.append(f'"{col_name}"::real')
                    elif col_type in ["double precision", "float8", "double"]:
                        column_list.append(f'"{col_name}"::double precision')
                    # Boolean type
                    elif col_type == "boolean" or col_type == "bool":
                        column_list.append(f'"{col_name}"::boolean')
                    # JSON types
                    elif col_type == "json":
                        column_list.append(f"TRANSLATE(\"{col_name}\", '''', '\"')::json")
                    elif col_type == "jsonb":
                        column_list.append(f"TRANSLATE(\"{col_name}\", '''', '\"')::jsonb")
                    # Array types
                    elif col_type.endswith("[]"):
                        # Use safe_to_text_array() function to safely convert text to text[] arrays
                        # This function catches malformed array literal errors and returns NULL
                        # instead of aborting the whole query
                        # The function handles:
                        # - Python list format ['a', 'b'] -> {a,b}
                        # - Malformed values like "{, sample..." -> NULL
                        # - Valid PostgreSQL arrays -> parsed correctly
                        column_list.append(f'safe_to_text_array("{col_name}"::text)::{col_type}')
                    # Text types - use convert_from() for binary columns
                    elif col_type in ["text", "varchar", "character varying", "char", "character", "bpchar"]:
                        # For TEXT columns that were written as binary, use convert_from() to convert back to text
                        # The root cause: Spark JDBC writes TEXT columns with {} braces, which PostgreSQL tries to parse as arrays
                        # Solution: Write TEXT columns as binary in Spark, then use convert_from() in PostgreSQL
                        # This completely bypasses the array literal parser because bytea values are never parsed as arrays
                        if col_type == "text":
                            # Check if this column was written as binary (it will be bytea in the temp table)
                            # Use convert_from() to convert bytea → text safely
                            # This works because bytea values are never parsed as array literals
                            if col_name in text_columns_to_convert:
                                # Column is stored as bytea in temp table
                                # Use convert_from() to convert bytea → text
                                # This prevents PostgreSQL from trying to parse it as text/array before convert_from() runs
                                column_list.append(f"convert_from(\"{col_name}\"::bytea, 'UTF8')::text")
                            else:
                                column_list.append(f'"{col_name}"::text')
                        else:
                            column_list.append(f'"{col_name}"')
                    # Unknown types - try to cast using the type name directly
                    else:
                        column_list.append(f'"{col_name}"::{col_type}')
                else:
                    # Column not in table info - no casting
                    missing_columns.append(col_name)
                    column_list.append(f'"{col_name}"')
            
            # Build INSERT query with explicit casting
            # For TEXT columns, we now use convert_from() which bypasses the parser completely
            # This is the industry-standard fix used by Debezium, Airbyte, and Fivetran
            insert_query = f"""
                INSERT INTO {destination} ({', '.join([f'"{c}"' for c in columns])})
                SELECT {', '.join(column_list)}
                FROM {temp_table}
            """
            
            self.logger.info(f"Executing INSERT with explicit casting for {destination}")
            
            # Parse JDBC URL to get connection parameters
            url = self.jdbc_url.replace("jdbc:postgresql://", "").split("/")
            host_port = url[0].split(":")
            host = host_port[0]
            port = int(host_port[1]) if len(host_port) > 1 else 5432
            database = url[1].split("?")[0] if len(url) > 1 else "postgres"
            
            # Get connection properties
            user = self.jdbc_properties.get("user", "postgres")
            password = self.jdbc_properties.get("password", "")
            
            # Connect to PostgreSQL and execute the INSERT query
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            
            try:
                cursor = conn.cursor()
                
                # Ensure safe_to_text_array function exists before executing INSERT
                # This function safely converts text to text[] arrays, catching malformed
                # array literal errors and returning NULL instead of aborting the query
                has_array_columns = any(col_type.endswith("[]") for col_type in (table_columns_info or {}).values())
                if has_array_columns:
                    self._ensure_safe_array_function(conn)
                
                cursor.execute(insert_query)
                rows_inserted = cursor.rowcount
                conn.commit()
                self.logger.info(f"Successfully executed INSERT with explicit casting for {destination}: {rows_inserted} rows inserted")
                
                # Drop temporary table
                try:
                    drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
                    cursor.execute(drop_sql)
                    conn.commit()
                    self.logger.debug(f"Dropped temporary table {temp_table}")
                except Exception as drop_error:
                    self.logger.warning(f"Could not drop temporary table {temp_table}: {drop_error}")
                    conn.rollback()
                
                return {"success": True, "rows_inserted": rows_inserted}
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Error executing INSERT with explicit casting: {e}", exc_info=True)
                return {"success": False, "rows_inserted": 0}
            finally:
                conn.close()
                
        except ImportError:
            self.logger.error("psycopg2 not available. Cannot use explicit casting. Install psycopg2-binary")
            return False
        except Exception as e:
            self.logger.error(f"Error during insert with casting: {e}", exc_info=True)
            return False

