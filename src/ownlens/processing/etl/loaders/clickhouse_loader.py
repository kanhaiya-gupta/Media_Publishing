"""
ClickHouse Loader
=================

Load data to ClickHouse database.

Supports:
- Append mode
- Data type conversions (UUID → String, JSONB → String)
- Table mapping (PostgreSQL → ClickHouse)
- Partition handling
"""

from typing import Optional, Dict, Any, List, Set
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, to_json
from pyspark.sql.types import StringType, ArrayType, MapType, StructType, BooleanType, IntegerType
from datetime import datetime, date

from ..base.loader import BaseLoader
from ..utils.config import ETLConfig, get_etl_config
from ..utils.loader_config import TABLE_LOAD_CONFIGS, get_table_load_config

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class ClickHouseLoader(BaseLoader):
    """
    Load data to ClickHouse database.
    
    Supports:
    - Append mode
    - Data type conversions (UUID → String, JSONB → String)
    - Table mapping (PostgreSQL → ClickHouse)
    - Partition handling
    - Batch loading
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ClickHouse loader.
        
        Args:
            spark: SparkSession instance
            config: ETLConfig instance (optional, uses global config if not provided)
            loader_config: Loader-specific configuration
        """
        super().__init__(spark, loader_config)
        self.etl_config = config or get_etl_config()
        # Build JDBC URL with password in query string (ClickHouse JDBC prefers this format)
        # Format: jdbc:clickhouse://host:port/database?password=xxx&user=xxx
        # This matches the format used in v1/scripts/test_clickhouse_connection.py
        query_params = []
        if self.etl_config.clickhouse_password:
            query_params.append(f"password={self.etl_config.clickhouse_password}")
        if self.etl_config.clickhouse_user:
            query_params.append(f"user={self.etl_config.clickhouse_user}")
        
        query_string = "?" + "&".join(query_params) if query_params else ""
        
        self.clickhouse_url = (
            f"jdbc:clickhouse://{self.etl_config.clickhouse_host}:{self.etl_config.clickhouse_port}/"
            f"{self.etl_config.clickhouse_database}{query_string}"
        )
        
        # Also set in properties as fallback (some JDBC drivers prefer properties)
        self.clickhouse_properties = {
            "user": self.etl_config.clickhouse_user,
            "password": self.etl_config.clickhouse_password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        }
        # Cache for parsed schema information
        self._schema_cache: Dict[str, Set[str]] = {}
        self._table_engine_cache: Dict[str, str] = {}  # Cache for table engine types
        self._schema_dir = Path(__file__).parent.parent.parent.parent / "clickhouse"
        
        # Log connection info (without password for security)
        safe_url = self.clickhouse_url.replace(f"password={self.etl_config.clickhouse_password}", "password=***") if self.etl_config.clickhouse_password else self.clickhouse_url
        self.logger.info(f"ClickHouse Loader configured to write to database: {self.etl_config.clickhouse_database}")
        self.logger.debug(f"ClickHouse JDBC URL: {safe_url}")
        
        # Warn if password is not set
        if not self.etl_config.clickhouse_password:
            self.logger.warning(
                "CLICKHOUSE_PASSWORD is not set. If ClickHouse requires authentication, "
                "set CLICKHOUSE_PASSWORD in your environment file."
            )
        
        # Ensure database exists (create if it doesn't)
        self._ensure_database_exists()
        
        # Note: Schema initialization is done beforehand using:
        # docker exec -i clickhouse clickhouse-client --password clickhouse < clickhouse/init.sql
        # The loader assumes schemas are already initialized and only creates individual tables
        # if they don't exist (via _ensure_table_exists in the load method)
    
    def load(
        self,
        df: DataFrame,
        destination: str,
        mode: str = "append",
        batch_size: int = 10000,
        **kwargs
    ) -> bool:
        """
        Load data to ClickHouse table.
        
        Args:
            df: DataFrame to load
            destination: PostgreSQL table name (will be mapped to ClickHouse table)
            mode: Write mode ("append", "overwrite", "replace_partition", "ignore", "error")
            batch_size: Batch size for JDBC writes (default: 10000, optimized for ClickHouse)
            **kwargs: Additional JDBC options
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            if not self.validate_input(df):
                return False
            
            # Map PostgreSQL table name to ClickHouse table name
            clickhouse_table = self._get_clickhouse_table_name(destination)
            
            # Get table-specific config
            table_config = self._get_table_config(destination)
            
            # Override mode from config if available
            if table_config and table_config.mode:
                mode = table_config.mode
            
            # Override batch_size if specified in config
            if table_config and table_config.kwargs and "batch_size" in table_config.kwargs:
                batch_size = table_config.kwargs["batch_size"]
            
            self.log_load_stats(df, f"ClickHouse: {clickhouse_table}")
            
            # Convert data types for ClickHouse compatibility
            df = self._convert_types_for_clickhouse(df, destination)
            
            # Ensure table exists (create from schema files if needed)
            # This will create the table if it doesn't exist
            self._ensure_table_exists(clickhouse_table)
            
            # Filter DataFrame to only include columns that exist in ClickHouse table
            # (ClickHouse tables may have different columns than PostgreSQL)
            df = self._filter_columns_for_clickhouse(df, clickhouse_table)
            
            # Handle different load modes
            if mode == "replace_partition":
                return self._replace_partition(df, clickhouse_table, destination, batch_size, **kwargs)
            elif mode == "merge" or self._is_replacing_merge_tree(clickhouse_table):
                # For ReplacingMergeTree tables, use append mode
                # ClickHouse will automatically deduplicate on merge
                self.logger.info(f"Table {clickhouse_table} uses ReplacingMergeTree - using append mode for automatic deduplication")
                mode = "append"
            
            # Optimize batch size for ClickHouse (ClickHouse prefers larger batches)
            # Default is 10000, but can be increased for better performance
            optimal_batch_size = max(batch_size, 50000) if df.count() > 100000 else batch_size
            
            # Use query-based approach instead of dbtable to prevent Spark from auto-creating table
            # Spark JDBC auto-creation uses wrong table name, so we'll use INSERT directly
            # First, verify table exists one more time (race condition check)
            import time
            max_retries = 3
            for retry in range(max_retries):
                if self._verify_table_exists(clickhouse_table):
                    break
                if retry < max_retries - 1:
                    self.logger.debug(f"Table '{clickhouse_table}' not found, retrying in 0.5s... (attempt {retry + 1}/{max_retries})")
                    time.sleep(0.5)
                else:
                    self.logger.error(f"Table '{clickhouse_table}' does not exist after {max_retries} attempts. Aborting write.")
                    return False
            
            # Use clickhouse-driver to bypass Spark JDBC auto-creation issues
            # This approach writes data directly using ClickHouse native protocol (like v1)
            try:
                from clickhouse_driver import Client
                from datetime import datetime
                
                # Try different native ports (9002 is mapped from container's 9000, like v1)
                native_ports = [9002, 9000]
                client = None
                
                for native_port in native_ports:
                    try:
                        client = Client(
                            host=self.etl_config.clickhouse_host,
                            port=native_port,
                            database=self.etl_config.clickhouse_database,
                            user=self.etl_config.clickhouse_user,
                            password=self.etl_config.clickhouse_password if self.etl_config.clickhouse_password else ''
                        )
                        client.execute("SELECT 1")  # Test connection
                        break
                    except Exception as e:
                        if client:
                            try:
                                client.disconnect()
                            except:
                                pass
                        client = None
                        if native_port == native_ports[-1]:  # Last port attempt
                            raise Exception(f"Could not connect to ClickHouse on ports {native_ports}: {e}")
                        continue
                
                try:
                    # Get ClickHouse table columns in order (like v1 - query directly from ClickHouse)
                    # This ensures column order matches the table definition
                    # DESCRIBE TABLE returns: (name, type, default_type, default_expression, comment, codec_expression, ttl_expression)
                    # But DESCRIBE TABLE might not show Nullable() wrapper, so query system.columns for full type
                    describe_result = client.execute(f"DESCRIBE TABLE {self.etl_config.clickhouse_database}.{clickhouse_table}")
                    clickhouse_columns = [col[0] for col in describe_result]  # Extract column names in order
                    clickhouse_types = {col[0]: col[1] for col in describe_result}  # Extract column types
                    
                    # Get full column types and default_kind from system.columns (includes Nullable wrapper and MATERIALIZED info)
                    # default_kind: '', 'DEFAULT', 'MATERIALIZED', 'ALIAS', 'EPHEMERAL'
                    full_types_result = client.execute(
                        f"SELECT name, type, default_kind FROM system.columns "
                        f"WHERE database = '{self.etl_config.clickhouse_database}' "
                        f"AND table = '{clickhouse_table}'"
                    )
                    full_types = {col[0]: col[1] for col in full_types_result}  # Extract full types (includes Nullable wrapper)
                    materialized_columns = {col[0] for col in full_types_result if col[2] == 'MATERIALIZED'}  # Extract MATERIALIZED columns
                    
                    # Update clickhouse_types with full types (which include Nullable wrapper)
                    clickhouse_types.update(full_types)
                    
                    # Filter out MATERIALIZED columns - they cannot be inserted (computed automatically)
                    if materialized_columns:
                        self.logger.debug(f"Filtering out MATERIALIZED columns from {clickhouse_table}: {materialized_columns}")
                        clickhouse_columns = [col for col in clickhouse_columns if col not in materialized_columns]
                    
                    if not clickhouse_columns:
                        raise Exception(f"Could not get columns for table {clickhouse_table}")
                    
                    self.logger.debug(f"ClickHouse table {clickhouse_table} has {len(clickhouse_columns)} columns in order: {clickhouse_columns[:5]}...")
                    
                    # Collect all rows from DataFrame (like v1 approach)
                    rows = df.collect()
                    
                    if not rows:
                        self.logger.warning(f"No rows to insert into ClickHouse table: {clickhouse_table}")
                        return True
                    
                    # Convert Spark Row to tuple matching ClickHouse column order (like v1)
                    data_to_insert = []
                    for row in rows:
                        row_dict = row.asDict()
                        values = []
                        
                        for col_name in clickhouse_columns:
                            if col_name in row_dict:
                                value = row_dict[col_name]
                                # Get full column type (includes Nullable wrapper) from clickhouse_types
                                full_col_type = clickhouse_types.get(col_name, '')
                                col_type_upper = full_col_type.upper()
                                
                                # Convert value based on ClickHouse column type (pass full type to ensure Nullable is detected)
                                value = self._convert_value_for_clickhouse(value, col_type_upper, col_name)
                                
                                # Special handling for Date/DateTime columns with None values
                                # ACTUAL TABLE HAS: DateTime (non-nullable), not Nullable(DateTime)
                                # So we must use placeholder values for None
                                if value is None:
                                    # Check if column is nullable using full_col_type (already set above)
                                    # full_col_type already includes Nullable wrapper from clickhouse_types
                                    is_nullable = 'NULLABLE(' in col_type_upper or col_type_upper.startswith('NULLABLE')
                                    
                                    if is_nullable:
                                        # For Nullable Date/DateTime columns, pass None directly - clickhouse-driver handles it!
                                        # Keep value as None - DO NOT convert to placeholder
                                        self.logger.debug(f"Keeping None for nullable column {col_name} (type: {full_col_type})")
                                    else:
                                        # For non-nullable columns, use placeholder values
                                        # ClickHouse DateTime only supports dates from 1970-01-01 onwards (Unix epoch).
                                        # Using dates before 1970 (like 1900 or 1500) causes 'I' format errors because
                                        # clickhouse-driver tries to pack them as UInt32 timestamps, which fails.
                                        # We use 1970-01-01 (Unix epoch) as the placeholder, which matches toDateTime(0) in the schema.
                                        if 'DATE' in col_type_upper and 'DATETIME' not in col_type_upper:
                                            # Use placeholder date for non-nullable Date columns
                                            # ClickHouse Date supports dates from 1970-01-01 onwards (Unix epoch)
                                            value = date(1970, 1, 1)  # Use Unix epoch as placeholder (matches toDate(0) in schema)
                                            self.logger.debug(f"Converted None to placeholder date for non-nullable {col_name} (type: {full_col_type})")
                                        elif 'DATETIME' in col_type_upper:
                                            # Use placeholder datetime for non-nullable DateTime columns
                                            # ClickHouse DateTime supports dates from 1970-01-01 00:00:00 onwards (Unix epoch)
                                            value = datetime(1970, 1, 1, 0, 0, 0)  # Use Unix epoch as placeholder (matches toDateTime(0) in schema)
                                            self.logger.debug(f"Converted None to placeholder datetime for non-nullable {col_name} (type: {full_col_type})")
                                
                                # Verify the converted value is the right type for DateTime columns
                                # Ensure we never pass None to a non-nullable DateTime column (clickhouse-driver will fail with 'NoneType' object has no attribute 'tzinfo')
                                if 'DATETIME' in col_type_upper:
                                    if value is None:
                                        # This should not happen - we should have converted None to placeholder above
                                        # But just in case, convert it now (safety check)
                                        is_nullable = 'NULLABLE(' in col_type_upper or col_type_upper.startswith('NULLABLE')
                                        if not is_nullable:
                                            value = datetime(1970, 1, 1, 0, 0, 0)
                                            self.logger.warning(f"Had to convert None to placeholder datetime for {col_name} (type: {full_col_type}) - this should not happen")
                                    elif not isinstance(value, datetime):
                                        self.logger.warning(f"Value for {col_name} is not a datetime after conversion: {type(value).__name__}, value: {value}")
                                        # Try to convert it using the dedicated datetime conversion method
                                        converted = self._convert_datetime_for_clickhouse(value, col_type_upper)
                                        if converted is not None:
                                            value = converted
                                        else:
                                            # If conversion still fails, use placeholder
                                            # ClickHouse DateTime supports dates from 1970-01-01 00:00:00 onwards (Unix epoch)
                                            value = datetime(1970, 1, 1, 0, 0, 0)  # Use Unix epoch as placeholder (matches toDateTime(0) in schema)
                                
                                values.append(value)
                            else:
                                # Column not in DataFrame - use appropriate default based on type
                                col_type = clickhouse_types.get(col_name, '').upper()
                                default_value = self._get_default_value_for_clickhouse_type(col_type)
                                values.append(default_value)
                        
                        data_to_insert.append(tuple(values))
                    
                    if data_to_insert:
                        # Build INSERT statement with explicit column names (like v1)
                        columns_str = ", ".join(clickhouse_columns)
                        insert_query = f"INSERT INTO {self.etl_config.clickhouse_database}.{clickhouse_table} ({columns_str}) VALUES"
                        
                        # DEBUG SCRIPT SHOWED: Placeholder datetime values FAIL with 'I' format error
                        # This suggests clickhouse-driver is trying to pack datetime as UInt32, not DateTime
                        # The issue might be that we need to ensure datetime objects are in the right format
                        # or we need to use a different approach
                        try:
                            client.execute(insert_query, data_to_insert)
                        except Exception as e:
                            # If it fails with 'I' format error, the issue is with datetime packing
                            if "'I' format" in str(e) or "Type mismatch" in str(e):
                                self.logger.error(f"Type mismatch error with datetime: {e}")
                                self.logger.error(f"This suggests clickhouse-driver is packing datetime incorrectly")
                                self.logger.error(f"First row data types: {[type(v).__name__ for v in data_to_insert[0]]}")
                                self.logger.error(f"First row values: {data_to_insert[0]}")
                                raise
                            else:
                                raise
                        
                        self.logger.info(f"Successfully loaded {len(data_to_insert)} rows to ClickHouse table: {clickhouse_table}")
                        return True
                    else:
                        self.logger.warning(f"No data to insert after filtering columns")
                        return True
                        
                finally:
                    client.disconnect()
                
            except ImportError:
                # clickhouse-driver not available, fall back to JDBC
                self.logger.warning("clickhouse-driver not available, falling back to JDBC (may have auto-creation issues)")
                
                jdbc_options = {
                    "url": self.clickhouse_url,
                    "dbtable": clickhouse_table,  # Use simple table name (database is in URL)
                    "batchsize": str(optimal_batch_size),
                    **self.clickhouse_properties,
                    **kwargs
                }
                
                df.write.format("jdbc").mode(mode).options(**jdbc_options).save()
                
                self.logger.info(f"Successfully loaded data to ClickHouse table: {clickhouse_table} (mode: {mode}, batch_size: {optimal_batch_size})")
                return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to ClickHouse: {e}", exc_info=True)
            return False
    
    def _ensure_database_exists(self) -> None:
        """
        Ensure the ClickHouse database exists, create it if it doesn't.
        
        Uses JDBC connection via HTTP interface (port 8123) to execute CREATE DATABASE IF NOT EXISTS.
        Falls back to using clickhouse-driver if JDBC doesn't work.
        """
        try:
            # First, try using JDBC/HTTP interface (same as we use for data loading)
            try:
                # Connect to default database to create our database
                base_url = (
                    f"jdbc:clickhouse://{self.etl_config.clickhouse_host}:{self.etl_config.clickhouse_port}/"
                    f"default"  # Use 'default' database to create our database
                )
                
                # Add query parameters
                query_params = []
                if self.etl_config.clickhouse_password:
                    query_params.append(f"password={self.etl_config.clickhouse_password}")
                if self.etl_config.clickhouse_user:
                    query_params.append(f"user={self.etl_config.clickhouse_user}")
                
                query_string = "?" + "&".join(query_params) if query_params else ""
                base_url = base_url + query_string
                
                # Check if database exists using JDBC
                try:
                    # Try to query SHOW DATABASES
                    databases_df = self.spark.read.format("jdbc").options(
                        url=base_url,
                        query="SHOW DATABASES",
                        driver="com.clickhouse.jdbc.ClickHouseDriver"
                    ).load()
                    
                    # Collect database names
                    db_names = [row[0] for row in databases_df.collect()]
                    
                    if self.etl_config.clickhouse_database in db_names:
                        self.logger.debug(f"Database '{self.etl_config.clickhouse_database}' already exists")
                    else:
                        # Create database using JDBC
                        self.logger.info(f"Creating ClickHouse database '{self.etl_config.clickhouse_database}'...")
                        # Execute CREATE DATABASE via JDBC
                        # We'll use a simple query execution
                        create_db_df = self.spark.read.format("jdbc").options(
                            url=base_url,
                            query=f"CREATE DATABASE IF NOT EXISTS {self.etl_config.clickhouse_database}",
                            driver="com.clickhouse.jdbc.ClickHouseDriver"
                        ).load()
                        # Force execution by collecting (even if empty)
                        create_db_df.collect()
                        self.logger.info(f"✅ Created database '{self.etl_config.clickhouse_database}'")
                        
                except Exception as jdbc_error:
                    # JDBC approach failed, try clickhouse-driver as fallback
                    self.logger.debug(f"JDBC approach failed: {jdbc_error}. Trying clickhouse-driver...")
                    raise jdbc_error  # Re-raise to trigger fallback
                    
            except Exception:
                # Fallback to clickhouse-driver (native protocol)
                try:
                    from clickhouse_driver import Client
                    
                    # Try different native ports (9000, 9002, or infer from HTTP port)
                    # Docker-compose maps native port 9000 to host 9002
                    native_ports = [9002, 9000]  # Try 9002 first (docker-compose), then 9000
                    
                    client = None
                    for native_port in native_ports:
                        try:
                            client = Client(
                                host=self.etl_config.clickhouse_host,
                                port=native_port,
                                database='default',
                                user=self.etl_config.clickhouse_user,
                                password=self.etl_config.clickhouse_password if self.etl_config.clickhouse_password else ''
                            )
                            # Test connection
                            client.execute("SELECT 1")
                            self.logger.debug(f"Connected to ClickHouse native protocol on port {native_port}")
                            break
                        except Exception:
                            if client:
                                try:
                                    client.disconnect()
                                except:
                                    pass
                            client = None
                            continue
                    
                    if not client:
                        raise Exception("Could not connect to ClickHouse native protocol on any port")
                    
                    try:
                        # Check if database exists
                        databases = client.execute("SHOW DATABASES")
                        db_names = [row[0] for row in databases]
                        
                        if self.etl_config.clickhouse_database in db_names:
                            self.logger.debug(f"Database '{self.etl_config.clickhouse_database}' already exists")
                        else:
                            # Create database
                            self.logger.info(f"Creating ClickHouse database '{self.etl_config.clickhouse_database}'...")
                            client.execute(f"CREATE DATABASE IF NOT EXISTS {self.etl_config.clickhouse_database}")
                            self.logger.info(f"✅ Created database '{self.etl_config.clickhouse_database}'")
                        
                    finally:
                        client.disconnect()
                        
                except ImportError:
                    # clickhouse-driver not available
                    self.logger.warning(
                        f"clickhouse-driver not available. Cannot automatically create database '{self.etl_config.clickhouse_database}'. "
                        f"Please create it manually: CREATE DATABASE IF NOT EXISTS {self.etl_config.clickhouse_database}"
                    )
                except Exception as driver_error:
                    # Connection error or other issue with clickhouse-driver
                    self.logger.warning(
                        f"Could not create database '{self.etl_config.clickhouse_database}' using clickhouse-driver: {driver_error}. "
                        f"Please create it manually: CREATE DATABASE IF NOT EXISTS {self.etl_config.clickhouse_database}"
                    )
                
        except Exception as e:
            self.logger.warning(
                f"Could not ensure database '{self.etl_config.clickhouse_database}' exists: {e}. "
                f"Please create it manually if it doesn't exist."
            )
    
    def _verify_table_exists(self, clickhouse_table: str) -> bool:
        """
        Verify that a ClickHouse table exists.
        
        Args:
            clickhouse_table: ClickHouse table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            # Try using JDBC first
            try:
                # Use SHOW TABLES to check if table exists (more reliable than SELECT)
                tables_df = self.spark.read.format("jdbc").options(
                    url=self.clickhouse_url,
                    query=f"SHOW TABLES FROM {self.etl_config.clickhouse_database} LIKE '{clickhouse_table}'",
                    driver="com.clickhouse.jdbc.ClickHouseDriver"
                ).load()
                
                # Check if table exists in results
                table_list = [row[0] for row in tables_df.collect()]
                if clickhouse_table in table_list:
                    return True
                
                # If not found, try direct query as fallback
                test_df = self.spark.read.format("jdbc").options(
                    url=self.clickhouse_url,
                    query=f"SELECT 1 FROM {clickhouse_table} LIMIT 1",
                    driver="com.clickhouse.jdbc.ClickHouseDriver"
                ).load()
                # If we get here, table exists
                return True
            except Exception as jdbc_error:
                # JDBC approach failed, try clickhouse-driver as fallback
                try:
                    from clickhouse_driver import Client
                    
                    # Try different native ports
                    native_ports = [9002, 9000]
                    client = None
                    for native_port in native_ports:
                        try:
                            client = Client(
                                host=self.etl_config.clickhouse_host,
                                port=native_port,
                                database=self.etl_config.clickhouse_database,
                                user=self.etl_config.clickhouse_user,
                                password=self.etl_config.clickhouse_password if self.etl_config.clickhouse_password else ''
                            )
                            client.execute("SELECT 1")
                            break
                        except Exception:
                            if client:
                                try:
                                    client.disconnect()
                                except:
                                    pass
                            client = None
                            continue
                    
                    if client:
                        try:
                            # Check if table exists using SHOW TABLES
                            tables = client.execute(f"SHOW TABLES FROM {self.etl_config.clickhouse_database} LIKE '{clickhouse_table}'")
                            if tables and len(tables) > 0 and tables[0][0] == clickhouse_table:
                                return True
                            
                            # Fallback: try to query the table
                            client.execute(f"SELECT 1 FROM {clickhouse_table} LIMIT 1")
                            return True
                        finally:
                            client.disconnect()
                    return False
                except ImportError:
                    # clickhouse-driver not available
                    return False
                except Exception:
                    # Table doesn't exist
                    return False
        except Exception:
            # Table doesn't exist
            return False
    
    def _ensure_table_exists(self, clickhouse_table: str) -> None:
        """
        Verify that the ClickHouse table exists.
        
        Tables are created via init_clickhouse.sh script, so this method only verifies
        that the table exists and logs a warning if it doesn't.
        
        Args:
            clickhouse_table: ClickHouse table name
        """
        if not self._verify_table_exists(clickhouse_table):
            self.logger.warning(
                f"Table '{clickhouse_table}' does not exist. "
                f"Please run init_clickhouse.sh to create all tables from schema files."
            )
    
    def _get_table_config(self, postgresql_table: str):
        """
        Get table-specific ClickHouse configuration.
        
        Args:
            postgresql_table: PostgreSQL table name
            
        Returns:
            TableLoadConfig for ClickHouse destination, or None
        """
        return get_table_load_config(postgresql_table, "clickhouse")
    
    def _convert_datetime_for_clickhouse(self, value: Any, col_type: str) -> Any:
        """
        Convert a value to a datetime or date object for ClickHouse DateTime/Date columns.
        
        Systematically handles all possible input types:
        1. None values → returns None
        2. datetime objects → handles timezone conversion, Date vs DateTime
        3. date objects (not datetime) → converts appropriately
        4. pandas Timestamp → handled by isinstance(value, datetime) check
        5. numpy datetime64 → converts to Python datetime
        6. integer/float Unix timestamps → detects seconds vs milliseconds
        7. string timestamps → ISO format, common formats, timezone-aware strings
        8. Other types → returns None
        
        Args:
            value: Value to convert (datetime, date, int, float, str, numpy datetime64, or None)
            col_type: ClickHouse column type (e.g., 'DateTime', 'Date', 'Nullable(DateTime)')
            
        Returns:
            datetime object for DateTime columns
            date object for Date columns
            None if value is None or conversion fails
        """
        if value is None:
            return None
        
        col_type_upper = col_type.upper()
        is_date_only = 'DATE' in col_type_upper and 'DATETIME' not in col_type_upper
        
        # Case 1: Handle datetime objects (including pandas Timestamp which is a subclass)
        if isinstance(value, datetime):
            # For Date columns, convert datetime to date
            if is_date_only:
                return date(value.year, value.month, value.day)
            # For DateTime columns, ensure timezone-naive datetime
            # ClickHouse DateTime doesn't support timezone, so remove tzinfo if present
            if value.tzinfo is not None:
                # Convert to UTC and remove timezone info
                from datetime import timezone
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return value
        
        # Case 2: Handle date objects (not datetime)
        if isinstance(value, date) and not isinstance(value, datetime):
            # For Date columns, return as-is
            if is_date_only:
                return value
            # For DateTime columns, convert date to datetime (midnight)
            return datetime(value.year, value.month, value.day, 0, 0, 0)
        
        # Case 3: Handle numpy datetime64 objects
        # Check by type name to avoid importing numpy if not needed
        if hasattr(value, '__class__') and 'datetime64' in str(type(value)):
            try:
                # Convert numpy datetime64 to Python datetime
                import numpy as np
                if isinstance(value, np.datetime64):
                    # Convert to pandas Timestamp first (handles timezone better), then to datetime
                    try:
                        import pandas as pd
                        ts = pd.Timestamp(value)
                        dt = ts.to_pydatetime()
                    except (ImportError, AttributeError):
                        # Fallback: convert directly via string
                        dt = datetime.fromisoformat(str(value).replace('T', ' ').split('.')[0])
                    
                    # For Date columns, convert datetime to date
                    if is_date_only:
                        return date(dt.year, dt.month, dt.day)
                    # Ensure timezone-naive
                    if dt.tzinfo is not None:
                        from datetime import timezone
                        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    return dt
            except (ImportError, ValueError, OSError, OverflowError):
                # If conversion fails, fall through to other handlers
                pass
        
        # Case 4: Handle integer/float Unix timestamps (seconds or milliseconds since epoch)
        # Spark might return timestamps as integers
        if isinstance(value, (int, float)):
            try:
                # More robust detection: milliseconds typically have 13+ digits, seconds have 10 digits
                # But also check reasonable date ranges to avoid false positives
                # Epoch seconds: 0 to ~2.1e9 (year 2038 problem)
                # Epoch milliseconds: 0 to ~2.1e12
                # Anything > 1e12 is likely milliseconds, but also check if it's a reasonable date
                if value > 1e12:
                    # Likely milliseconds - divide by 1000
                    dt = datetime.fromtimestamp(value / 1000.0)
                elif value < 0:
                    # Negative timestamp (before 1970) - treat as seconds
                    dt = datetime.fromtimestamp(float(value))
                else:
                    # Could be seconds or milliseconds - try seconds first
                    # If result is before 1970, it's likely milliseconds
                    dt = datetime.fromtimestamp(float(value))
                    if dt.year < 1970 and value > 0:
                        # Likely milliseconds, try again
                        dt = datetime.fromtimestamp(value / 1000.0)
                
                # For Date columns, convert datetime to date
                if is_date_only:
                    return date(dt.year, dt.month, dt.day)
                return dt
            except (ValueError, OSError, OverflowError):
                # If timestamp conversion fails, return None (will be handled by placeholder logic)
                return None
        
        # Case 5: Handle string timestamps
        if isinstance(value, str):
            # Strip whitespace
            value = value.strip()
            if not value:
                return None
            
            try:
                # Try ISO format first (e.g., '2023-01-01T12:00:00Z' or '2023-01-01T12:00:00+00:00')
                # Check for ISO format (contains 'T') or space-separated datetime (has space at position 10)
                is_iso_format = 'T' in value
                is_space_separated = len(value) > 10 and value[10] == ' '
                
                if is_iso_format or is_space_separated:
                    # ISO format or space-separated datetime
                    # Handle 'Z' timezone indicator
                    iso_str = value.replace('Z', '+00:00')
                    try:
                        dt = datetime.fromisoformat(iso_str)
                    except ValueError:
                        # Try parsing with dateutil if available (handles more formats)
                        try:
                            from dateutil import parser
                            dt = parser.parse(value)
                        except (ImportError, ValueError):
                            # Fall back to strptime with common formats
                            raise ValueError("Cannot parse ISO format")
                    
                    # Remove timezone info for ClickHouse
                    if dt.tzinfo is not None:
                        from datetime import timezone
                        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    # For Date columns, convert datetime to date
                    if is_date_only:
                        return date(dt.year, dt.month, dt.day)
                    return dt
                
                # Try common string formats (ordered by likelihood)
                formats = [
                    '%Y-%m-%d %H:%M:%S.%f',  # With microseconds
                    '%Y-%m-%d %H:%M:%S',     # Standard datetime
                    '%Y-%m-%d',              # Date only
                    '%m/%d/%Y %H:%M:%S',    # US format
                    '%d/%m/%Y %H:%M:%S',    # European format
                    '%Y%m%d %H%M%S',        # Compact format
                ]
                
                for fmt in formats:
                    try:
                        dt = datetime.strptime(value, fmt)
                        # For Date columns, convert datetime to date
                        if is_date_only:
                            return date(dt.year, dt.month, dt.day)
                        return dt
                    except ValueError:
                        continue
                
                # If all fail, return None (will be handled by placeholder logic)
                return None
            except (ValueError, TypeError, AttributeError):
                return None
        
        # Case 6: Unknown type - return None
        # This handles any other types we haven't accounted for
        return None
    
    def _convert_value_for_clickhouse(self, value: Any, col_type: str, col_name: str) -> Any:
        """
        Convert a value to match ClickHouse column type automatically.
        
        This method handles all ClickHouse types intelligently:
        - Integer types (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64)
        - Float types (Float32, Float64)
        - String types (String, FixedString)
        - DateTime types (DateTime, DateTime64, Date)
        - Boolean (converted to UInt8)
        - Nullable types
        - Array types
        - Other types
        
        Args:
            value: Value to convert
            col_type: ClickHouse column type (e.g., 'UInt8', 'String', 'DateTime')
            col_name: Column name (for context in datetime conversion)
            
        Returns:
            Converted value appropriate for ClickHouse column type
        """
        if value is None:
            # Handle None based on type
            col_type_upper = col_type.upper()
            
            # Check if column is nullable first
            is_nullable = 'NULLABLE(' in col_type_upper or col_type_upper.startswith('NULLABLE')
            
            # DateTime/Date types - handle None based on nullable status
            if 'DATETIME' in col_type_upper or 'DATE' in col_type_upper:
                if is_nullable:
                    # For nullable Date/DateTime columns, return None - clickhouse-driver handles it
                    return None
                else:
                    # For non-nullable Date/DateTime columns, use placeholder value
                    # ClickHouse DateTime/Date only supports dates from 1970-01-01 onwards (Unix epoch)
                    if 'DATE' in col_type_upper and 'DATETIME' not in col_type_upper:
                        # Use placeholder date for non-nullable Date columns
                        return date(1970, 1, 1)  # Use Unix epoch as placeholder (matches toDate(0) in schema)
                    elif 'DATETIME' in col_type_upper:
                        # Use placeholder datetime for non-nullable DateTime columns
                        return datetime(1970, 1, 1, 0, 0, 0)  # Use Unix epoch as placeholder (matches toDateTime(0) in schema)
                    # Fallback (shouldn't reach here)
                    return None
            
            # Integer types - default to 0
            if any(col_type_upper.startswith(t) for t in ['INT8', 'INT16', 'INT32', 'INT64', 'UINT8', 'UINT16', 'UINT32', 'UINT64', 'INT', 'UINT']):
                return 0
            
            # Float types - default to 0.0
            if any(col_type_upper.startswith(t) for t in ['FLOAT32', 'FLOAT64', 'FLOAT', 'DOUBLE', 'DECIMAL']):
                return 0.0
            
            # String types - default to empty string (ClickHouse can't encode None for strings)
            if any(col_type_upper.startswith(t) for t in ['STRING', 'FIXEDSTRING']):
                return ''
            
            # Array types - default to empty list
            if 'ARRAY' in col_type_upper:
                return []
            
            # Default - return None
            return None
        
        col_type_upper = col_type.upper()
        
        # Handle Nullable types - extract inner type
        inner_type = col_type_upper
        if 'NULLABLE(' in col_type_upper:
            # Extract type inside Nullable(...)
            start = col_type_upper.find('NULLABLE(') + 9
            end = col_type_upper.rfind(')')
            if end > start:
                inner_type = col_type_upper[start:end].strip()
        
        # Integer types (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64)
        if any(inner_type.startswith(t) for t in ['INT8', 'INT16', 'INT32', 'INT64', 'UINT8', 'UINT16', 'UINT32', 'UINT64', 'INT', 'UINT']):
            if isinstance(value, bool):
                return 1 if value else 0
            elif isinstance(value, int):
                return value
            elif isinstance(value, float):
                return int(value)
            elif isinstance(value, str):
                # Try to convert string to int
                try:
                    return int(value)
                except ValueError:
                    # Try boolean-like strings
                    if value.lower() in ('true', '1', 'yes', 'y', 'on'):
                        return 1
                    elif value.lower() in ('false', '0', 'no', 'n', 'off', ''):
                        return 0
                    return 0
            else:
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0
        
        # Float types (Float32, Float64, Double, Decimal)
        elif any(inner_type.startswith(t) for t in ['FLOAT32', 'FLOAT64', 'FLOAT', 'DOUBLE', 'DECIMAL']):
            if isinstance(value, float):
                return value
            elif isinstance(value, int):
                return float(value)
            elif isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    return 0.0
            else:
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0
        
        # String types (String, FixedString)
        elif any(inner_type.startswith(t) for t in ['STRING', 'FIXEDSTRING']):
            if isinstance(value, str):
                return value
            else:
                return str(value) if value is not None else ''
        
        # DateTime types (DateTime, DateTime64, Date)
        elif 'DATETIME' in inner_type or 'DATE' in inner_type:
            return self._convert_datetime_for_clickhouse(value, inner_type)
        
        # Boolean - ClickHouse doesn't have native boolean, convert to UInt8
        elif inner_type == 'BOOL' or inner_type == 'BOOLEAN':
            if isinstance(value, bool):
                return 1 if value else 0
            elif isinstance(value, str):
                return 1 if value.lower() in ('true', '1', 'yes', 'y', 'on') else 0
            elif isinstance(value, int):
                return 1 if value != 0 else 0
            else:
                return 1 if bool(value) else 0
        
        # Array types - keep as is (ClickHouse driver handles arrays)
        elif 'ARRAY' in inner_type:
            if isinstance(value, (list, tuple)):
                return list(value)
            else:
                return [value] if value is not None else []
        
        # UUID - convert to string
        elif inner_type == 'UUID':
            return str(value) if value is not None else ''
        
        # JSON/Object types - convert to string
        elif any(t in inner_type for t in ['JSON', 'OBJECT', 'MAP']):
            if isinstance(value, str):
                return value
            else:
                import json
                try:
                    return json.dumps(value) if value is not None else '{}'
                except (TypeError, ValueError):
                    return str(value) if value is not None else '{}'
        
        # Default - return as is or convert to string
        else:
            return value
    
    def _get_default_value_for_clickhouse_type(self, col_type: str) -> Any:
        """
        Get default value for a ClickHouse column type when column is missing from DataFrame.
        
        Args:
            col_type: ClickHouse column type
            
        Returns:
            Default value appropriate for the column type
        """
        col_type_upper = col_type.upper()
        
        # Handle Nullable types
        if 'NULLABLE(' in col_type_upper:
            return None
        
        # Integer types
        if any(col_type_upper.startswith(t) for t in ['INT8', 'INT16', 'INT32', 'INT64', 'UINT8', 'UINT16', 'UINT32', 'UINT64', 'INT', 'UINT']):
            return 0
        
        # Float types
        if any(col_type_upper.startswith(t) for t in ['FLOAT32', 'FLOAT64', 'FLOAT', 'DOUBLE', 'DECIMAL']):
            return 0.0
        
        # String types
        if any(col_type_upper.startswith(t) for t in ['STRING', 'FIXEDSTRING']):
            return ''
        
        # DateTime/Date types - use placeholder for non-nullable columns
        # For non-nullable DateTime/Date columns, use placeholder (1970-01-01) to avoid None errors
        if 'DATETIME' in col_type_upper or 'DATE' in col_type_upper:
            # Check if column is nullable
            is_nullable = 'NULLABLE(' in col_type_upper or col_type_upper.startswith('NULLABLE')
            if is_nullable:
                # For nullable columns, return None - clickhouse-driver handles it
                return None
            else:
                # For non-nullable columns, use placeholder value (1970-01-01)
                # ClickHouse DateTime/Date only supports dates from 1970-01-01 onwards (Unix epoch)
                if 'DATE' in col_type_upper and 'DATETIME' not in col_type_upper:
                    return date(1970, 1, 1)  # Use Unix epoch as placeholder (matches toDate(0) in schema)
                elif 'DATETIME' in col_type_upper:
                    return datetime(1970, 1, 1, 0, 0, 0)  # Use Unix epoch as placeholder (matches toDateTime(0) in schema)
                return None
        
        # Array types
        if 'ARRAY' in col_type_upper:
            return []
        
        # Boolean
        if col_type_upper in ('BOOL', 'BOOLEAN'):
            return 0
        
        # UUID
        if col_type_upper == 'UUID':
            return ''
        
        # JSON/Object types
        if any(t in col_type_upper for t in ['JSON', 'OBJECT', 'MAP']):
            return '{}'
        
        # Default
        return None
    
    def _get_clickhouse_table_name(self, postgresql_table: str) -> str:
        """
        Get ClickHouse table name from PostgreSQL table name.
        
        Args:
            postgresql_table: PostgreSQL table name
            
        Returns:
            ClickHouse table name
        """
        # Check if table has ClickHouse configuration
        if postgresql_table in TABLE_LOAD_CONFIGS:
            clickhouse_config = TABLE_LOAD_CONFIGS[postgresql_table].get("clickhouse")
            if clickhouse_config and clickhouse_config.clickhouse_table:
                return clickhouse_config.clickhouse_table
        
        # Default: use same table name
        return postgresql_table
    
    def _convert_types_for_clickhouse(self, df: DataFrame, destination: str) -> DataFrame:
        """
        Convert PostgreSQL data types to ClickHouse-compatible types.
        
        ClickHouse type mappings:
        - UUID → String
        - JSONB → String (JSON serialized)
        - Boolean → UInt8 (0 or 1)
        - Timestamp → DateTime
        - Array → Array (if compatible)
        
        Args:
            df: DataFrame to convert
            destination: Table name (for context)
            
        Returns:
            DataFrame with converted types
        """
        converted_df = df
        
        for col_name in df.columns:
            col_type = df.schema[col_name].dataType
            
            # Boolean columns → UInt8 (0 or 1)
            # ClickHouse uses UInt8 for boolean values (0 = false, 1 = true)
            if isinstance(col_type, BooleanType):
                # Cast boolean to integer (True → 1, False → 0, None → None)
                converted_df = converted_df.withColumn(
                    col_name,
                    col(col_name).cast(IntegerType())
                )
            
            # UUID columns → String
            # UUIDs from PostgreSQL are already strings in Spark, so no conversion needed
            elif isinstance(col_type, StringType):
                # Already String in Spark, no conversion needed
                pass
            
            # JSONB/JSON columns → String
            # In Spark, JSONB from PostgreSQL is typically already String or StructType
            elif isinstance(col_type, (StructType, MapType)):
                # Convert struct/map to JSON string
                converted_df = converted_df.withColumn(
                    col_name,
                    to_json(col(col_name)).cast(StringType())
                )
            
            # Array types - ClickHouse supports arrays, but need to ensure compatibility
            elif isinstance(col_type, ArrayType):
                # Arrays are generally compatible, but ensure element types match
                # ClickHouse arrays can contain: String, Int*, UInt*, Float*, Date, DateTime
                element_type = col_type.elementType
                if isinstance(element_type, StringType):
                    # String arrays are compatible
                    pass
                elif isinstance(element_type, BooleanType):
                    # Array of booleans → Array of UInt8
                    converted_df = converted_df.withColumn(
                        col_name,
                        col(col_name).cast(ArrayType(IntegerType()))
                    )
                elif isinstance(element_type, (StructType, MapType)):
                    # Array of structs/maps → Array of JSON strings
                    converted_df = converted_df.withColumn(
                        col_name,
                        col(col_name).cast(ArrayType(StringType()))
                    )
                    self.logger.warning(
                        f"Array of structs/maps in column {col_name} - "
                        f"converted to Array(String)"
                    )
        
        return converted_df
    
    def _get_clickhouse_table_columns(self, table_name: str) -> Set[str]:
        """
        Get column names from ClickHouse schema files.
        
        Parses SQL schema files to extract table column definitions.
        
        Args:
            table_name: ClickHouse table name
            
        Returns:
            Set of column names in the table
        """
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]
        
        columns = set()
        
        # Search for table definition in schema files
        if not self._schema_dir.exists():
            self.logger.warning(f"ClickHouse schema directory not found: {self._schema_dir}")
            return columns
        
        # Pattern to match CREATE TABLE statements
        # Matches: CREATE TABLE IF NOT EXISTS table_name ( ... )
        table_pattern = re.compile(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
        
        # Pattern to match column definitions
        # Matches: column_name Type, or column_name Type DEFAULT, etc.
        column_pattern = re.compile(
            r'^\s*`?(\w+)`?\s+',  # Column name at start of line
            re.IGNORECASE | re.MULTILINE
        )
        
        # Read all SQL files in schema directory
        for sql_file in self._schema_dir.glob("*.sql"):
            try:
                with open(sql_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Find all table definitions
                    for match in table_pattern.finditer(content):
                        found_table_name = match.group(1)
                        
                        # Check if this is the table we're looking for
                        # Match exact name or with _ref suffix (for reference tables)
                        if found_table_name == table_name or found_table_name == f"{table_name}_ref" or found_table_name == table_name.replace("_ref", ""):
                            # Extract columns from this table definition
                            # Find the position of the opening parenthesis
                            start_pos = match.end()
                            # Find matching closing parenthesis
                            paren_count = 1
                            end_pos = start_pos
                            
                            for i, char in enumerate(content[start_pos:], start_pos):
                                if char == '(':
                                    paren_count += 1
                                elif char == ')':
                                    paren_count -= 1
                                    if paren_count == 0:
                                        end_pos = i
                                        break
                            
                            # Extract table definition
                            table_def = content[start_pos:end_pos]
                            
                            # Extract column names - better pattern that matches column definitions
                            # Pattern: column_name Type, or column_name Type DEFAULT, etc.
                            # Split by lines and parse each line
                            for line in table_def.split('\n'):
                                line = line.strip()
                                # Skip empty lines and comments
                                if not line or line.startswith('--'):
                                    continue
                                
                                # Skip table-level keywords
                                if any(keyword in line.upper() for keyword in ['ENGINE', 'ORDER BY', 'PARTITION BY', 'TTL', 'SETTINGS']):
                                    break  # End of column definitions
                                
                                # Match column definition: column_name Type
                                # Examples:
                                #   column_name String,
                                #   column_name String DEFAULT 1,
                                #   column_name String CODEC(ZSTD(3)),
                                col_match = re.match(r'^`?(\w+)`?\s+', line, re.IGNORECASE)
                                if col_match:
                                    col_name = col_match.group(1)
                                    # Skip keywords
                                    if col_name.upper() not in ['ENGINE', 'ORDER', 'PARTITION', 'TTL', 'SETTINGS', 'MATERIALIZED']:
                                        columns.add(col_name)
                            
                            # Cache the result
                            self._schema_cache[table_name] = columns
                            self.logger.debug(f"Found {len(columns)} columns for table {table_name} in {sql_file.name}")
                            return columns
                            
            except Exception as e:
                self.logger.warning(f"Could not parse schema file {sql_file}: {e}")
        
        # If not found, cache empty set to avoid repeated searches
        self._schema_cache[table_name] = columns
        if not columns:
            self.logger.warning(f"Could not find table {table_name} in ClickHouse schema files")
        
        return columns
    
    def _filter_columns_for_clickhouse(self, df: DataFrame, clickhouse_table: str) -> DataFrame:
        """
        Filter DataFrame columns to match ClickHouse table schema.
        
        Uses parsed schema files to determine which columns exist in ClickHouse table.
        
        Args:
            df: DataFrame to filter
            clickhouse_table: ClickHouse table name
            
        Returns:
            Filtered DataFrame with only columns that exist in ClickHouse table
        """
        # Get ClickHouse table columns from schema files
        clickhouse_columns = self._get_clickhouse_table_columns(clickhouse_table)
        
        if not clickhouse_columns:
            # If we can't find the schema, log warning and return DataFrame as-is
            # ClickHouse JDBC driver will handle column mismatches
            self.logger.warning(
                f"Could not determine columns for ClickHouse table {clickhouse_table}. "
                f"Using all DataFrame columns."
            )
            return df
        
        # Filter DataFrame to only include columns that exist in ClickHouse table
        df_columns = set(df.columns)
        common_columns = df_columns.intersection(clickhouse_columns)
        
        if not common_columns:
            self.logger.error(
                f"No common columns between DataFrame and ClickHouse table {clickhouse_table}"
            )
            self.logger.error(f"DataFrame columns: {sorted(df_columns)}")
            self.logger.error(f"ClickHouse columns: {sorted(clickhouse_columns)}")
            return df
        
        # Select only common columns if there's a mismatch
        if common_columns != df_columns:
            missing_cols = df_columns - common_columns
            extra_cols = clickhouse_columns - df_columns
            self.logger.warning(
                f"DataFrame has columns not in ClickHouse table {clickhouse_table}: {missing_cols}. "
                f"These will be skipped."
            )
            if extra_cols:
                self.logger.info(
                    f"ClickHouse table {clickhouse_table} has columns not in DataFrame: {extra_cols}. "
                    f"These will use their default values."
                )
            df = df.select(*sorted(common_columns))
        
        return df
    
    def _get_table_engine(self, table_name: str) -> Optional[str]:
        """
        Get table engine type from ClickHouse schema files.
        
        Args:
            table_name: ClickHouse table name
            
        Returns:
            Table engine type (e.g., "MergeTree", "ReplacingMergeTree", "SummingMergeTree") or None
        """
        # Check cache first
        if table_name in self._table_engine_cache:
            return self._table_engine_cache[table_name]
        
        engine = None
        
        # Search for table definition in schema files
        if not self._schema_dir.exists():
            return None
        
        # Pattern to match ENGINE clause
        # Matches: ENGINE = MergeTree() or ENGINE = ReplacingMergeTree(updated_at)
        engine_pattern = re.compile(
            r'ENGINE\s*=\s*(\w+)(?:\([^)]*\))?',
            re.IGNORECASE | re.MULTILINE
        )
        
        # Pattern to match CREATE TABLE statements
        table_pattern = re.compile(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\(',
            re.IGNORECASE | re.MULTILINE
        )
        
        # Read all SQL files in schema directory
        for sql_file in self._schema_dir.glob("*.sql"):
            try:
                with open(sql_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Find all table definitions
                    for match in table_pattern.finditer(content):
                        found_table_name = match.group(1)
                        
                        # Check if this is the table we're looking for
                        if found_table_name == table_name or found_table_name == f"{table_name}_ref" or found_table_name == table_name.replace("_ref", ""):
                            # Find ENGINE clause after this table definition
                            # Search from the table definition to the end of the CREATE TABLE statement
                            start_pos = match.end()
                            # Find matching closing parenthesis for CREATE TABLE
                            paren_count = 1
                            end_pos = start_pos
                            
                            for i, char in enumerate(content[start_pos:], start_pos):
                                if char == '(':
                                    paren_count += 1
                                elif char == ')':
                                    paren_count -= 1
                                    if paren_count == 0:
                                        end_pos = i
                                        break
                            
                            # Extract table definition (include extra for ENGINE clause)
                            table_def = content[start_pos:end_pos + 200]
                            
                            # Find ENGINE clause
                            engine_match = engine_pattern.search(table_def)
                            if engine_match:
                                engine = engine_match.group(1)
                                # Cache the result
                                self._table_engine_cache[table_name] = engine
                                self.logger.debug(f"Found engine {engine} for table {table_name} in {sql_file.name}")
                                return engine
                            
            except Exception as e:
                self.logger.warning(f"Could not parse schema file {sql_file} for engine: {e}")
        
        # If not found, cache None to avoid repeated searches
        self._table_engine_cache[table_name] = engine
        return engine
    
    def _is_replacing_merge_tree(self, table_name: str) -> bool:
        """
        Check if table uses ReplacingMergeTree engine.
        
        Args:
            table_name: ClickHouse table name
            
        Returns:
            True if table uses ReplacingMergeTree, False otherwise
        """
        engine = self._get_table_engine(table_name)
        return engine == "ReplacingMergeTree" if engine else False
    
    def _replace_partition(
        self,
        df: DataFrame,
        clickhouse_table: str,
        destination: str,
        batch_size: int,
        **kwargs
    ) -> bool:
        """
        Replace partition in ClickHouse table.
        
        This is used for daily/hourly aggregations where we want to replace
        the entire partition with new data.
        
        Note: ClickHouse JDBC doesn't support direct partition replacement.
        This method uses append mode and logs partition information.
        For true partition replacement, use ClickHouse client library.
        
        Args:
            df: DataFrame to load
            clickhouse_table: ClickHouse table name
            destination: PostgreSQL table name (for config lookup)
            batch_size: Batch size for JDBC writes
            **kwargs: Additional JDBC options
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            # Get partition information from config
            table_config = self._get_table_config(destination)
            partition_expr = None
            if table_config and table_config.clickhouse_partition:
                partition_expr = table_config.clickhouse_partition
            
            if not partition_expr:
                self.logger.warning(
                    f"No partition expression found for {clickhouse_table}. "
                    f"Falling back to append mode."
                )
                return self.load(df, destination, mode="append", batch_size=batch_size, **kwargs)
            
            # Extract partition value from DataFrame
            # Partition expression is like "toYYYYMM(session_start)" or "toYYYYMM(event_date)"
            # We need to extract the column name and get the partition value
            partition_col_match = re.search(r'toYYYYMM\((\w+)\)', partition_expr, re.IGNORECASE)
            if not partition_col_match:
                self.logger.warning(
                    f"Could not parse partition expression {partition_expr}. "
                    f"Falling back to append mode."
                )
                return self.load(df, destination, mode="append", batch_size=batch_size, **kwargs)
            
            partition_col = partition_col_match.group(1)
            
            if partition_col not in df.columns:
                self.logger.warning(
                    f"Partition column {partition_col} not found in DataFrame. "
                    f"Falling back to append mode."
                )
                return self.load(df, destination, mode="append", batch_size=batch_size, **kwargs)
            
            # Get unique partition values from DataFrame
            partition_values = df.select(partition_col).distinct().collect()
            
            if not partition_values:
                self.logger.warning(f"No partition values found in DataFrame. Skipping replace partition.")
                return False
            
            # For ClickHouse, replace partition means:
            # 1. Delete the partition (requires ClickHouse client)
            # 2. Insert new data
            # Note: ClickHouse JDBC doesn't support partition replacement
            # We'll use append mode and log partition information
            # In production, you'd want to use a ClickHouse client library for partition operations
            
            partition_info = [str(row[partition_col]) for row in partition_values[:10]]  # Limit to first 10
            self.logger.info(
                f"Replace partition mode requested for {clickhouse_table}. "
                f"Partition values: {partition_info}{'...' if len(partition_values) > 10 else ''} "
                f"(total: {len(partition_values)}). "
                f"Using append mode (ClickHouse JDBC doesn't support partition replacement)."
            )
            
            # Use append mode (ClickHouse will handle deduplication if using ReplacingMergeTree)
            return self.load(df, destination, mode="append", batch_size=batch_size, **kwargs)
            
        except Exception as e:
            self.logger.error(f"Error in replace partition for {clickhouse_table}: {e}", exc_info=True)
            return False
