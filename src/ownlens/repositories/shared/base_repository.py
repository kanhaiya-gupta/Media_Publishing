"""
OwnLens - Base Repository Class
================================

Database-agnostic base repository with type safety and modern patterns.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union, Type, TypeVar, Generic
from datetime import datetime, date
from uuid import UUID
import logging
import json
import os
import re

from .query_builder import (
    QueryBuilder, QueryType, OrderDirection, PaginationParams,
    select, select_all, count, insert_into, update_table, delete_from
)

logger = logging.getLogger(__name__)

T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Database-agnostic base repository class.
    
    Provides common CRUD operations with type safety and database abstraction.
    Integrates with schemas for modern database operations.
    """
    
    def __init__(self, connection_manager, table_name: str):
        """
        Initialize the repository.
        
        Args:
            connection_manager: Database connection manager
            table_name: Name of the primary table
        """
        self.connection_manager = connection_manager
        self.table_name = table_name
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._initialized = False
        self._json_fields_cache = None  # Cache for JSON fields discovery
        self._array_fields_cache = None  # Cache for array fields discovery
    
    @abstractmethod
    def get_primary_key(self) -> str:
        """Get the primary key column name."""
        pass
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Get the table name for this repository."""
        pass
    
    # ============================================================================
    # Utility Methods
    # ============================================================================
    
    def _get_json_fields_from_schemas(self) -> set:
        """
        Dynamically discover JSONB fields from all SQL schema files.
        Uses caching for better performance.
        
        Returns:
            Set of all JSONB field names found in schema files
        """
        # Return cached result if available
        if self._json_fields_cache is not None:
            return self._json_fields_cache
        
        json_fields = set()
        schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'schemas')
        
        if not os.path.exists(schema_dir):
            # No fallback - log error and return empty set
            self.logger.error(f"Schema directory not found: {schema_dir}")
            self._json_fields_cache = set()
            return self._json_fields_cache
        
        try:
            # Walk through schema directory
            for root, dirs, files in os.walk(schema_dir):
                for file in files:
                    if file.endswith('.sql'):
                        filepath = os.path.join(root, file)
                        try:
                            with open(filepath, 'r', encoding='utf-8') as f:
                                content = f.read()
                                # Find all JSONB column definitions
                                # Pattern: field_name JSONB or field_name JSONB DEFAULT
                                # Examples: metadata JSONB DEFAULT '{}'::jsonb
                                matches = re.findall(r'(\w+)\s+JSONB(?:\s+DEFAULT|\s*,|\s*\)|\s*--)', content, re.IGNORECASE)
                                json_fields.update(matches)
                        except Exception as e:
                            self.logger.warning(f"Could not read schema file {filepath}: {e}")
        except Exception as e:
            self.logger.warning(f"Could not scan schema directory {schema_dir}: {e}")
        
        # Cache the result
        self._json_fields_cache = json_fields
        self.logger.info(f"Discovered {len(json_fields)} JSONB fields from schemas")
        
        return json_fields

    def _get_array_fields_from_schemas(self) -> set:
        """
        Dynamically discover array fields from all SQL schema files.
        Uses caching for better performance.
        
        Returns:
            Set of all array field names found in schema files
        """
        # Return cached result if available
        if self._array_fields_cache is not None:
            return self._array_fields_cache
        
        array_fields = set()
        schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'schemas')
        
        if not os.path.exists(schema_dir):
            # No fallback - log error and return empty set
            self.logger.error(f"Schema directory not found: {schema_dir}")
            self._array_fields_cache = set()
            return self._array_fields_cache

        # Search for schema files
        for root, dirs, files in os.walk(schema_dir):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            
                        # Look for PostgreSQL array column definitions
                        # Pattern: field_name TYPE[] or field_name ARRAY
                        # Examples: tags VARCHAR(100)[], resource_ids UUID[], language_codes CHAR(5)[]
                        # Match field name before the array type
                        array_matches = re.findall(r'(\w+)\s+\w+(?:\([^)]+\))?\[\](?:\s*,|\s*--|\s*\))', content, re.IGNORECASE)
                        array_fields.update(array_matches)
                        
                    except Exception as e:
                        self.logger.warning(f"Error reading schema file {file_path}: {e}")
                        continue
        
        # Cache the result
        self._array_fields_cache = array_fields
        self.logger.info(f"Discovered {len(array_fields)} array fields from schemas")
        
        return array_fields

    def _convert_json_to_lists(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert JSON strings back to lists for Pydantic models.
        Also converts IPv4Address/IPv6Address objects to strings.
        
        Args:
            data: Dictionary containing the record data
            
        Returns:
            Dictionary with JSON strings converted back to lists and IP addresses converted to strings
        """
        converted_data = data.copy()
        
        # Convert IPv4Address/IPv6Address objects to strings
        # PostgreSQL INET type is returned as IPv4Address/IPv6Address by asyncpg
        try:
            from ipaddress import IPv4Address, IPv6Address
            for key, value in converted_data.items():
                if isinstance(value, (IPv4Address, IPv6Address)):
                    converted_data[key] = str(value)
        except ImportError:
            # ipaddress module not available, skip conversion
            pass
        
        # Get JSON fields dynamically from schemas
        json_fields = self._get_json_fields_from_schemas()
        
        for field_name, field_value in converted_data.items():
            if field_name in json_fields and isinstance(field_value, str):
                try:
                    # Try to parse as JSON
                    parsed_value = json.loads(field_value)
                    converted_data[field_name] = parsed_value
                except (json.JSONDecodeError, TypeError):
                    # If parsing fails, keep the original value
                    pass
        
        return converted_data

    def _convert_model_to_dict(self, data: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
        """
        Convert Pydantic model or dict to dictionary for database operations.
        
        Args:
            data: Pydantic model instance or dictionary
            
        Returns:
            Dictionary representation of the data
        """
        if isinstance(data, dict):
            return data
        elif hasattr(data, 'model_dump'):
            # Pydantic v2 model
            return data.model_dump(exclude_unset=False)
        elif hasattr(data, 'dict'):
            # Pydantic v1 model
            return data.dict(exclude_unset=True)
        else:
            # Try to convert to dict
            try:
                return dict(data)
            except (TypeError, ValueError):
                self.logger.warning(f"Could not convert {type(data)} to dict, returning as-is")
                return data

    def _convert_lists_to_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert JSON fields to JSON strings while keeping array fields as Python lists.
        
        Args:
            data: Dictionary containing the record data
            
        Returns:
            Dictionary with JSON fields converted to JSON strings, array fields kept as lists
        """
        converted_data = data.copy()
        
        # Get JSON fields (for JSON columns) and array fields (for array columns) from schemas
        json_fields = self._get_json_fields_from_schemas()
        array_fields = self._get_array_fields_from_schemas()
        
        for field_name, field_value in converted_data.items():
            if field_name in array_fields:
                # Keep array fields as Python lists (PostgreSQL array columns expect lists)
                # No conversion needed - they should already be Python lists
                pass
            elif field_name in json_fields:
                # Convert JSON fields to JSON strings
                if isinstance(field_value, list):
                    converted_data[field_name] = json.dumps(field_value)
                elif isinstance(field_value, dict):
                    # Also handle nested dictionaries that might contain lists
                    converted_data[field_name] = json.dumps(field_value)
                elif field_value is not None:
                    # Handle any other non-None values that should be JSON
                    converted_data[field_name] = json.dumps(field_value)
        
        return converted_data
    
    # ============================================================================
    # Core CRUD Operations
    # ============================================================================
    
    async def _get_table_columns(self, table_name: str) -> set:
        """
        Get the set of column names for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Set of column names
        """
        try:
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = $1
            """
            result = await self.connection_manager.execute_query(query, [table_name])
            return {row['column_name'] for row in result.get('rows', [])}
        except Exception as e:
            self.logger.warning(f"Could not get columns for table {table_name}: {e}")
            return set()
    
    async def _get_date_columns(self, table_name: str) -> set:
        """
        Get the set of DATE column names for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Set of DATE column names
        """
        try:
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = $1
                AND data_type = 'date'
            """
            result = await self.connection_manager.execute_query(query, [table_name])
            return {row['column_name'] for row in result.get('rows', [])}
        except Exception as e:
            self.logger.warning(f"Could not get date columns for table {table_name}: {e}")
            return set()
    
    def _convert_date_strings(self, data: Dict[str, Any], date_columns: set) -> Dict[str, Any]:
        """
        Convert date strings to date objects for DATE columns.
        
        Args:
            data: Dictionary containing record data
            date_columns: Set of DATE column names
            
        Returns:
            Dictionary with date strings converted to date objects
        """
        converted_data = data.copy()
        for col_name in date_columns:
            if col_name in converted_data:
                value = converted_data[col_name]
                if isinstance(value, str):
                    try:
                        # Try parsing ISO format date string (YYYY-MM-DD)
                        converted_data[col_name] = date.fromisoformat(value)
                    except (ValueError, AttributeError):
                        # If parsing fails, try other common formats
                        try:
                            from datetime import datetime as dt
                            # Try parsing with datetime and extract date
                            parsed = dt.strptime(value, '%Y-%m-%d')
                            converted_data[col_name] = parsed.date()
                        except (ValueError, AttributeError):
                            # If all parsing fails, leave as-is (will error later)
                            self.logger.warning(f"Could not parse date string '{value}' for column '{col_name}'")
        return converted_data
    
    async def create(self, data: Union[Dict[str, Any], Any], table_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new record.
        
        Args:
            data: Dictionary or Pydantic model containing the record data
            table_name: Optional table name override (defaults to self.table_name)
            
        Returns:
            Created record with generated ID, or None if creation failed
        """
        try:
            # Use provided table name or default to instance table name
            target_table = table_name or self.table_name
            
            # Convert model to dict if needed
            data = self._convert_model_to_dict(data)
            
            # Get table columns to check which columns exist
            table_columns = await self._get_table_columns(target_table)
            
            # Get primary key column name
            primary_key = self.get_primary_key()
            
            # Filter out fields that don't exist in the table
            # This prevents errors when trying to insert into non-existent columns
            data = {k: v for k, v in data.items() if k in table_columns}
            
            # Remove primary key if it's None (let database use DEFAULT)
            # This prevents "null value violates not-null constraint" errors
            # BUT: If validator already generated a UUID, we should keep it
            if primary_key in data and data[primary_key] is None:
                # Only remove if it's actually None - validators should have generated a UUID by now
                # Log a warning if we're removing a primary key that should have been generated
                self.logger.debug(f"Removing {primary_key} from data (value is None) - database will generate it")
                data.pop(primary_key, None)
            
            # Add timestamps only if columns exist
            now = datetime.utcnow()
            if 'created_at' in table_columns:
                data['created_at'] = now
            if 'updated_at' in table_columns:
                data['updated_at'] = now
            
            # Get DATE columns and convert date strings to date objects
            date_columns = await self._get_date_columns(target_table)
            data = self._convert_date_strings(data, date_columns)
            
            # Convert list fields to JSON strings for PostgreSQL compatibility
            data = self._convert_lists_to_json(data)
            
            # Build insert query
            query, params = (
                insert_into(target_table)
                .data(data)
                .build()
            )
            
            # Verify RETURNING * is in the query
            if "RETURNING" not in query.upper():
                self.logger.warning(f"INSERT query for {target_table} does not have RETURNING * clause. Generated ID will not be available.")
            
            # Execute query - use execute_update for INSERT (following existing pattern)
            # execute_update returns the row when RETURNING * is used
            result = await self.connection_manager.execute_update(query, params)
            
            # If result is a dict (from RETURNING *), use it; otherwise use input data
            if isinstance(result, dict) and result:
                # Check if this is the row data (has primary key) or just affected count
                # Check both exact match and case-insensitive match for primary key
                primary_key_lower = primary_key.lower()
                result_key_lower = {k.lower(): k for k in result.keys()}
                
                if primary_key in result:
                    # This is the row data from RETURNING * with the generated primary key
                    self.logger.debug(f"Using RETURNING * result with {primary_key}={result[primary_key]}")
                    # Convert JSON strings back to lists for Pydantic models
                    converted_data = self._convert_json_to_lists(result)
                    return converted_data
                elif primary_key_lower in result_key_lower:
                    # Primary key exists but with different case - normalize it
                    actual_key = result_key_lower[primary_key_lower]
                    result[primary_key] = result[actual_key]
                    self.logger.debug(f"Using RETURNING * result with {primary_key}={result[primary_key]} (normalized from {actual_key})")
                    # Convert JSON strings back to lists for Pydantic models
                    converted_data = self._convert_json_to_lists(result)
                    return converted_data
                elif 'affected' not in result:
                    # This is row data but doesn't have primary key (shouldn't happen)
                    # This can happen if the primary key column name doesn't match
                    # Try to find the primary key by checking all keys
                    # Check if result has any UUID field that might be the primary key
                    for key, value in result.items():
                        if 'id' in key.lower() and isinstance(value, UUID):
                            # Found a potential primary key - use it silently (this is expected for some tables)
                            result[primary_key] = value
                            converted_data = self._convert_json_to_lists(result)
                            return converted_data
                    # Still use it as it has other data from RETURNING *
                    converted_data = self._convert_json_to_lists(result)
                    return converted_data
                # Otherwise it's just {"affected": N}, fall through to use input data
            
            # If result is None or doesn't have the row data, log a warning
            # This shouldn't happen for successful INSERTs with RETURNING *
            if result is None:
                self.logger.warning(f"INSERT with RETURNING * returned None for table {target_table}. This may indicate the INSERT failed silently.")
                # Try to query the record back using a unique combination of fields
                # This is a fallback if RETURNING * doesn't work
                # For tables with unique constraints, we can query back the inserted record
                # But for now, we'll just return None and let the caller handle it
                # The INSERT might have succeeded but RETURNING * didn't return data
                # In this case, we can't get the generated primary key, so we'll return None
                # and let the caller handle the error
                return None
            elif isinstance(result, dict) and 'affected' in result:
                self.logger.warning(f"INSERT for {target_table} returned affected count instead of row data. Query may not have RETURNING *.")
            else:
                self.logger.warning(f"INSERT for {target_table} returned unexpected result type: {type(result)}, value: {result}")
            
            # Fallback: convert JSON strings back to lists for Pydantic models
            # This happens if RETURNING * didn't return data or if result is None
            # Note: This fallback won't have the generated primary key, which will cause validation errors
            self.logger.error(f"Falling back to input data for {target_table} (missing {primary_key}). This will cause validation errors. Result was: {result}")
            # Don't return data without primary key - this will cause validation errors
            # Instead, raise an error so the caller knows the INSERT failed
            raise ValueError(f"Failed to get generated primary key for {target_table}. INSERT may have failed or RETURNING * didn't return data.")
            
        except Exception as e:
            # Handle duplicate key errors gracefully (record already exists)
            # Check for asyncpg UniqueViolationError or generic duplicate key error
            error_str = str(e)
            is_duplicate = (
                "duplicate key" in error_str.lower() or 
                "unique constraint" in error_str.lower() or
                "uniqueviolationerror" in error_str.lower()
            )
            
            if is_duplicate:
                # Record already exists, treat as success
                self.logger.debug(f"Record already exists in {target_table}, skipping insert")
                # Convert JSON strings back to lists for Pydantic models
                converted_data = self._convert_json_to_lists(data)
                return converted_data
            
            self.logger.error(f"Error creating record in {target_table}: {str(e)}")
            raise
    
    async def get_by_id(self, record_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Get a record by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            Record data or None if not found
        """
        try:
            query, params = (
                select_all()
                .table(self.table_name)
                .where_equals(self.get_primary_key(), record_id)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            if result and 'rows' in result and result['rows']:
                row = result['rows'][0]
                # Convert JSON strings back to lists
                return self._convert_json_to_lists(row)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting record by ID from {self.table_name}: {str(e)}")
            raise
    
    async def get_all(self, 
                     filters: Optional[Dict[str, Any]] = None,
                     order_by: Optional[str] = None,
                     order_direction: OrderDirection = OrderDirection.ASC,
                     pagination: Optional[PaginationParams] = None) -> List[Dict[str, Any]]:
        """
        Get all records with optional filtering, ordering, and pagination.
        
        Args:
            filters: Dictionary of field-value pairs to filter by
            order_by: Field name to order by
            order_direction: Order direction (ASC or DESC)
            pagination: Pagination parameters
            
        Returns:
            List of record data
        """
        try:
            query_builder = select_all().table(self.table_name)
            
            # Apply filters
            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        query_builder = query_builder.where_in(field, value)
                    else:
                        query_builder = query_builder.where_equals(field, value)
            
            # Apply ordering
            if order_by:
                query_builder = query_builder.order_by(order_by, order_direction)
            
            # Apply pagination
            if pagination:
                # PaginationParams has limit and offset
                query_builder = query_builder.limit(pagination.limit).offset(pagination.offset)
            
            query, params = query_builder.build()
            result = await self.connection_manager.execute_query(query, params)
            
            rows = result.get('rows', []) if result else []
            # Convert JSON strings back to lists for all rows
            return [self._convert_json_to_lists(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"Error getting all records from {self.table_name}: {str(e)}")
            raise
    
    async def update(self, record_id: Union[str, int], data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Update a record by its primary key.
        
        Args:
            record_id: Primary key value
            data: Dictionary containing the updated data
            
        Returns:
            Updated record data or None if not found
        """
        try:
            # Add update timestamp
            data['updated_at'] = datetime.utcnow()
            
            # Convert list fields to JSON strings for PostgreSQL compatibility
            data = self._convert_lists_to_json(data)
            
            # Build update query
            query, params = (
                update_table(self.table_name)
                .set(data)
                .where_equals(self.get_primary_key(), record_id)
                .build()
            )
            
            # Execute query
            result = await self.connection_manager.execute_query(query, params)
            
            if result and result.get('rowcount', 0) > 0:
                # Get the updated record
                return await self.get_by_id(record_id)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error updating record in {self.table_name}: {str(e)}")
            raise
    
    async def delete(self, record_id: Union[str, int]) -> bool:
        """
        Delete a record by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            True if record was deleted, False otherwise
        """
        try:
            query, params = (
                delete_from(self.table_name)
                .where_equals(self.get_primary_key(), record_id)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            return result.get('rowcount', 0) > 0 if result else False
            
        except Exception as e:
            self.logger.error(f"Error deleting record from {self.table_name}: {str(e)}")
            raise
    
    # ============================================================================
    # Advanced Query Operations
    # ============================================================================
    
    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records with optional filtering.
        
        Args:
            filters: Dictionary of field-value pairs to filter by
            
        Returns:
            Number of matching records
        """
        try:
            query_builder = count().table(self.table_name)
            
            # Apply filters
            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        query_builder = query_builder.where_in(field, value)
                    else:
                        query_builder = query_builder.where_equals(field, value)
            
            query, params = query_builder.build()
            result = await self.connection_manager.execute_query(query, params)
            
            if result and 'rows' in result and result['rows']:
                return result['rows'][0]['count']
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error counting records in {self.table_name}: {str(e)}")
            raise
    
    async def exists(self, record_id: Union[str, int]) -> bool:
        """
        Check if a record exists by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            True if record exists, False otherwise
        """
        try:
            query, params = (
                count()
                .table(self.table_name)
                .where_equals(self.get_primary_key(), record_id)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            if result and 'rows' in result and result['rows']:
                return result['rows'][0]['count'] > 0
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking if record exists in {self.table_name}: {str(e)}")
            raise
    
    async def find_by_field(self, field: str, value: Any) -> Optional[Dict[str, Any]]:
        """
        Find a record by a specific field value.
        
        Args:
            field: Field name
            value: Field value
            
        Returns:
            Record data or None if not found
        """
        try:
            query, params = (
                select_all()
                .table(self.table_name)
                .where_equals(field, value)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            if result and 'rows' in result and result['rows']:
                row = result['rows'][0]
                # Convert JSON strings back to lists
                return self._convert_json_to_lists(row)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding record by field in {self.table_name}: {str(e)}")
            raise
    
    async def find_all_by_field(self, field: str, value: Any) -> List[Dict[str, Any]]:
        """
        Find all records by a specific field value.
        
        Args:
            field: Field name
            value: Field value
            
        Returns:
            List of record data
        """
        try:
            query, params = (
                select_all()
                .table(self.table_name)
                .where_equals(field, value)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            rows = result.get('rows', []) if result else []
            # Convert JSON strings back to lists for all rows
            return [self._convert_json_to_lists(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"Error finding all records by field in {self.table_name}: {str(e)}")
            raise
    
    # ============================================================================
    # Batch Operations
    # ============================================================================
    
    async def create_many(self, data_list: List[Union[Dict[str, Any], Any]]) -> List[Dict[str, Any]]:
        """
        Create multiple records in a single transaction.
        
        Args:
            data_list: List of dictionaries or Pydantic models containing record data
            
        Returns:
            List of created records
        """
        try:
            if not data_list:
                return []
            
            # Convert models to dicts and add timestamps
            now = datetime.utcnow()
            converted_data_list = []
            for data in data_list:
                data = self._convert_model_to_dict(data)
                data['created_at'] = now
                data['updated_at'] = now
                converted_data_list.append(data)
            
            # Get field names from first record
            fields = list(converted_data_list[0].keys())
            
            # Build batch insert query
            query, params = (
                insert_into(self.table_name)
                .columns(fields)
                .values_many([list(record.values()) for record in converted_data_list])
                .build()
            )
            
            # Execute query
            result = await self.connection_manager.execute_update(query, params)
            
            if result:
                # For bulk inserts, we can't easily return individual records without knowing their IDs
                # Return a simple success indicator - the records were created
                return [{"status": "created", "count": len(converted_data_list)}]
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error creating multiple records in {self.table_name}: {str(e)}")
            raise
    
    async def update_many(self, updates: List[Tuple[Union[str, int], Dict[str, Any]]]) -> int:
        """
        Update multiple records in a single transaction.
        
        Args:
            updates: List of tuples (record_id, update_data)
            
        Returns:
            Number of records updated
        """
        try:
            if not updates:
                return 0
            
            total_updated = 0
            
            for record_id, data in updates:
                # Add update timestamp
                data['updated_at'] = datetime.utcnow()
                
                # Build update query
                query, params = (
                    update_table(self.table_name)
                    .set(data)
                    .where_equals(self.get_primary_key(), record_id)
                    .build()
                )
                
                # Execute query
                result = await self.connection_manager.execute_query(query, params)
                
                if result:
                    total_updated += result.get('rowcount', 0)
            
            return total_updated
            
        except Exception as e:
            self.logger.error(f"Error updating multiple records in {self.table_name}: {str(e)}")
            raise
    
    async def delete_many(self, record_ids: List[Union[str, int]]) -> int:
        """
        Delete multiple records in a single transaction.
        
        Args:
            record_ids: List of primary key values
            
        Returns:
            Number of records deleted
        """
        try:
            if not record_ids:
                return 0
            
            query, params = (
                delete_from(self.table_name)
                .where_in(self.get_primary_key(), record_ids)
                .build()
            )
            
            result = await self.connection_manager.execute_query(query, params)
            
            return result.get('rowcount', 0) if result else 0
            
        except Exception as e:
            self.logger.error(f"Error deleting multiple records from {self.table_name}: {str(e)}")
            raise
    
    # ============================================================================
    # Utility Methods
    # ============================================================================
    
    async def initialize(self) -> bool:
        """
        Initialize the repository.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            if self._initialized:
                return True
            
            # Verify table exists
            table_exists = await self._verify_table_exists()
            if not table_exists:
                self.logger.error(f"Table {self.table_name} does not exist")
                return False
            
            self._initialized = True
            self.logger.info(f"Repository {self.__class__.__name__} initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing repository {self.__class__.__name__}: {str(e)}")
            return False
    
    async def _verify_table_exists(self) -> bool:
        """
        Verify that the table exists in the database.
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            # PostgreSQL-specific table existence check
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )
            """
            result = await self.connection_manager.execute_query(query, [self.table_name])
            
            if result and 'rows' in result and result['rows']:
                return result['rows'][0].get('exists', False)
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error verifying table existence: {str(e)}")
            return False
    
    def get_logger(self) -> logging.Logger:
        """Get the repository logger."""
        return self.logger
    
    def get_table_name(self) -> str:
        """Get the table name."""
        return self.table_name
    
    def is_initialized(self) -> bool:
        """Check if repository is initialized."""
        return self._initialized

