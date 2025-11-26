"""
Query Builder
=============

Database-agnostic query builder with fluent interface.
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import os


class QueryType(Enum):
    """Query types."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    COUNT = "COUNT"


class OrderDirection(Enum):
    """Order directions."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class PaginationParams:
    """Pagination parameters."""
    limit: int
    offset: int = 0
    
    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("Limit must be positive")
        if self.offset < 0:
            raise ValueError("Offset must be non-negative")


class QueryBuilder:
    """
    Database-agnostic query builder with fluent interface.
    
    Provides a type-safe, database-agnostic way to build SQL queries.
    """
    
    def __init__(self, query_type: QueryType, database_type: str = None):
        """Initialize the query builder."""
        self.query_type = query_type
        self.database_type = database_type or os.getenv('DATABASE_TYPE', 'postgresql').lower()
        self.table_name = ""
        self.selected_columns = []
        self.values_list = []
        self.insert_data = {}
        self.where_conditions = []
        self.where_params = []
        self.order_by_fields = []
        self.order_directions = []
        self.limit_value = None
        self.offset_value = None
        self.joins = []
        self.group_by_fields = []
        self.having_conditions = []
        self.having_params = []
    
    def _get_placeholder(self, param_index: int) -> str:
        """Get the correct placeholder for the database type."""
        if self.database_type == 'postgresql':
            return f"${param_index}"
        else:
            return "?"
    
    def table(self, table_name: str) -> 'QueryBuilder':
        """Set the table name."""
        self.table_name = table_name
        return self
    
    def columns(self, columns: List[str]) -> 'QueryBuilder':
        """Set the columns for SELECT queries."""
        if self.query_type not in [QueryType.SELECT, QueryType.COUNT]:
            raise ValueError(f"Cannot set columns for {self.query_type.value} query")
        self.selected_columns = columns
        return self
    
    def values(self, values: List[Any]) -> 'QueryBuilder':
        """Set the values for INSERT queries."""
        if self.query_type != QueryType.INSERT:
            raise ValueError("Can only set values for INSERT queries")
        self.values_list = values
        return self
    
    def values_many(self, values_list: List[List[Any]]) -> 'QueryBuilder':
        """Set multiple value sets for batch INSERT queries."""
        if self.query_type != QueryType.INSERT:
            raise ValueError("Can only set values for INSERT queries")
        self.values_list = values_list
        return self
    
    def data(self, data: Dict[str, Any]) -> 'QueryBuilder':
        """Set the data for INSERT queries from a dictionary."""
        if self.query_type != QueryType.INSERT:
            raise ValueError("Can only set data for INSERT queries")
        self.insert_data = data
        return self
    
    def set(self, data: Dict[str, Any]) -> 'QueryBuilder':
        """Set the data for UPDATE queries."""
        if self.query_type != QueryType.UPDATE:
            raise ValueError("Can only set data for UPDATE queries")
        self.values_list = data
        return self
    
    def where_equals(self, field: str, value: Any) -> 'QueryBuilder':
        """Add an equality condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} = {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_not_equals(self, field: str, value: Any) -> 'QueryBuilder':
        """Add a not-equals condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} != {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_in(self, field: str, values: List[Any]) -> 'QueryBuilder':
        """Add an IN condition to the WHERE clause."""
        if not values:
            self.where_conditions.append("1 = 0")  # Always false
            return self
        
        placeholders = []
        for value in values:
            param_index = len(self.where_params) + 1
            placeholder = self._get_placeholder(param_index)
            placeholders.append(placeholder)
            self.where_params.append(value)
        
        self.where_conditions.append(f"{field} IN ({', '.join(placeholders)})")
        return self
    
    def where_not_in(self, field: str, values: List[Any]) -> 'QueryBuilder':
        """Add a NOT IN condition to the WHERE clause."""
        if not values:
            return self  # No condition needed
        
        placeholders = []
        for value in values:
            param_index = len(self.where_params) + 1
            placeholder = self._get_placeholder(param_index)
            placeholders.append(placeholder)
            self.where_params.append(value)
        
        self.where_conditions.append(f"{field} NOT IN ({', '.join(placeholders)})")
        return self
    
    def where_like(self, field: str, pattern: str) -> 'QueryBuilder':
        """Add a LIKE condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} LIKE {placeholder}")
        self.where_params.append(pattern)
        return self
    
    def where_not_like(self, field: str, pattern: str) -> 'QueryBuilder':
        """Add a NOT LIKE condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} NOT LIKE {placeholder}")
        self.where_params.append(pattern)
        return self
    
    def where_is_null(self, field: str) -> 'QueryBuilder':
        """Add an IS NULL condition to the WHERE clause."""
        self.where_conditions.append(f"{field} IS NULL")
        return self
    
    def where_is_not_null(self, field: str) -> 'QueryBuilder':
        """Add an IS NOT NULL condition to the WHERE clause."""
        self.where_conditions.append(f"{field} IS NOT NULL")
        return self
    
    def where_greater_than(self, field: str, value: Any) -> 'QueryBuilder':
        """Add a greater than condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} > {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_greater_than_or_equal(self, field: str, value: Any) -> 'QueryBuilder':
        """Add a greater than or equal condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} >= {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_less_than(self, field: str, value: Any) -> 'QueryBuilder':
        """Add a less than condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} < {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_less_than_or_equal(self, field: str, value: Any) -> 'QueryBuilder':
        """Add a less than or equal condition to the WHERE clause."""
        param_index = len(self.where_params) + 1
        placeholder = self._get_placeholder(param_index)
        self.where_conditions.append(f"{field} <= {placeholder}")
        self.where_params.append(value)
        return self
    
    def where_between(self, field: str, start_value: Any, end_value: Any) -> 'QueryBuilder':
        """Add a BETWEEN condition to the WHERE clause."""
        param_index1 = len(self.where_params) + 1
        param_index2 = len(self.where_params) + 2
        placeholder1 = self._get_placeholder(param_index1)
        placeholder2 = self._get_placeholder(param_index2)
        self.where_conditions.append(f"{field} BETWEEN {placeholder1} AND {placeholder2}")
        self.where_params.extend([start_value, end_value])
        return self
    
    def where_not_between(self, field: str, start_value: Any, end_value: Any) -> 'QueryBuilder':
        """Add a NOT BETWEEN condition to the WHERE clause."""
        param_index1 = len(self.where_params) + 1
        param_index2 = len(self.where_params) + 2
        placeholder1 = self._get_placeholder(param_index1)
        placeholder2 = self._get_placeholder(param_index2)
        self.where_conditions.append(f"{field} NOT BETWEEN {placeholder1} AND {placeholder2}")
        self.where_params.extend([start_value, end_value])
        return self
    
    def order_by(self, field: str, direction: OrderDirection = OrderDirection.ASC) -> 'QueryBuilder':
        """Add an ORDER BY clause."""
        self.order_by_fields.append(field)
        self.order_directions.append(direction)
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Add a LIMIT clause."""
        if limit <= 0:
            raise ValueError("Limit must be positive")
        self.limit_value = limit
        return self
    
    def offset(self, offset: int) -> 'QueryBuilder':
        """Add an OFFSET clause."""
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        self.offset_value = offset
        return self
    
    def join(self, table: str, condition: str) -> 'QueryBuilder':
        """Add a JOIN clause."""
        self.joins.append(f"JOIN {table} ON {condition}")
        return self
    
    def left_join(self, table: str, condition: str) -> 'QueryBuilder':
        """Add a LEFT JOIN clause."""
        self.joins.append(f"LEFT JOIN {table} ON {condition}")
        return self
    
    def right_join(self, table: str, condition: str) -> 'QueryBuilder':
        """Add a RIGHT JOIN clause."""
        self.joins.append(f"RIGHT JOIN {table} ON {condition}")
        return self
    
    def inner_join(self, table: str, condition: str) -> 'QueryBuilder':
        """Add an INNER JOIN clause."""
        self.joins.append(f"INNER JOIN {table} ON {condition}")
        return self
    
    def group_by(self, fields: List[str]) -> 'QueryBuilder':
        """Add a GROUP BY clause."""
        self.group_by_fields = fields
        return self
    
    def having(self, condition: str, *params: Any) -> 'QueryBuilder':
        """Add a HAVING clause."""
        self.having_conditions.append(condition)
        self.having_params.extend(params)
        return self
    
    def build(self) -> Tuple[str, List[Any]]:
        """Build the final query and parameters."""
        if not self.table_name:
            raise ValueError("Table name is required")
        
        query = ""
        params = []
        
        if self.query_type == QueryType.SELECT:
            query, params = self._build_select()
        elif self.query_type == QueryType.INSERT:
            query, params = self._build_insert()
        elif self.query_type == QueryType.UPDATE:
            query, params = self._build_update()
        elif self.query_type == QueryType.DELETE:
            query, params = self._build_delete()
        elif self.query_type == QueryType.COUNT:
            query, params = self._build_count()
        else:
            raise ValueError(f"Unsupported query type: {self.query_type}")
        
        return query, params
    
    def _build_select(self) -> Tuple[str, List[Any]]:
        """Build a SELECT query."""
        # SELECT clause
        if self.selected_columns:
            columns_str = ", ".join(self.selected_columns)
        else:
            columns_str = "*"
        
        query = f"SELECT {columns_str} FROM {self.table_name}"
        params = []
        
        # JOIN clauses
        if self.joins:
            query += " " + " ".join(self.joins)
        
        # WHERE clause
        if self.where_conditions:
            query += " WHERE " + " AND ".join(self.where_conditions)
            params.extend(self.where_params)
        
        # GROUP BY clause
        if self.group_by_fields:
            query += " GROUP BY " + ", ".join(self.group_by_fields)
        
        # HAVING clause
        if self.having_conditions:
            query += " HAVING " + " AND ".join(self.having_conditions)
            params.extend(self.having_params)
        
        # ORDER BY clause
        if self.order_by_fields:
            order_parts = []
            for field, direction in zip(self.order_by_fields, self.order_directions):
                order_parts.append(f"{field} {direction.value}")
            query += " ORDER BY " + ", ".join(order_parts)
        
        # LIMIT clause
        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"
        
        # OFFSET clause
        if self.offset_value is not None:
            query += f" OFFSET {self.offset_value}"
        
        return query, params
    
    def _build_insert(self) -> Tuple[str, List[Any]]:
        """Build an INSERT query."""
        if self.insert_data:
            # Insert from dictionary using insert_data
            columns = list(self.insert_data.keys())
            values = list(self.insert_data.values())
            columns_str = ", ".join(columns)
            placeholders = ", ".join([self._get_placeholder(i+1) for i in range(len(columns))])
            query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders}) RETURNING *"
            params = values
        elif self.values_list:
            if isinstance(self.values_list, dict):
                # Dictionary values
                columns = list(self.values_list.keys())
                values = list(self.values_list.values())
                columns_str = ", ".join(columns)
                placeholders = ", ".join([self._get_placeholder(i+1) for i in range(len(values))])
                query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"
                params = values
            elif isinstance(self.values_list[0], list):
                # Batch insert
                if not self.selected_columns:
                    raise ValueError("Columns are required for batch INSERT")
                
                columns_str = ", ".join(self.selected_columns)
                placeholders = ", ".join([self._get_placeholder(i+1) for i in range(len(self.selected_columns))])
                query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"
                
                # For batch insert, we need to handle multiple value sets
                # This is a simplified version - in practice, you might want to use executemany
                params = self.values_list[0] if self.values_list else []
            else:
                # List values
                if not self.selected_columns:
                    raise ValueError("Columns are required for INSERT with list values")
                
                columns_str = ", ".join(self.selected_columns)
                placeholders = ", ".join([self._get_placeholder(i+1) for i in range(len(self.values_list))])
                query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"
                params = self.values_list
        else:
            raise ValueError("Values or data are required for INSERT query")
        
        return query, params
    
    def _build_update(self) -> Tuple[str, List[Any]]:
        """Build an UPDATE query."""
        if not self.values_list:
            raise ValueError("Values are required for UPDATE query")
        
        if not isinstance(self.values_list, dict):
            raise ValueError("UPDATE values must be a dictionary")
        
        # SET clause
        set_parts = []
        params = []
        for i, (field, value) in enumerate(self.values_list.items()):
            placeholder = self._get_placeholder(i + 1)
            set_parts.append(f"{field} = {placeholder}")
            params.append(value)
        
        query = f"UPDATE {self.table_name} SET {', '.join(set_parts)}"
        
        # WHERE clause - need to renumber placeholders to continue from SET clause
        if self.where_conditions:
            # Renumber WHERE placeholders to continue from SET clause
            where_conditions_renumbered = []
            for condition in self.where_conditions:
                # Replace $N placeholders with correct numbers
                import re
                def replace_placeholder(match):
                    old_num = int(match.group(1))
                    new_num = old_num + len(params)
                    return f"${new_num}"
                
                condition_renumbered = re.sub(r'\$(\d+)', replace_placeholder, condition)
                where_conditions_renumbered.append(condition_renumbered)
            
            query += " WHERE " + " AND ".join(where_conditions_renumbered)
            params.extend(self.where_params)
        else:
            # Prevent accidental updates of all records
            raise ValueError("WHERE clause is required for UPDATE queries")
        
        return query, params
    
    def _build_delete(self) -> Tuple[str, List[Any]]:
        """Build a DELETE query."""
        query = f"DELETE FROM {self.table_name}"
        params = []
        
        # WHERE clause
        if self.where_conditions:
            query += " WHERE " + " AND ".join(self.where_conditions)
            params.extend(self.where_params)
        else:
            # Prevent accidental deletion of all records
            raise ValueError("WHERE clause is required for DELETE queries")
        
        return query, params
    
    def _build_count(self) -> Tuple[str, List[Any]]:
        """Build a COUNT query."""
        query = f"SELECT COUNT(*) as count FROM {self.table_name}"
        params = []
        
        # JOIN clauses
        if self.joins:
            query += " " + " ".join(self.joins)
        
        # WHERE clause
        if self.where_conditions:
            query += " WHERE " + " AND ".join(self.where_conditions)
            params.extend(self.where_params)
        
        # GROUP BY clause
        if self.group_by_fields:
            query += " GROUP BY " + ", ".join(self.group_by_fields)
        
        # HAVING clause
        if self.having_conditions:
            query += " HAVING " + " AND ".join(self.having_conditions)
            params.extend(self.having_params)
        
        return query, params


# ============================================================================
# Convenience Functions
# ============================================================================

def select(columns: Optional[List[str]] = None) -> QueryBuilder:
    """Create a SELECT query builder."""
    builder = QueryBuilder(QueryType.SELECT)
    if columns:
        builder.columns(columns)
    return builder


def select_all() -> QueryBuilder:
    """Create a SELECT * query builder."""
    return QueryBuilder(QueryType.SELECT)


def count() -> QueryBuilder:
    """Create a COUNT query builder."""
    return QueryBuilder(QueryType.COUNT)


def insert_into(table_name: str) -> QueryBuilder:
    """Create an INSERT query builder."""
    return QueryBuilder(QueryType.INSERT).table(table_name)


def update_table(table_name: str) -> QueryBuilder:
    """Create an UPDATE query builder."""
    return QueryBuilder(QueryType.UPDATE).table(table_name)


def delete_from(table_name: str) -> QueryBuilder:
    """Create a DELETE query builder."""
    return QueryBuilder(QueryType.DELETE).table(table_name)

