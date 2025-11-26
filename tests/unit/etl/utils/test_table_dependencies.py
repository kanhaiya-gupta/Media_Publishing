"""
Unit Tests for Table Dependencies
==================================
"""

import pytest

from src.ownlens.processing.etl.utils.table_dependencies import (
    TABLE_DEPENDENCY_ORDER,
    TABLE_DEPENDENCIES,
    resolve_dependencies,
    sort_tables_by_dependency,
)


class TestTableDependencies:
    """Test table dependency resolution."""

    def test_resolve_dependencies_single_table(self):
        """Test resolving dependencies for a single table."""
        result = resolve_dependencies(["customer_events"])
        
        # Should include all dependencies
        assert "countries" in result
        assert "cities" in result
        assert "device_types" in result
        assert "operating_systems" in result
        assert "browsers" in result
        assert "companies" in result
        assert "brands" in result
        assert "categories" in result
        assert "users" in result
        assert "customer_sessions" in result
        assert "customer_events" in result

    def test_resolve_dependencies_multiple_tables(self):
        """Test resolving dependencies for multiple tables."""
        result = resolve_dependencies(["customer_events", "editorial_articles"])
        
        # Should include dependencies for both tables
        assert "customer_events" in result
        assert "editorial_articles" in result
        assert "countries" in result
        assert "brands" in result
        assert "users" in result

    def test_resolve_dependencies_base_table(self):
        """Test resolving dependencies for a base table (no dependencies)."""
        result = resolve_dependencies(["countries"])
        
        assert result == ["countries"]

    def test_resolve_dependencies_empty_list(self):
        """Test resolving dependencies for empty list."""
        result = resolve_dependencies([])
        assert result == []

    def test_resolve_dependencies_unknown_table(self):
        """Test resolving dependencies for unknown table."""
        result = resolve_dependencies(["unknown_table"])
        assert result == []

    def test_sort_tables_by_dependency(self):
        """Test sorting tables by dependency order."""
        tables = ["customer_events", "countries", "users", "brands"]
        result = sort_tables_by_dependency(tables)
        
        # Should be sorted in dependency order
        assert result.index("countries") < result.index("brands")
        assert result.index("brands") < result.index("users")
        assert result.index("users") < result.index("customer_events")

    def test_sort_tables_by_dependency_empty(self):
        """Test sorting empty list."""
        result = sort_tables_by_dependency([])
        assert result == []

    def test_sort_tables_by_dependency_unknown_tables(self):
        """Test sorting with unknown tables."""
        tables = ["unknown_table1", "countries", "unknown_table2"]
        result = sort_tables_by_dependency(tables)
        
        # Known tables should be sorted, unknown tables at the end
        assert "countries" in result
        assert result.index("countries") < result.index("unknown_table1")
        assert result.index("countries") < result.index("unknown_table2")

    def test_table_dependency_order_completeness(self):
        """Test that all tables in TABLE_DEPENDENCIES are in TABLE_DEPENDENCY_ORDER."""
        for table in TABLE_DEPENDENCIES.keys():
            assert table in TABLE_DEPENDENCY_ORDER, f"Table {table} not in dependency order"

    def test_table_dependency_order_validity(self):
        """Test that dependency order respects dependencies."""
        for table, deps in TABLE_DEPENDENCIES.items():
            if table in TABLE_DEPENDENCY_ORDER:
                table_index = TABLE_DEPENDENCY_ORDER.index(table)
                for dep in deps:
                    if dep in TABLE_DEPENDENCY_ORDER:
                        dep_index = TABLE_DEPENDENCY_ORDER.index(dep)
                        assert dep_index < table_index, (
                            f"Table {table} appears before dependency {dep}"
                        )

