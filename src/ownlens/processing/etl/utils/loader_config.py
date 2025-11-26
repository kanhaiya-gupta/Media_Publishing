"""
Loader Configuration
====================

Table-specific load configurations for all 79 tables.
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum


class LoadMode(str, Enum):
    """Load mode enumeration."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    UPSERT = "upsert"
    IGNORE = "ignore"
    ERROR = "error"


class ConflictResolution(str, Enum):
    """Conflict resolution strategy."""
    UPDATE = "update"
    SKIP = "skip"
    ERROR = "error"


@dataclass
class TableLoadConfig:
    """
    Configuration for loading a specific table to a destination.
    
    Attributes:
        table_name: Name of the table
        destination: Destination type ("s3", "postgresql", "clickhouse")
        mode: Load mode ("append", "overwrite", "upsert", "ignore", "error")
        partition_by: Columns to partition by (for S3/ClickHouse)
        format: File format for S3 ("parquet", "delta", "json")
        conflict_resolution: Conflict resolution strategy for PostgreSQL upserts
        primary_key: Primary key columns for upsert operations
        clickhouse_table: ClickHouse table name (if different from PostgreSQL)
        clickhouse_partition: ClickHouse partition expression
        s3_path_template: S3 path template (e.g., "{domain}/{table}/{date}/")
        kwargs: Additional destination-specific options
    """
    table_name: str
    destination: str
    mode: str = "append"
    partition_by: Optional[List[str]] = None
    format: Optional[str] = None
    conflict_resolution: Optional[str] = None
    primary_key: Optional[List[str]] = None
    clickhouse_table: Optional[str] = None
    clickhouse_partition: Optional[str] = None
    s3_path_template: Optional[str] = None
    kwargs: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Initialize default values."""
        if self.partition_by is None:
            self.partition_by = []
        if self.kwargs is None:
            self.kwargs = {}


# Table-specific load configurations
# Format: {table_name: {destination: TableLoadConfig}}

TABLE_LOAD_CONFIGS: Dict[str, Dict[str, TableLoadConfig]] = {
    # Base Domain (11 tables)
    "companies": {
        "s3": TableLoadConfig(
            table_name="companies",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/companies/",
        ),
        "postgresql": TableLoadConfig(
            table_name="companies",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["company_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="companies",
            destination="clickhouse",
            mode="append",
            clickhouse_table="companies",  # Match PostgreSQL table name
        ),
    },
    "brands": {
        "s3": TableLoadConfig(
            table_name="brands",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/brands/",
        ),
        "postgresql": TableLoadConfig(
            table_name="brands",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["brand_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="brands",
            destination="clickhouse",
            mode="append",
            clickhouse_table="brands",  # Match PostgreSQL table name
        ),
    },
    "brand_countries": {
        "s3": TableLoadConfig(
            table_name="brand_countries",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/brand_countries/",
        ),
        "postgresql": TableLoadConfig(
            table_name="brand_countries",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["brand_id", "country_code"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="brand_countries",
            destination="clickhouse",
            mode="append",
            clickhouse_table="brand_countries",
        ),
    },
    "countries": {
        "s3": TableLoadConfig(
            table_name="countries",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/countries/",
        ),
        "postgresql": TableLoadConfig(
            table_name="countries",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["country_code"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="countries",
            destination="clickhouse",
            mode="append",
            clickhouse_table="countries",  # Match PostgreSQL table name
        ),
    },
    "cities": {
        "s3": TableLoadConfig(
            table_name="cities",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/cities/",
        ),
        "postgresql": TableLoadConfig(
            table_name="cities",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["city_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="cities",
            destination="clickhouse",
            mode="append",
            clickhouse_table="cities",  # Match PostgreSQL table name
        ),
    },
    "categories": {
        "s3": TableLoadConfig(
            table_name="categories",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/categories/",
        ),
        "postgresql": TableLoadConfig(
            table_name="categories",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["category_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="categories",
            destination="clickhouse",
            mode="append",
            clickhouse_table="categories",  # Match PostgreSQL table name
        ),
    },
    "users": {
        "s3": TableLoadConfig(
            table_name="users",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/users/",
        ),
        "postgresql": TableLoadConfig(
            table_name="users",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["user_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="users",
            destination="clickhouse",
            mode="append",
            clickhouse_table="users",
        ),
    },
    "user_accounts": {
        "s3": TableLoadConfig(
            table_name="user_accounts",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/user_accounts/",
        ),
        "postgresql": TableLoadConfig(
            table_name="user_accounts",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["account_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="user_accounts",
            destination="clickhouse",
            mode="append",
            clickhouse_table="user_accounts",
        ),
    },
    "device_types": {
        "s3": TableLoadConfig(
            table_name="device_types",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/device_types/",
        ),
        "postgresql": TableLoadConfig(
            table_name="device_types",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["device_type_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="device_types",
            destination="clickhouse",
            mode="append",
            clickhouse_table="device_types",  # Match PostgreSQL table name
        ),
    },
    "operating_systems": {
        "s3": TableLoadConfig(
            table_name="operating_systems",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/operating_systems/",
        ),
        "postgresql": TableLoadConfig(
            table_name="operating_systems",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["os_code"],  # Use business key for ON CONFLICT (os_code is UNIQUE)
        ),
        "clickhouse": TableLoadConfig(
            table_name="operating_systems",
            destination="clickhouse",
            mode="append",
            clickhouse_table="operating_systems",  # Match PostgreSQL table name
        ),
    },
    "browsers": {
        "s3": TableLoadConfig(
            table_name="browsers",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/base/browsers/",
        ),
        "postgresql": TableLoadConfig(
            table_name="browsers",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["browser_code"],  # Use business key for ON CONFLICT (browser_code is UNIQUE)
        ),
        "clickhouse": TableLoadConfig(
            table_name="browsers",
            destination="clickhouse",
            mode="append",
            clickhouse_table="browsers",  # Match PostgreSQL table name
        ),
    },
    
    # Customer Domain (8 tables)
    "customer_sessions": {
        "s3": TableLoadConfig(
            table_name="customer_sessions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["session_start"],
            s3_path_template="data-lake/customer/sessions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_sessions",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_sessions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_sessions",
            clickhouse_partition="toYYYYMM(session_start)",
        ),
    },
    "customer_events": {
        "s3": TableLoadConfig(
            table_name="customer_events",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_date"],
            s3_path_template="data-lake/customer/events/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_events",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_events",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_events",
            clickhouse_partition="toYYYYMM(event_date)",
        ),
    },
    "customer_user_features": {
        "s3": TableLoadConfig(
            table_name="customer_user_features",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/customer/features/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_user_features",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["user_id", "feature_date"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_user_features",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_user_features",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "customer_user_segments": {
        "s3": TableLoadConfig(
            table_name="customer_user_segments",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/customer/segments/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_user_segments",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["segment_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_user_segments",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_user_segments",
        ),
    },
    "customer_user_segment_assignments": {
        "s3": TableLoadConfig(
            table_name="customer_user_segment_assignments",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/customer/segment_assignments/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_user_segment_assignments",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["assignment_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_user_segment_assignments",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_user_segment_assignments",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "customer_churn_predictions": {
        "s3": TableLoadConfig(
            table_name="customer_churn_predictions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["prediction_date"],
            s3_path_template="data-lake/customer/churn/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_churn_predictions",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["prediction_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_churn_predictions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_churn_predictions",
            clickhouse_partition="toYYYYMM(prediction_date)",
        ),
    },
    "customer_recommendations": {
        "s3": TableLoadConfig(
            table_name="customer_recommendations",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/customer/recommendations/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_recommendations",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["recommendation_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_recommendations",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_recommendations",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "customer_conversion_predictions": {
        "s3": TableLoadConfig(
            table_name="customer_conversion_predictions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["prediction_date"],
            s3_path_template="data-lake/customer/conversions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="customer_conversion_predictions",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["prediction_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="customer_conversion_predictions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="customer_conversion_predictions",
            clickhouse_partition="toYYYYMM(prediction_date)",
        ),
    },
    
    # Editorial Domain (17 tables)
    "editorial_authors": {
        "s3": TableLoadConfig(
            table_name="editorial_authors",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/editorial/authors/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_authors",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["author_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_authors",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_authors",  # Match PostgreSQL table name
        ),
    },
    "editorial_articles": {
        "s3": TableLoadConfig(
            table_name="editorial_articles",
            destination="s3",
            mode="append",
            format="parquet",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/articles/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_articles",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["article_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_articles",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_articles",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_article_performance": {
        "s3": TableLoadConfig(
            table_name="editorial_article_performance",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["performance_date"],
            s3_path_template="data-lake/editorial/performance/articles/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_article_performance",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["performance_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_article_performance",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_article_performance",
            clickhouse_partition="toYYYYMM(performance_date)",
        ),
    },
    "editorial_author_performance": {
        "s3": TableLoadConfig(
            table_name="editorial_author_performance",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["performance_date"],
            s3_path_template="data-lake/editorial/performance/authors/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_author_performance",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["performance_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_author_performance",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_author_performance",
            clickhouse_partition="toYYYYMM(performance_date)",
        ),
    },
    "editorial_category_performance": {
        "s3": TableLoadConfig(
            table_name="editorial_category_performance",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["performance_date"],
            s3_path_template="data-lake/editorial/performance/categories/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_category_performance",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["performance_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_category_performance",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_category_performance",
            clickhouse_partition="toYYYYMM(performance_date)",
        ),
    },
    "editorial_content_events": {
        "s3": TableLoadConfig(
            table_name="editorial_content_events",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_date"],
            s3_path_template="data-lake/editorial/events/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_content_events",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_content_events",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_content_events",
            clickhouse_partition="toYYYYMM(event_date)",
        ),
    },
    "editorial_headline_tests": {
        "s3": TableLoadConfig(
            table_name="editorial_headline_tests",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/headline_tests/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_headline_tests",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["test_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_headline_tests",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_headline_tests",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_trending_topics": {
        "s3": TableLoadConfig(
            table_name="editorial_trending_topics",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["period_start"],
            s3_path_template="data-lake/editorial/trending/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_trending_topics",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["topic_id"],  # Fixed: was trending_id, but schema uses topic_id
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_trending_topics",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_trending_topics",
            clickhouse_partition="toYYYYMM(trending_date)",
        ),
    },
    "editorial_content_recommendations": {
        "s3": TableLoadConfig(
            table_name="editorial_content_recommendations",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/recommendations/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_content_recommendations",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["recommendation_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_content_recommendations",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_content_recommendations",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_article_content": {
        "s3": TableLoadConfig(
            table_name="editorial_article_content",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/content/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_article_content",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["content_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_article_content",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_article_content",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_content_versions": {
        "s3": TableLoadConfig(
            table_name="editorial_content_versions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/versions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_content_versions",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_content_versions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_content_versions",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_media_assets": {
        "s3": TableLoadConfig(
            table_name="editorial_media_assets",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/editorial/media/assets/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_media_assets",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["media_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_media_assets",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_media_assets",  # Match PostgreSQL table name
        ),
    },
    "editorial_media_variants": {
        "s3": TableLoadConfig(
            table_name="editorial_media_variants",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/editorial/media/variants/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_media_variants",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["variant_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_media_variants",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_media_variants",
        ),
    },
    "editorial_content_media": {
        "s3": TableLoadConfig(
            table_name="editorial_content_media",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/editorial/media/content/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_content_media",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["relationship_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_content_media",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_content_media",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "editorial_media_collections": {
        "s3": TableLoadConfig(
            table_name="editorial_media_collections",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/editorial/media/collections/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_media_collections",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["collection_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_media_collections",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_media_collections",
        ),
    },
    "editorial_media_collection_items": {
        "s3": TableLoadConfig(
            table_name="editorial_media_collection_items",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/editorial/media/collections/items/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_media_collection_items",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["item_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_media_collection_items",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_media_collection_items",
        ),
    },
    "editorial_media_usage": {
        "s3": TableLoadConfig(
            table_name="editorial_media_usage",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["usage_timestamp"],
            s3_path_template="data-lake/editorial/media/usage/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="editorial_media_usage",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="editorial_media_usage",
            destination="clickhouse",
            mode="append",
            clickhouse_table="editorial_media_usage",
            clickhouse_partition="toYYYYMM(usage_timestamp)",
        ),
    },
    
    # Company Domain (8 tables)
    "company_departments": {
        "s3": TableLoadConfig(
            table_name="company_departments",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/company/departments/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_departments",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["department_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_departments",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_departments",
        ),
    },
    "company_employees": {
        "s3": TableLoadConfig(
            table_name="company_employees",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/company/employees/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_employees",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["employee_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_employees",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_employees",
        ),
    },
    "company_internal_content": {
        "s3": TableLoadConfig(
            table_name="company_internal_content",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/company/content/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_internal_content",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["content_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_internal_content",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_internal_content",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "company_content_performance": {
        "s3": TableLoadConfig(
            table_name="company_content_performance",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["performance_date"],
            s3_path_template="data-lake/company/performance/content/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_content_performance",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["performance_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_content_performance",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_content_performance",
            clickhouse_partition="toYYYYMM(performance_date)",
        ),
    },
    "company_department_performance": {
        "s3": TableLoadConfig(
            table_name="company_department_performance",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["performance_date"],
            s3_path_template="data-lake/company/performance/departments/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_department_performance",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["performance_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_department_performance",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_department_performance",
            clickhouse_partition="toYYYYMM(performance_date)",
        ),
    },
    "company_content_events": {
        "s3": TableLoadConfig(
            table_name="company_content_events",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_date"],
            s3_path_template="data-lake/company/events/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_content_events",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_content_events",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_content_events",
            clickhouse_partition="toYYYYMM(event_date)",
        ),
    },
    "company_employee_engagement": {
        "s3": TableLoadConfig(
            table_name="company_employee_engagement",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["engagement_date"],
            s3_path_template="data-lake/company/engagement/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_employee_engagement",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["engagement_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_employee_engagement",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_employee_engagement",
            clickhouse_partition="toYYYYMM(engagement_date)",
        ),
    },
    "company_communications_analytics": {
        "s3": TableLoadConfig(
            table_name="company_communications_analytics",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["analytics_date"],
            s3_path_template="data-lake/company/analytics/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="company_communications_analytics",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["analytics_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="company_communications_analytics",
            destination="clickhouse",
            mode="append",
            clickhouse_table="company_communications_analytics",
            clickhouse_partition="toYYYYMM(analytics_date)",
        ),
    },
    
    # Security Domain (7 tables)
    "security_roles": {
        "s3": TableLoadConfig(
            table_name="security_roles",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/security/roles/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_roles",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["role_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_roles",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_roles",
        ),
    },
    "security_permissions": {
        "s3": TableLoadConfig(
            table_name="security_permissions",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/security/permissions/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_permissions",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["permission_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_permissions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_permissions",
        ),
    },
    "security_role_permissions": {
        "s3": TableLoadConfig(
            table_name="security_role_permissions",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/security/role_permissions/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_role_permissions",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["role_id", "permission_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_role_permissions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_role_permissions",
        ),
    },
    "security_user_roles": {
        "s3": TableLoadConfig(
            table_name="security_user_roles",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/security/user_roles/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_user_roles",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["user_role_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_user_roles",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_user_roles",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "security_api_keys": {
        "s3": TableLoadConfig(
            table_name="security_api_keys",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/security/api_keys/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_api_keys",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["api_key_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_api_keys",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_api_keys",
        ),
    },
    "security_api_key_usage": {
        "s3": TableLoadConfig(
            table_name="security_api_key_usage",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["usage_date"],
            s3_path_template="data-lake/security/api_key_usage/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_api_key_usage",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_api_key_usage",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_api_key_usage",
            clickhouse_partition="toYYYYMM(usage_date)",
        ),
    },
    "security_user_sessions": {
        "s3": TableLoadConfig(
            table_name="security_user_sessions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["session_start"],
            s3_path_template="data-lake/security/user_sessions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="security_user_sessions",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="security_user_sessions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="security_user_sessions",
            clickhouse_partition="toYYYYMM(session_start)",
        ),
    },
    
    # Audit Domain (6 tables)
    "audit_logs": {
        "s3": TableLoadConfig(
            table_name="audit_logs",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_timestamp"],
            s3_path_template="data-lake/audit/logs/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_logs",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_logs",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_logs",
            clickhouse_partition="toYYYYMM(event_timestamp)",
        ),
    },
    "audit_data_changes": {
        "s3": TableLoadConfig(
            table_name="audit_data_changes",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["change_timestamp"],
            s3_path_template="data-lake/audit/data_changes/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_data_changes",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_data_changes",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_data_changes",
            clickhouse_partition="toYYYYMM(change_timestamp)",
        ),
    },
    "audit_data_access": {
        "s3": TableLoadConfig(
            table_name="audit_data_access",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["access_timestamp"],
            s3_path_template="data-lake/audit/data_access/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_data_access",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_data_access",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_data_access",
            clickhouse_partition="toYYYYMM(access_timestamp)",
        ),
    },
    "audit_security_events": {
        "s3": TableLoadConfig(
            table_name="audit_security_events",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_timestamp"],
            s3_path_template="data-lake/audit/security_events/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_security_events",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_security_events",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_security_events",
            clickhouse_partition="toYYYYMM(event_timestamp)",
        ),
    },
    "audit_data_lineage": {
        "s3": TableLoadConfig(
            table_name="audit_data_lineage",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/audit/data_lineage/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_data_lineage",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_data_lineage",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_data_lineage",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "audit_compliance_events": {
        "s3": TableLoadConfig(
            table_name="audit_compliance_events",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["event_timestamp"],
            s3_path_template="data-lake/audit/compliance_events/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="audit_compliance_events",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="audit_compliance_events",
            destination="clickhouse",
            mode="append",
            clickhouse_table="audit_compliance_events",
            clickhouse_partition="toYYYYMM(event_timestamp)",
        ),
    },
    
    # Compliance Domain (7 tables)
    "compliance_user_consent": {
        "s3": TableLoadConfig(
            table_name="compliance_user_consent",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/compliance/consent/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_user_consent",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["consent_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_user_consent",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_user_consent",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "compliance_data_subject_requests": {
        "s3": TableLoadConfig(
            table_name="compliance_data_subject_requests",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/compliance/requests/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_data_subject_requests",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["request_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_data_subject_requests",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_data_subject_requests",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "compliance_retention_policies": {
        "s3": TableLoadConfig(
            table_name="compliance_retention_policies",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/compliance/policies/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_retention_policies",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["policy_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_retention_policies",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_retention_policies",
        ),
    },
    "compliance_retention_executions": {
        "s3": TableLoadConfig(
            table_name="compliance_retention_executions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["execution_started_at"],
            s3_path_template="data-lake/compliance/executions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_retention_executions",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_retention_executions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_retention_executions",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(execution_started_at)",
        ),
    },
    "compliance_anonymized_data": {
        "s3": TableLoadConfig(
            table_name="compliance_anonymized_data",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["anonymization_date"],
            s3_path_template="data-lake/compliance/anonymized/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_anonymized_data",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_anonymized_data",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_anonymized_data",
            clickhouse_partition="toYYYYMM(anonymization_date)",
        ),
    },
    "compliance_privacy_assessments": {
        "s3": TableLoadConfig(
            table_name="compliance_privacy_assessments",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/compliance/assessments/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_privacy_assessments",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["assessment_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_privacy_assessments",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_privacy_assessments",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    "compliance_breach_incidents": {
        "s3": TableLoadConfig(
            table_name="compliance_breach_incidents",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["discovered_at"],
            s3_path_template="data-lake/compliance/breaches/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="compliance_breach_incidents",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["incident_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="compliance_breach_incidents",
            destination="clickhouse",
            mode="append",
            clickhouse_table="compliance_breach_incidents",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(discovered_at)",
        ),
    },
    
    # ML Models Domain (6 tables)
    "ml_model_registry": {
        "s3": TableLoadConfig(
            table_name="ml_model_registry",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/ml_models/registry/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_registry",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["model_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_registry",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_registry",
        ),
    },
    "ml_model_features": {
        "s3": TableLoadConfig(
            table_name="ml_model_features",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/ml_models/features/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_features",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["feature_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_features",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_features",
        ),
    },
    "ml_model_training_runs": {
        "s3": TableLoadConfig(
            table_name="ml_model_training_runs",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["training_started_at"],
            s3_path_template="data-lake/ml_models/training/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_training_runs",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["run_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_training_runs",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_training_runs",
            clickhouse_partition="toYYYYMM(training_started_at)",
        ),
    },
    "ml_model_predictions": {
        "s3": TableLoadConfig(
            table_name="ml_model_predictions",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["prediction_timestamp"],
            s3_path_template="data-lake/ml_models/predictions/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_predictions",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_predictions",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_predictions",
            clickhouse_partition="toYYYYMM(prediction_timestamp)",
        ),
    },
    "ml_model_monitoring": {
        "s3": TableLoadConfig(
            table_name="ml_model_monitoring",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["check_timestamp"],
            s3_path_template="data-lake/ml_models/monitoring/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_monitoring",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["monitoring_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_monitoring",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_monitoring",
            clickhouse_partition="toYYYYMM(check_timestamp)",
        ),
    },
    "ml_model_ab_tests": {
        "s3": TableLoadConfig(
            table_name="ml_model_ab_tests",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["created_at"],
            s3_path_template="data-lake/ml_models/ab_tests/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="ml_model_ab_tests",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["test_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="ml_model_ab_tests",
            destination="clickhouse",
            mode="append",
            clickhouse_table="ml_model_ab_tests",
            clickhouse_partition="toYYYYMM(created_at)",
        ),
    },
    
    # Data Quality Domain (5 tables)
    "data_quality_rules": {
        "s3": TableLoadConfig(
            table_name="data_quality_rules",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/data_quality/rules/",
        ),
        "postgresql": TableLoadConfig(
            table_name="data_quality_rules",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["rule_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="data_quality_rules",
            destination="clickhouse",
            mode="append",
            clickhouse_table="data_quality_rules",
        ),
    },
    "data_quality_checks": {
        "s3": TableLoadConfig(
            table_name="data_quality_checks",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["check_timestamp"],
            s3_path_template="data-lake/data_quality/checks/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="data_quality_checks",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["check_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="data_quality_checks",
            destination="clickhouse",
            mode="append",
            clickhouse_table="data_quality_checks",
            clickhouse_partition="toYYYYMM(check_timestamp)",
        ),
    },
    "data_quality_metrics": {
        "s3": TableLoadConfig(
            table_name="data_quality_metrics",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["check_timestamp"],
            s3_path_template="data-lake/data_quality/metrics/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="data_quality_metrics",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["metric_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="data_quality_metrics",
            destination="clickhouse",
            mode="append",
            clickhouse_table="data_quality_metrics",
            clickhouse_partition="toYYYYMM(check_timestamp)",
        ),
    },
    "data_quality_alerts": {
        "s3": TableLoadConfig(
            table_name="data_quality_alerts",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["alert_timestamp"],
            s3_path_template="data-lake/data_quality/alerts/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="data_quality_alerts",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["alert_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="data_quality_alerts",
            destination="clickhouse",
            mode="append",
            clickhouse_table="data_quality_alerts",
            clickhouse_partition="toYYYYMM(alert_timestamp)",
        ),
    },
    "data_validation_results": {
        "s3": TableLoadConfig(
            table_name="data_validation_results",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["validation_timestamp"],
            s3_path_template="data-lake/data_quality/validation/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="data_validation_results",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="data_validation_results",
            destination="clickhouse",
            mode="append",
            clickhouse_table="data_validation_results",
            clickhouse_partition="toYYYYMM(validation_timestamp)",
        ),
    },
    
    # Configuration Domain (4 tables)
    "configuration_feature_flags": {
        "s3": TableLoadConfig(
            table_name="configuration_feature_flags",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/configuration/feature_flags/",
        ),
        "postgresql": TableLoadConfig(
            table_name="configuration_feature_flags",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["flag_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="configuration_feature_flags",
            destination="clickhouse",
            mode="append",
            clickhouse_table="configuration_feature_flags",  # Match PostgreSQL table name
        ),
    },
    "configuration_feature_flag_history": {
        "s3": TableLoadConfig(
            table_name="configuration_feature_flag_history",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["changed_at"],
            s3_path_template="data-lake/configuration/feature_flags/history/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="configuration_feature_flag_history",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="configuration_feature_flag_history",
            destination="clickhouse",
            mode="append",
            clickhouse_table="configuration_feature_flag_history",  # Match PostgreSQL table name
            clickhouse_partition="toYYYYMM(changed_at)",
        ),
    },
    "configuration_system_settings": {
        "s3": TableLoadConfig(
            table_name="configuration_system_settings",
            destination="s3",
            mode="overwrite",
            format="parquet",
            s3_path_template="data-lake/configuration/system_settings/",
        ),
        "postgresql": TableLoadConfig(
            table_name="configuration_system_settings",
            destination="postgresql",
            mode="upsert",
            conflict_resolution="update",
            primary_key=["setting_id"],
        ),
        "clickhouse": TableLoadConfig(
            table_name="configuration_system_settings",
            destination="clickhouse",
            mode="append",
            clickhouse_table="configuration_system_settings",
        ),
    },
    "configuration_system_settings_history": {
        "s3": TableLoadConfig(
            table_name="configuration_system_settings_history",
            destination="s3",
            mode="append",
            format="delta",
            partition_by=["changed_at"],
            s3_path_template="data-lake/configuration/system_settings/history/{date}/",
        ),
        "postgresql": TableLoadConfig(
            table_name="configuration_system_settings_history",
            destination="postgresql",
            mode="append",
        ),
        "clickhouse": TableLoadConfig(
            table_name="configuration_system_settings_history",
            destination="clickhouse",
            mode="append",
            clickhouse_table="configuration_system_settings_history",
            clickhouse_partition="toYYYYMM(changed_at)",
        ),
    },
}


def get_table_load_config(
    table_name: str,
    destination: str
) -> Optional[TableLoadConfig]:
    """
    Get load configuration for a table and destination.
    
    Args:
        table_name: Name of the table
        destination: Destination type ("s3", "postgresql", "clickhouse")
        
    Returns:
        TableLoadConfig if found, None otherwise
    """
    table_configs = TABLE_LOAD_CONFIGS.get(table_name)
    if not table_configs:
        return None
    
    return table_configs.get(destination.lower())


def get_all_table_configs(table_name: str) -> Dict[str, TableLoadConfig]:
    """
    Get all load configurations for a table across all destinations.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary mapping destination to TableLoadConfig
    """
    return TABLE_LOAD_CONFIGS.get(table_name, {})

