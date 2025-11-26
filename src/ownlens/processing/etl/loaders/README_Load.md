# ETL Load Phase Implementation Roadmap
## Complete Plan for Loading Transformed Data to S3/MinIO, PostgreSQL, and ClickHouse

This document outlines the comprehensive roadmap for implementing the Load phase of the ETL pipeline, which will load transformed data to three destinations:
1. **S3/MinIO** - Data Lake storage (Parquet/Delta format)
2. **PostgreSQL** - Operational database (upsert/append operations)
3. **ClickHouse** - Analytics database (append-only for analytics)

---

## Overview

**Total Tables:** 79 PostgreSQL tables  
**Destinations:** 3 (S3/MinIO, PostgreSQL, ClickHouse)  
**Current Status:** Loaders exist but need integration and table-specific configurations

### Load Strategy

| Destination | Purpose | Write Mode | Format | Partitioning |
|-------------|---------|------------|--------|--------------|
| **S3/MinIO** | Data Lake / Archive | Overwrite/Append | Parquet/Delta | By date/domain |
| **PostgreSQL** | Operational DB | Upsert/Append | JDBC | N/A |
| **ClickHouse** | Analytics DB | Append | JDBC | By date/domain |

---

## Architecture

### Load Flow

```
Transformed DataFrame
        |
        ├──> S3/MinIO Loader (Data Lake)
        |       └──> s3a://ownlens-data-lake/{domain}/{table}/{date}/
        |
        ├──> PostgreSQL Loader (Operational)
        |       └──> {schema}.{table} (upsert/append)
        |
        └──> ClickHouse Loader (Analytics)
                └──> {database}.{table} (append)
```

### Loader Factory Pattern

Similar to `TransformerFactory`, we'll implement a `LoaderFactory` that:
- Selects the appropriate loader based on destination
- Configures load-specific parameters (mode, format, partitioning)
- Handles table-specific load strategies

---

## Implementation Plan by Destination

### 1. S3/MinIO Loader (`s3_loader.py`)

**Status:** ✅ Basic implementation exists  
**Enhancements Needed:** Table-specific configurations, partitioning strategies

#### Current Implementation

```python
class S3Loader(BaseLoader):
    - Supports Parquet, Delta, JSON formats
    - Supports overwrite/append modes
    - Supports partitioning
```

#### Required Enhancements

1. **Table-Specific Path Mapping**
   - Map each table to its S3 path structure
   - Format: `s3a://{bucket}/data-lake/{domain}/{table}/{partition_date}/`
   - Example: `s3a://ownlens-data-lake/customer/customer_sessions/2025-11-08/`

2. **Partitioning Strategy**
   - **Time-based tables:** Partition by `event_date`, `created_at`, `updated_at`
   - **Reference tables:** No partitioning (small, infrequent updates)
   - **Analytics tables:** Partition by `date`, `month`, `year`

3. **Format Selection**
   - **Event/Time-series data:** Delta Lake (for ACID transactions)
   - **Reference/Lookup data:** Parquet (for read performance)
   - **JSON data:** JSON (for compatibility)

4. **Load Modes**
   - **Initial load:** `overwrite` (full table replacement)
   - **Incremental load:** `append` (new data only)
   - **Upsert load:** Delta Lake merge operations

#### Table-Specific Configurations

| Domain | Tables | Partition By | Format | Mode |
|--------|--------|--------------|--------|------|
| **Base** | All 11 tables | None | Parquet | Overwrite |
| **Customer** | `customer_sessions` | `session_start::date` | Delta | Append |
| **Customer** | `customer_events` | `event_date` | Delta | Append |
| **Customer** | `customer_user_features` | `created_at::date` | Delta | Append |
| **Editorial** | `editorial_articles` | `created_at::date` | Parquet | Overwrite |
| **Editorial** | `editorial_content_events` | `event_date` | Delta | Append |
| **Company** | `company_content_events` | `event_date` | Delta | Append |
| **Audit** | All 6 tables | `event_timestamp::date` | Delta | Append |
| **Security** | `security_api_key_usage` | `usage_timestamp::date` | Delta | Append |
| **Compliance** | All 7 tables | `created_at::date` | Delta | Append |
| **ML Models** | `ml_model_predictions` | `prediction_timestamp::date` | Delta | Append |
| **Data Quality** | All 5 tables | `check_timestamp::date` | Delta | Append |

#### TODO for S3 Loader

- [ ] Create `S3LoaderConfig` class with table-specific configurations
- [ ] Implement path mapping logic (`get_s3_path(table_name, partition_date)`)
- [ ] Add Delta Lake merge support for upsert operations
- [ ] Implement partitioning logic based on table type
- [ ] Add data retention policies (delete old partitions)
- [ ] Add compression options (ZSTD, Snappy, Gzip)
- [ ] Add metadata tracking (table schema, load timestamp, row count)

---

### 2. PostgreSQL Loader (`postgresql_loader.py`)

**Status:** ✅ Basic implementation exists  
**Enhancements Needed:** Upsert support, table-specific strategies, conflict resolution

#### Current Implementation

```python
class PostgreSQLLoader(BaseLoader):
    - Supports append, overwrite, ignore, error modes
    - Uses JDBC for writes
    - Batch loading support
```

#### Required Enhancements

1. **Upsert Support (MERGE)**
   - Implement PostgreSQL `ON CONFLICT` upsert logic
   - Support for composite primary keys
   - Handle unique constraint violations

2. **Table-Specific Load Strategies**
   - **Reference tables:** Upsert (update existing, insert new)
   - **Event tables:** Append only (immutable events)
   - **Analytics tables:** Upsert by date (daily aggregations)

3. **Conflict Resolution**
   - **Primary key conflicts:** Update with new data
   - **Unique constraint conflicts:** Update or skip based on strategy
   - **Foreign key violations:** Log and skip invalid rows

4. **Batch Processing**
   - Split large DataFrames into batches
   - Process batches sequentially or in parallel
   - Track batch progress and failures

#### Table-Specific Configurations

| Domain | Tables | Load Mode | Conflict Resolution | Primary Key |
|--------|--------|-----------|---------------------|-------------|
| **Base** | All 11 tables | Upsert | Update | `{table}_id` |
| **Customer** | `customer_sessions` | Append | Error | `session_id` |
| **Customer** | `customer_events` | Append | Error | `event_id` |
| **Customer** | `customer_user_features` | Upsert | Update | `user_id`, `feature_date` |
| **Editorial** | `editorial_articles` | Upsert | Update | `article_id` |
| **Editorial** | `editorial_content_events` | Append | Error | `event_id` |
| **Company** | `company_content_events` | Append | Error | `event_id` |
| **Audit** | All 6 tables | Append | Error | `{table}_id` |
| **Security** | `security_api_key_usage` | Append | Error | `usage_id` |
| **Compliance** | All 7 tables | Upsert | Update | `{table}_id` |
| **ML Models** | `ml_model_predictions` | Append | Error | `prediction_id` |
| **Data Quality** | All 5 tables | Upsert | Update | `{table}_id` |

#### TODO for PostgreSQL Loader

- [ ] Implement `upsert()` method with `ON CONFLICT` logic
- [ ] Create `PostgreSQLLoaderConfig` with table-specific strategies
- [ ] Add conflict resolution strategies (update, skip, error)
- [ ] Implement batch processing for large DataFrames
- [ ] Add transaction support (rollback on failure)
- [ ] Add retry logic for transient failures
- [ ] Add load statistics tracking (rows inserted/updated/skipped)
- [ ] Add schema validation before load (column mapping, type checking)

---

### 3. ClickHouse Loader (`clickhouse_loader.py`)

**Status:** ✅ Basic implementation exists  
**Enhancements Needed:** Table-specific configurations, partitioning, materialized views

#### Current Implementation

```python
class ClickHouseLoader(BaseLoader):
    - Supports append mode
    - Uses JDBC for writes
    - Batch loading support
```

#### Required Enhancements

1. **Table Mapping**
   - Map PostgreSQL table names to ClickHouse table names
   - Handle schema differences (UUID → String, JSON → String)
   - Support for materialized views and aggregation tables

2. **Partitioning Strategy**
   - **Time-series tables:** Partition by date/month
   - **Analytics tables:** Partition by date for efficient queries
   - **Reference tables:** No partitioning

3. **Data Type Conversions**
   - PostgreSQL UUID → ClickHouse String
   - PostgreSQL JSONB → ClickHouse String
   - PostgreSQL Timestamp → ClickHouse DateTime
   - PostgreSQL Array → ClickHouse Array

4. **Load Modes**
   - **Append:** Default for time-series data
   - **Replace Partition:** For daily/hourly aggregations
   - **Merge:** For upsert scenarios (using ReplacingMergeTree)

#### Table-Specific Configurations

| Domain | PostgreSQL Table | ClickHouse Table | Partition By | Engine |
|--------|-----------------|------------------|--------------|--------|
| **Base** | `companies` | `companies` | None | MergeTree |
| **Base** | `brands` | `brands` | None | MergeTree |
| **Customer** | `customer_sessions` | `customer_sessions` | `toYYYYMM(session_start)` | MergeTree |
| **Customer** | `customer_events` | `customer_events` | `toYYYYMM(event_date)` | MergeTree |
| **Customer** | `customer_user_features` | `customer_user_features` | `toYYYYMM(created_at)` | ReplacingMergeTree |
| **Editorial** | `editorial_articles` | `editorial_articles` | None | MergeTree |
| **Editorial** | `editorial_content_events` | `editorial_content_events` | `toYYYYMM(event_date)` | MergeTree |
| **Company** | `company_content_events` | `company_content_events` | `toYYYYMM(event_date)` | MergeTree |
| **Audit** | All 6 tables | `audit_*` | `toYYYYMM(event_timestamp)` | MergeTree |
| **Security** | `security_api_key_usage` | `security_api_key_usage` | `toYYYYMM(usage_timestamp)` | MergeTree |
| **Compliance** | All 7 tables | `compliance_*` | `toYYYYMM(created_at)` | MergeTree |
| **ML Models** | `ml_model_predictions` | `ml_model_predictions` | `toYYYYMM(prediction_timestamp)` | MergeTree |
| **Data Quality** | All 5 tables | `data_quality_*` | `toYYYYMM(check_timestamp)` | MergeTree |

#### TODO for ClickHouse Loader

- [ ] Create `ClickHouseLoaderConfig` with table mappings
- [ ] Implement data type conversion logic
- [ ] Add partition key extraction from DataFrame
- [ ] Implement `replace_partition()` method for daily aggregations
- [ ] Add support for ReplacingMergeTree (upsert scenarios)
- [ ] Add schema validation (column mapping, type checking)
- [ ] Add batch processing with optimal batch sizes
- [ ] Add compression options (LZ4, ZSTD)
- [ ] Add TTL (Time To Live) configuration for data retention

---

## Loader Factory Implementation

### LoaderFactory Class

```python
class LoaderFactory:
    """
    Factory for creating and configuring loaders based on destination.
    """
    
    LOADER_MAP = {
        "s3": S3Loader,
        "minio": S3Loader,  # Alias for S3
        "postgresql": PostgreSQLLoader,
        "postgres": PostgreSQLLoader,  # Alias
        "clickhouse": ClickHouseLoader,
        "ch": ClickHouseLoader,  # Alias
    }
    
    @staticmethod
    def get_loader(
        destination: str,
        spark: SparkSession,
        config: Optional[ETLConfig] = None,
        loader_config: Optional[Dict[str, Any]] = None
    ) -> BaseLoader:
        """
        Get loader instance for destination.
        
        Args:
            destination: Destination type ("s3", "postgresql", "clickhouse")
            spark: SparkSession instance
            config: ETLConfig instance
            loader_config: Loader-specific configuration
            
        Returns:
            Loader instance
        """
        loader_class = LOADER_MAP.get(destination.lower())
        if not loader_class:
            raise ValueError(f"Unknown destination: {destination}")
        
        return loader_class(spark, config, loader_config)
```

### Table-Specific Load Configuration

```python
class TableLoadConfig:
    """
    Configuration for loading a specific table to a destination.
    """
    
    def __init__(
        self,
        table_name: str,
        destination: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        format: Optional[str] = None,
        conflict_resolution: Optional[str] = None,
        **kwargs
    ):
        self.table_name = table_name
        self.destination = destination
        self.mode = mode
        self.partition_by = partition_by or []
        self.format = format
        self.conflict_resolution = conflict_resolution
        self.kwargs = kwargs
```

---

## Integration with ETL Pipeline

### Updated `run_etl_extraction_transformation.py`

```python
def extract_postgresql_tables(
    spark,
    config,
    tables: Optional[List[str]] = None,
    transformer: Optional[BaseDataTransformer] = None,
    load: bool = False,
    load_destinations: Optional[List[str]] = None  # ["s3", "postgresql", "clickhouse"]
) -> Dict[str, Any]:
    """
    Extract, transform, and optionally load data from PostgreSQL tables.
    
    Args:
        load_destinations: List of destinations to load to
                          ["s3", "postgresql", "clickhouse"]
    """
    # ... existing extraction and transformation logic ...
    
    if load and load_destinations:
        # Get loaders for each destination
        loaders = {}
        for dest in load_destinations:
            loaders[dest] = LoaderFactory.get_loader(dest, spark, config)
        
        # Load transformed data to each destination
        for dest, loader in loaders.items():
            table_config = get_table_load_config(table, dest)
            success = loader.load(
                df_transformed,
                destination=table_config.destination,
                mode=table_config.mode,
                partition_by=table_config.partition_by,
                format=table_config.format,
                **table_config.kwargs
            )
            
            if success:
                logger.info(f"✅ Loaded {table} to {dest}")
            else:
                logger.error(f"❌ Failed to load {table} to {dest}")
```

---

## Domain-by-Domain Load Configuration

### 1. Base Domain (11 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `companies` | `base/companies/` | Upsert | None | Reference table |
| `brands` | `base/brands/` | Upsert | None | Reference table |
| `brand_countries` | `base/brand_countries/` | Upsert | None | Reference table |
| `countries` | `base/countries/` | Upsert | None | Reference table |
| `cities` | `base/cities/` | Upsert | None | Reference table |
| `categories` | `base/categories/` | Upsert | None | Reference table |
| `users` | `base/users/` | Upsert | None | Reference table |
| `user_accounts` | `base/user_accounts/` | Upsert | None | Reference table |
| `device_types` | `base/device_types/` | Upsert | None | Reference table |
| `operating_systems` | `base/operating_systems/` | Upsert | None | Reference table |
| `browsers` | `base/browsers/` | Upsert | None | Reference table |

**Load Strategy:** All reference tables use **Upsert** mode (update existing, insert new).

---

### 2. Customer Domain (8 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `customer_sessions` | `customer/sessions/{date}/` | Append | `session_start` | Time-series |
| `customer_events` | `customer/events/{date}/` | Append | `event_date` | Time-series |
| `customer_user_features` | `customer/features/{date}/` | Upsert | `created_at` | Daily snapshots |
| `customer_user_segments` | `customer/segments/` | Upsert | None | Reference |
| `customer_user_segment_assignments` | `customer/segment_assignments/{date}/` | Upsert | `created_at` | Time-series |
| `customer_churn_predictions` | `customer/churn/{date}/` | Upsert | `prediction_date` | Daily predictions |
| `customer_recommendations` | `customer/recommendations/{date}/` | Upsert | `created_at` | Daily recommendations |
| `customer_conversion_predictions` | `customer/conversions/{date}/` | Upsert | `prediction_date` | Daily predictions |

**Load Strategy:** 
- **Event tables** (`customer_sessions`, `customer_events`): Append only
- **Feature tables** (`customer_user_features`): Upsert by `user_id` + `date`
- **Prediction tables**: Upsert by `user_id` + `prediction_date`

---

### 3. Editorial Domain (17 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `editorial_authors` | `editorial/authors/` | Upsert | None | Reference |
| `editorial_articles` | `editorial/articles/{date}/` | Upsert | `created_at` | Content |
| `editorial_article_performance` | `editorial/performance/articles/{date}/` | Upsert | `date` | Daily aggregations |
| `editorial_author_performance` | `editorial/performance/authors/{date}/` | Upsert | `date` | Daily aggregations |
| `editorial_category_performance` | `editorial/performance/categories/{date}/` | Upsert | `date` | Daily aggregations |
| `editorial_content_events` | `editorial/events/{date}/` | Append | `event_date` | Time-series |
| `editorial_headline_tests` | `editorial/headline_tests/{date}/` | Upsert | `created_at` | A/B tests |
| `editorial_trending_topics` | `editorial/trending/{date}/` | Upsert | `date` | Daily trends |
| `editorial_content_recommendations` | `editorial/recommendations/{date}/` | Upsert | `created_at` | Daily recommendations |
| `editorial_article_content` | `editorial/content/{date}/` | Upsert | `created_at` | Content versions |
| `editorial_content_versions` | `editorial/versions/{date}/` | Append | `created_at` | Version history |
| `editorial_media_assets` | `editorial/media/assets/` | Upsert | None | Media metadata |
| `editorial_media_variants` | `editorial/media/variants/` | Upsert | None | Media variants |
| `editorial_content_media` | `editorial/media/content/{date}/` | Upsert | `created_at` | Media associations |
| `editorial_media_collections` | `editorial/media/collections/` | Upsert | None | Collections |
| `editorial_media_collection_items` | `editorial/media/collections/items/` | Upsert | None | Collection items |
| `editorial_media_usage` | `editorial/media/usage/{date}/` | Append | `usage_timestamp` | Usage tracking |

**Load Strategy:**
- **Content tables** (`editorial_articles`, `editorial_article_content`): Upsert
- **Event tables** (`editorial_content_events`, `editorial_media_usage`): Append
- **Performance tables**: Upsert by `date` (daily aggregations)
- **Reference tables**: Upsert

---

### 4. Company Domain (8 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `company_departments` | `company/departments/` | Upsert | None | Reference |
| `company_employees` | `company/employees/` | Upsert | None | Reference |
| `company_internal_content` | `company/content/{date}/` | Upsert | `created_at` | Content |
| `company_content_performance` | `company/performance/content/{date}/` | Upsert | `date` | Daily aggregations |
| `company_department_performance` | `company/performance/departments/{date}/` | Upsert | `date` | Daily aggregations |
| `company_content_events` | `company/events/{date}/` | Append | `event_date` | Time-series |
| `company_employee_engagement` | `company/engagement/{date}/` | Upsert | `date` | Daily aggregations |
| `company_communications_analytics` | `company/analytics/{date}/` | Upsert | `date` | Daily aggregations |

**Load Strategy:**
- **Event tables** (`company_content_events`): Append
- **Performance/Analytics tables**: Upsert by `date` (daily aggregations)
- **Reference tables**: Upsert

---

### 5. Security Domain (7 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `security_roles` | `security/roles/` | Upsert | None | Reference |
| `security_permissions` | `security/permissions/` | Upsert | None | Reference |
| `security_role_permissions` | `security/role_permissions/` | Upsert | None | Reference |
| `security_user_roles` | `security/user_roles/{date}/` | Upsert | `created_at` | Time-series |
| `security_api_keys` | `security/api_keys/` | Upsert | None | Reference |
| `security_api_key_usage` | `security/api_key_usage/{date}/` | Append | `usage_timestamp` | Time-series |
| `security_user_sessions` | `security/user_sessions/{date}/` | Append | `session_start` | Time-series |

**Load Strategy:**
- **Usage/Event tables** (`security_api_key_usage`, `security_user_sessions`): Append
- **Reference tables**: Upsert

---

### 6. Audit Domain (6 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `audit_logs` | `audit/logs/{date}/` | Append | `event_timestamp` | Time-series |
| `audit_data_changes` | `audit/data_changes/{date}/` | Append | `change_timestamp` | Time-series |
| `audit_data_access` | `audit/data_access/{date}/` | Append | `access_timestamp` | Time-series |
| `audit_security_events` | `audit/security_events/{date}/` | Append | `event_timestamp` | Time-series |
| `audit_data_lineage` | `audit/data_lineage/{date}/` | Append | `created_at` | Time-series |
| `audit_compliance_events` | `audit/compliance_events/{date}/` | Append | `event_timestamp` | Time-series |

**Load Strategy:** All audit tables are **Append only** (immutable audit trail).

---

### 7. Compliance Domain (7 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `compliance_user_consent` | `compliance/consent/{date}/` | Upsert | `created_at` | Time-series |
| `compliance_data_subject_requests` | `compliance/requests/{date}/` | Upsert | `created_at` | Time-series |
| `compliance_retention_policies` | `compliance/policies/` | Upsert | None | Reference |
| `compliance_retention_executions` | `compliance/executions/{date}/` | Append | `execution_timestamp` | Time-series |
| `compliance_anonymized_data` | `compliance/anonymized/{date}/` | Append | `anonymized_at` | Time-series |
| `compliance_privacy_assessments` | `compliance/assessments/{date}/` | Upsert | `created_at` | Time-series |
| `compliance_breach_incidents` | `compliance/breaches/{date}/` | Upsert | `created_at` | Time-series |

**Load Strategy:**
- **Execution/Anonymization tables**: Append (immutable)
- **Other tables**: Upsert

---

### 8. ML Models Domain (6 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `ml_model_registry` | `ml_models/registry/` | Upsert | None | Reference |
| `ml_model_features` | `ml_models/features/` | Upsert | None | Reference |
| `ml_model_training_runs` | `ml_models/training/{date}/` | Upsert | `created_at` | Training history |
| `ml_model_predictions` | `ml_models/predictions/{date}/` | Append | `prediction_timestamp` | Time-series |
| `ml_model_monitoring` | `ml_models/monitoring/{date}/` | Upsert | `check_timestamp` | Monitoring metrics |
| `ml_model_ab_tests` | `ml_models/ab_tests/{date}/` | Upsert | `created_at` | A/B test results |

**Load Strategy:**
- **Predictions table**: Append (immutable predictions)
- **Other tables**: Upsert

---

### 9. Data Quality Domain (5 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `data_quality_rules` | `data_quality/rules/` | Upsert | None | Reference |
| `data_quality_checks` | `data_quality/checks/{date}/` | Upsert | `check_timestamp` | Check history |
| `data_quality_metrics` | `data_quality/metrics/{date}/` | Upsert | `check_timestamp` | Daily metrics |
| `data_quality_alerts` | `data_quality/alerts/{date}/` | Upsert | `alert_timestamp` | Alert history |
| `data_validation_results` | `data_quality/validation/{date}/` | Append | `validation_timestamp` | Validation results |

**Load Strategy:**
- **Validation results**: Append (immutable)
- **Other tables**: Upsert

---

### 10. Configuration Domain (4 tables)

| Table | S3 Path | PostgreSQL Mode | ClickHouse Partition | Notes |
|-------|---------|----------------|---------------------|-------|
| `configuration_feature_flags` | `configuration/feature_flags/` | Upsert | None | Reference |
| `configuration_feature_flag_history` | `configuration/feature_flags/history/{date}/` | Append | `changed_at` | History |
| `configuration_system_settings` | `configuration/system_settings/` | Upsert | None | Reference |
| `configuration_system_settings_history` | `configuration/system_settings/history/{date}/` | Append | `changed_at` | History |

**Load Strategy:**
- **History tables**: Append (immutable history)
- **Current settings**: Upsert

---

## Implementation Phases

### Phase 1: Core Loader Enhancements (Week 1-2)

**Priority: High**

- [ ] Enhance `S3Loader` with table-specific path mapping
- [ ] Enhance `PostgreSQLLoader` with upsert support
- [ ] Enhance `ClickHouseLoader` with data type conversions
- [ ] Create `LoaderFactory` class
- [ ] Create `TableLoadConfig` class
- [ ] Add table-specific configuration files

**Deliverables:**
- Enhanced loaders with table-specific configurations
- LoaderFactory implementation
- Configuration files for all 79 tables

---

### Phase 2: Integration with ETL Pipeline (Week 3)

**Priority: High**

- [ ] Update `run_etl_extraction_transformation.py` to support loading
- [ ] Add `--load` flag with destination selection
- [ ] Add `--load-destinations` flag (e.g., `--load-destinations s3,postgresql,clickhouse`)
- [ ] Integrate LoaderFactory into pipeline
- [ ] Add load statistics tracking

**Deliverables:**
- Updated ETL script with load support
- Load statistics reporting

---

### Phase 3: Table-Specific Configurations (Week 4-5)

**Priority: Medium**

- [ ] Create configuration files for all 79 tables
- [ ] Implement partitioning logic for time-series tables
- [ ] Implement upsert logic for reference tables
- [ ] Add conflict resolution strategies
- [ ] Add data type conversion mappings

**Deliverables:**
- Complete configuration for all tables
- Partitioning and upsert logic

---

### Phase 4: Advanced Features (Week 6-7)

**Priority: Low**

- [ ] Add Delta Lake merge support for S3
- [ ] Add transaction support for PostgreSQL
- [ ] Add TTL configuration for ClickHouse
- [ ] Add data retention policies
- [ ] Add load monitoring and alerting
- [ ] Add load performance optimization

**Deliverables:**
- Advanced load features
- Monitoring and alerting

---

## Testing Strategy

### Unit Tests

- [ ] Test S3Loader with different formats and modes
- [ ] Test PostgreSQLLoader upsert logic
- [ ] Test ClickHouseLoader data type conversions
- [ ] Test LoaderFactory with different destinations
- [ ] Test TableLoadConfig parsing

### Integration Tests

- [ ] Test end-to-end ETL pipeline with loading
- [ ] Test loading to multiple destinations simultaneously
- [ ] Test load failure scenarios and recovery
- [ ] Test load performance with large datasets

### Validation Tests

- [ ] Validate data integrity after load (row counts, checksums)
- [ ] Validate schema compatibility (column mapping, types)
- [ ] Validate partitioning correctness
- [ ] Validate upsert logic (no duplicates, correct updates)

---

## Monitoring and Observability

### Load Metrics

- **Rows loaded:** Total rows loaded per table/destination
- **Load duration:** Time taken to load each table
- **Load failures:** Number of failed loads and reasons
- **Load throughput:** Rows per second loaded

### Load Logging

- Log load start/end times
- Log load statistics (rows inserted/updated/skipped)
- Log load errors with full stack traces
- Log load configuration used

### Load Alerts

- Alert on load failures
- Alert on load performance degradation
- Alert on data quality issues (row count mismatches)

---

## Summary Statistics

| Destination | Tables | Load Mode | Status |
|-------------|--------|-----------|--------|
| **S3/MinIO** | 79 | Overwrite/Append/Delta Merge | ⚠️ Needs Enhancement |
| **PostgreSQL** | 79 | Upsert/Append | ⚠️ Needs Enhancement |
| **ClickHouse** | 79 | Append/Replace Partition | ⚠️ Needs Enhancement |

**Total Implementation:** 0/79 tables fully configured for all destinations

---

## Next Steps

1. ✅ **Phase 1:** Enhance core loaders (S3, PostgreSQL, ClickHouse)
2. ⏭️ **Phase 2:** Integrate loaders into ETL pipeline
3. ⏭️ **Phase 3:** Create table-specific configurations
4. ⏭️ **Phase 4:** Add advanced features and monitoring

---

**Last Updated:** 2025-11-08  
**Status:** Planning phase - Ready for implementation

