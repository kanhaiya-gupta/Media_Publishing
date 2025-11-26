# ClickHouse Schema for OwnLens Media Publishing

This directory contains ClickHouse schema definitions for analytics and event data warehouse.

## Overview

ClickHouse is used for:
- High-performance analytics queries
- Time-series event data storage
- Aggregated metrics and summaries
- ML model predictions and monitoring
- Data quality metrics

## Schema Files

The schemas are organized by domain, matching the PostgreSQL schema structure:

### Core Schemas
- **`base.sql`** - Reference data tables (companies, brands, countries, cities, categories, devices)
- **`init_schema.sql`** - Initialization script to run all schemas

### Domain Schemas
- **`customer.sql`** - Customer domain (sessions, events, user features, churn predictions, recommendations)
- **`editorial_core.sql`** - Editorial core (articles, authors, content events, performance metrics)
- **`editorial_content.sql`** - Editorial content (content versioning analytics)
- **`editorial_media.sql`** - Editorial media (media assets, usage tracking)
- **`company.sql`** - Company domain (internal content events, department performance, employee engagement)
- **`ml_models.sql`** - ML models (predictions, monitoring, A/B tests)
- **`data_quality.sql`** - Data quality (metrics, alerts, checks)
- **`compliance.sql`** - Compliance (GDPR, consent, data retention, breach incidents)
- **`security.sql`** - Security (API key usage tracking)
- **`audit.sql`** - Audit (logs, data access, security events, data lineage, compliance events)
- **`configuration.sql`** - Configuration (feature flag usage analytics)

### Other Files
- **`example.sql`** - Example schema file showing ClickHouse table structure patterns
- **`init_schema.sql`** - Instructions for running all schemas
- **`init_schema.sh`** - Bash script to run all schemas in order

## Key Design Decisions

### Data Types
- **UUIDs**: Stored as `String` (ClickHouse doesn't have native UUID type)
- **Booleans**: Stored as `UInt8` (0 or 1)
- **JSON**: Stored as `String` (ClickHouse JSON type available but String is more flexible)
- **Timestamps**: `DateTime` for timestamps, `Date` for dates
- **Decimals**: `Float32` or `Float64` depending on precision needs

### Table Engines
- **MergeTree**: For event tables and time-series data
- **SummingMergeTree**: For aggregated metrics that need automatic summation

### Partitioning
- All tables partitioned by month using `toYYYYMM(date_column)`
- Enables efficient data management and query performance

### TTL (Time To Live)
- Event tables: 1 year retention
- Aggregated tables: 2 years retention
- Can be adjusted based on business requirements

### Ordering Keys
- Optimized for common query patterns
- Typically include entity IDs and timestamps

## Table Categories

### 1. Event Tables
Raw event data from Kafka streams:
- `customer_events`
- `editorial_content_events`
- `company_content_events`

### 2. Performance Tables
Aggregated performance metrics:
- `editorial_article_performance`
- `editorial_author_performance`
- `editorial_category_performance`
- `company_content_performance`

### 3. ML Tables
Machine learning related:
- `ml_model_predictions`
- `ml_model_monitoring`
- `customer_user_features`

### 4. Analytics Tables
Analytics and summaries:
- `customer_sessions`
- `session_summary_daily`
- `article_performance_summary_daily`

### 5. Data Quality Tables
Data quality monitoring:
- `data_quality_metrics`
- `data_quality_alerts`

### 6. Audit Tables
Compliance and audit:
- `audit_logs`

## Usage

### Creating the Schema

**Option 1: Run all schemas at once**
```bash
clickhouse-client < init_schema.sql
```

**Option 2: Run schemas individually**
```bash
clickhouse-client < base.sql
clickhouse-client < customer.sql
clickhouse-client < editorial_core.sql
# ... etc
```

**Note:** Run `base.sql` first as other schemas may reference it.

### Querying Data

```sql
-- Example: Get daily article views
SELECT 
    performance_date,
    article_id,
    total_views,
    engagement_score
FROM editorial_article_performance
WHERE brand_id = 'your-brand-id'
  AND performance_date >= today() - 30
ORDER BY total_views DESC
LIMIT 100;
```

### Inserting Data

```sql
-- Example: Insert customer event
INSERT INTO customer_events VALUES
(
    'event-id',
    'session-id',
    'user-id',
    'brand-id',
    'article_view',
    now(),
    'article-id',
    'Article Title',
    'news',
    'category-id',
    'https://example.com/article',
    'US',
    'city-id',
    'UTC',
    'device-type-id',
    'os-id',
    'browser-id',
    '{}',
    '{}',
    now(),
    today()
);
```

## Differences from PostgreSQL Schema

1. **No Foreign Keys**: ClickHouse doesn't support foreign key constraints
2. **No Triggers**: Business logic handled in application layer
3. **No Stored Procedures**: Use application code or ClickHouse functions
4. **Different Indexing**: ClickHouse uses primary key for indexing
5. **Partitioning**: Required for large tables, different syntax
6. **TTL**: Built-in data retention, different from PostgreSQL

## Performance Considerations

1. **Partitioning**: Always partition by date for time-series data
2. **Ordering Keys**: Choose keys that match common query patterns
3. **Materialized Views**: Use for pre-aggregated data
4. **TTL**: Set appropriate retention periods
5. **Batch Inserts**: Insert data in batches for better performance

## Maintenance

### Adding New Partitions
Partitions are created automatically when data is inserted. For manual creation:

```sql
ALTER TABLE customer_events ADD PARTITION '202501';
```

### Updating TTL
```sql
ALTER TABLE customer_events MODIFY TTL created_at + INTERVAL 2 YEAR;
```

### Dropping Old Data
```sql
ALTER TABLE customer_events DELETE WHERE created_at < now() - INTERVAL 1 YEAR;
```

## Notes

- UUIDs are stored as strings for compatibility
- JSON fields are stored as strings; parse with `JSONExtract*` functions
- Boolean values use UInt8 (0 = false, 1 = true)
- Timestamps are stored in UTC
- All tables use `now()` for default timestamps

