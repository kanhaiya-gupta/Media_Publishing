# ClickHouse Schema Review & Best Practices

## ✅ What We Have Right

### 1. Table Engines
- ✅ **MergeTree**: Used correctly for event tables and time-series data
- ✅ **SummingMergeTree**: Used correctly for aggregated metrics (performance tables)
- ✅ **ReplacingMergeTree**: Used correctly for reference data that needs deduplication

### 2. Partitioning
- ✅ All event tables partitioned by month using `toYYYYMM(timestamp_column)`
- ✅ Aggregated tables partitioned by date column
- ✅ Partitioning strategy is consistent and appropriate

### 3. TTL (Time To Live)
- ✅ Event tables: 1 year retention
- ✅ Aggregated tables: 2 years retention
- ✅ Reference tables: No TTL (appropriate)

### 4. Ordering Keys
- ✅ Most tables have appropriate ordering keys
- ✅ Event tables ordered by (event_id, timestamp)
- ✅ Performance tables ordered by (entity_id, date)

### 5. Data Types
- ✅ UUIDs stored as String (ClickHouse doesn't have native UUID)
- ✅ Booleans stored as UInt8 (0/1)
- ✅ JSON stored as String (flexible)
- ✅ Appropriate numeric types (UInt32, Float32, etc.)

## ⚠️ Areas for Improvement

### 1. Missing Database Creation
**Issue**: Each schema file should create the database if it doesn't exist.

**Fix**: Add to each file:
```sql
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;
```

### 2. Ordering Keys Optimization
**Issue**: Some ordering keys could be optimized for common query patterns.

**Recommendations**:
- Event tables: Order by (timestamp, event_type, entity_id) for time-range queries
- Performance tables: Order by (date, entity_id) for date-range queries
- Reference tables: Current ordering is fine

### 3. Missing Materialized Views
**Issue**: We have summary tables but could add materialized views for real-time aggregations.

**Recommendation**: Add materialized views for:
- Real-time session summaries
- Real-time article performance
- Real-time user engagement

### 4. Compression Settings
**Issue**: No explicit compression settings.

**Recommendation**: Add compression codec for better storage efficiency:
```sql
-- Example for String columns
column_name String CODEC(ZSTD(3))
```

### 5. Sampling Keys
**Issue**: No sampling keys for large tables.

**Recommendation**: Add sampling keys for very large event tables:
```sql
SAMPLE BY cityHash64(event_id)
```

### 6. Missing Indexes
**Issue**: ClickHouse uses primary key for indexing, but we could optimize with:
- Projections for common query patterns
- Materialized columns for frequently filtered fields

### 7. Date Column Consistency
**Issue**: Some tables have `event_date` derived from `event_timestamp`, but it's not always populated automatically.

**Recommendation**: Use materialized columns or ensure application populates both.

### 8. Missing Tables
**Check**: Need to verify all PostgreSQL tables that should be in ClickHouse are present.

## 🔧 Recommended Improvements

### 1. Add Compression
Add compression codecs to reduce storage:
```sql
-- For String columns with JSON
metadata String CODEC(ZSTD(3))

-- For numeric columns
total_views UInt32 CODEC(Delta, ZSTD)
```

### 2. Add Projections
Add projections for common query patterns:
```sql
-- Example: Projection for filtering by brand and date
ALTER TABLE customer_events 
ADD PROJECTION brand_date_projection
(
    SELECT brand_id, event_date, event_type, count()
    GROUP BY brand_id, event_date, event_type
);
```

### 3. Add Materialized Views
Create materialized views for real-time aggregations:
```sql
CREATE MATERIALIZED VIEW customer_sessions_realtime_mv
ENGINE = SummingMergeTree()
ORDER BY (brand_id, toStartOfHour(session_start))
AS SELECT
    brand_id,
    toStartOfHour(session_start) as hour,
    count() as sessions,
    sum(total_events) as total_events
FROM customer_sessions
GROUP BY brand_id, hour;
```

### 4. Optimize Ordering Keys
Reorder keys for better query performance:
```sql
-- Event tables: Put timestamp first for time-range queries
ORDER BY (event_timestamp, event_id, brand_id)

-- Performance tables: Put date first for date-range queries
ORDER BY (performance_date, article_id, brand_id)
```

### 5. Add Settings
Add performance settings:
```sql
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400,
    max_bytes_to_merge_at_max_space_in_pool = 161061273600
```

## 📊 Completeness Check

### Tables Present ✅
- ✅ Customer: sessions, events, features, segments, churn, recommendations, conversions
- ✅ Editorial: articles, authors, events, performance (article/author/category), headline tests, trending topics
- ✅ Company: events, content performance, department performance, employee engagement, communications analytics
- ✅ ML: predictions, monitoring, A/B tests
- ✅ Data Quality: metrics, alerts, checks
- ✅ Compliance: consent, data subject requests, retention executions, breach incidents
- ✅ Security: API key usage
- ✅ Audit: logs, data access, security events, data lineage, compliance events
- ✅ Configuration: feature flag usage, history
- ✅ Base: All reference tables

### Missing Tables (if any)
- Need to verify against PostgreSQL schemas

## 🎯 World-Class Checklist

- ✅ Proper table engines for use cases
- ✅ Appropriate partitioning strategy
- ✅ TTL for data retention
- ✅ Optimized ordering keys
- ⚠️ Compression codecs (recommended)
- ⚠️ Materialized views (recommended)
- ⚠️ Projections (recommended)
- ✅ Proper data types
- ✅ Consistent naming conventions
- ✅ Documentation (README)
- ✅ Initialization scripts

## 🚀 Next Steps

1. Add compression codecs to all tables
2. Add materialized views for real-time aggregations
3. Add projections for common query patterns
4. Optimize ordering keys based on query patterns
5. Add performance settings
6. Create query examples in README
7. Add monitoring queries for schema health

