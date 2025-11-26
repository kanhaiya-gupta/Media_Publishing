-- ============================================================================
-- OwnLens - Data Quality Schema (ClickHouse)
-- ============================================================================
-- Data quality checks, validation rules, data quality metrics
-- ============================================================================

USE ownlens_analytics;

-- Data quality validation rules
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id String,  -- UUID as String
    rule_name String,
    rule_code String,
    rule_type String,  -- 'completeness', 'accuracy', 'consistency', 'validity', 'timeliness', 'uniqueness'
    table_name String,
    column_name String,  -- NULL for table-level rules
    domain String,  -- 'customer', 'editorial', 'company'
    rule_expression String,  -- SQL expression or validation logic
    rule_threshold Float64,  -- Threshold for rule (e.g., 0.95 for 95% completeness)
    rule_severity String DEFAULT 'WARNING',  -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    is_active UInt8 DEFAULT 1,  -- Boolean
    is_enforced UInt8 DEFAULT 0,  -- Boolean (Whether to block data if rule fails)
    description String,
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (rule_id)
SETTINGS index_granularity = 8192;

-- Data quality metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id String,  -- UUID as String
    
    -- Metric scope
    table_name String,
    column_name String,
    domain String,  -- 'customer', 'editorial', 'company'
    metric_date Date,
    
    -- Completeness metrics
    total_records UInt64 DEFAULT 0,
    null_count UInt64 DEFAULT 0,
    null_percentage Float32,
    completeness_score Float32,
    
    -- Accuracy metrics
    accuracy_score Float32,
    invalid_count UInt64 DEFAULT 0,
    invalid_percentage Float32,
    
    -- Consistency metrics
    consistency_score Float32,
    duplicate_count UInt64 DEFAULT 0,
    duplicate_percentage Float32,
    
    -- Validity metrics
    validity_score Float32,
    out_of_range_count UInt64 DEFAULT 0,
    format_errors UInt64 DEFAULT 0,
    
    -- Timeliness metrics
    timeliness_score Float32,
    stale_records UInt64 DEFAULT 0,
    avg_data_age_hours Float32,
    
    -- Uniqueness metrics
    uniqueness_score Float32,
    unique_count UInt64 DEFAULT 0,
    
    -- Overall quality
    overall_quality_score Float32,
    quality_grade String,  -- 'A', 'B', 'C', 'D', 'F'
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (metric_date, table_name, column_name, domain)
PARTITION BY toYYYYMM(metric_date)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Data quality alerts
CREATE TABLE IF NOT EXISTS data_quality_alerts (
    alert_id String,  -- UUID as String
    rule_id String,  -- UUID as String
    check_id String,  -- UUID as String
    
    -- Alert details
    alert_type String,  -- 'QUALITY_DROP', 'RULE_FAILURE', 'THRESHOLD_BREACH'
    alert_severity String,  -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    alert_title String,
    alert_message String,
    
    -- Alert status
    alert_status String,  -- 'OPEN', 'ACKNOWLEDGED', 'RESOLVED', 'CLOSED'
    acknowledged_at DateTime,
    acknowledged_by String,  -- UUID as String
    resolved_at DateTime,
    resolved_by String,  -- UUID as String
    
    -- Alert context
    table_name String,
    column_name String,
    domain String,
    quality_score Float32,
    threshold Float32,
    
    -- Timestamps
    alert_timestamp DateTime DEFAULT now(),
    alert_date Date,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (alert_timestamp, alert_severity, alert_status, alert_id)
PARTITION BY toYYYYMM(alert_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Data quality checks (execution results)
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id String,  -- UUID as String
    rule_id String,  -- UUID as String
    
    -- Check execution
    check_timestamp DateTime DEFAULT now(),
    check_date Date MATERIALIZED toDate(check_timestamp),
    check_status String,  -- 'PASSED', 'FAILED', 'WARNING', 'ERROR'
    
    -- Check results
    records_checked UInt64 DEFAULT 0,
    records_passed UInt64 DEFAULT 0,
    records_failed UInt64 DEFAULT 0,
    pass_rate Float32,
    quality_score Float32,
    
    -- Check details
    check_result String,  -- JSON
    failed_records String,  -- JSON
    error_message String,
    
    -- Execution context
    execution_job_id String,
    execution_duration_sec UInt32,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (check_timestamp, rule_id, check_status, check_id)
PARTITION BY toYYYYMM(check_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Data validation results (for individual records)
CREATE TABLE IF NOT EXISTS data_validation_results (
    validation_id String,  -- UUID as String
    rule_id String,  -- UUID as String
    table_name String,
    record_id String,  -- UUID as String (Primary key of validated record)
    column_name String,
    validation_status String,  -- 'PASSED', 'FAILED', 'WARNING'
    validation_message String,
    validation_timestamp DateTime DEFAULT now(),
    validation_date Date MATERIALIZED toDate(validation_timestamp),
    record_data String,  -- JSON (Snapshot of record data)
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    batch_id String,  -- UUID as String
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (validation_timestamp, table_name, record_id, validation_id)
PARTITION BY toYYYYMM(validation_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

