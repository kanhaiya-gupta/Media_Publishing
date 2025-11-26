-- ============================================================================
-- OwnLens - Audit & Logging Schema (ClickHouse)
-- ============================================================================
-- Comprehensive audit logging for compliance, security, and data lineage
-- ============================================================================

USE ownlens_analytics;

-- Audit logs (all actions)
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id String,  -- UUID as String
    
    -- Who
    user_id String,  -- UUID as String
    api_key_id String,  -- UUID as String
    session_id String,  -- UUID as String
    
    -- What
    action String,  -- 'READ', 'WRITE', 'DELETE', 'UPDATE', etc.
    resource_type String,
    resource_id String,  -- UUID as String
    resource_identifier String,
    
    -- Where
    ip_address String,
    user_agent String,
    endpoint String,
    method String,  -- HTTP method
    
    -- When
    event_timestamp DateTime DEFAULT now(),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    -- Result
    success UInt8,  -- Boolean
    status_code UInt16,
    error_message String,
    error_code String,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Request/Response details
    request_data String CODEC(ZSTD(3)),  -- JSON
    response_data String CODEC(ZSTD(3)),  -- JSON
    request_size_bytes UInt32,
    response_size_bytes UInt32,
    response_time_ms UInt32,
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_timestamp, action, resource_type, brand_id, log_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Track all data changes (INSERT, UPDATE, DELETE)
CREATE TABLE IF NOT EXISTS audit_data_changes (
    change_id String,  -- UUID as String
    
    -- Who
    user_id String,  -- UUID as String
    api_key_id String,  -- UUID as String
    
    -- What
    table_name String,
    record_id String,  -- UUID as String (Primary key of changed record)
    change_type String,  -- 'INSERT', 'UPDATE', 'DELETE'
    
    -- Changes
    old_values String CODEC(ZSTD(3)),  -- JSON (Previous values for UPDATE/DELETE)
    new_values String CODEC(ZSTD(3)),  -- JSON (New values for INSERT/UPDATE)
    changed_fields Array(String),  -- Array of changed field names
    
    -- When
    change_timestamp DateTime DEFAULT now(),
    change_date Date MATERIALIZED toDate(change_timestamp),
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Transaction info
    transaction_id String,  -- Database transaction ID
    batch_id String,  -- UUID as String (Batch processing ID)
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (change_timestamp, table_name, change_type, brand_id, change_id)
PARTITION BY toYYYYMM(change_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Data access logs (who accessed what data)
CREATE TABLE IF NOT EXISTS audit_data_access (
    access_id String,  -- UUID as String
    
    -- Who
    user_id String,  -- UUID as String
    api_key_id String,  -- UUID as String
    
    -- What
    resource_type String,
    resource_id String,  -- UUID as String
    resource_identifier String,
    access_type String,  -- 'QUERY', 'EXPORT', 'API_READ', etc.
    
    -- Query details
    query_text String,
    query_params String,  -- JSON
    rows_returned UInt32,
    execution_time_ms UInt32,
    
    -- When
    access_timestamp DateTime DEFAULT now(),
    access_date Date MATERIALIZED toDate(access_timestamp),
    
    -- Where
    ip_address String,
    user_agent String,
    endpoint String,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Compliance
    data_classification String,  -- 'public', 'internal', 'confidential', etc.
    gdpr_relevant UInt8 DEFAULT 0,  -- Boolean
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (access_timestamp, resource_type, user_id, access_id)
PARTITION BY toYYYYMM(access_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Security events
CREATE TABLE IF NOT EXISTS audit_security_events (
    event_id String,  -- UUID as String
    
    -- Who
    user_id String,  -- UUID as String
    ip_address String,
    user_agent String,
    
    -- What
    event_type String,  -- 'LOGIN_SUCCESS', 'LOGIN_FAILED', etc.
    event_severity String,  -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    description String,
    
    -- Result
    success UInt8,  -- Boolean
    failure_reason String,
    
    -- When
    event_timestamp DateTime DEFAULT now(),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    session_id String,  -- UUID as String
    
    -- Additional data
    event_data String,  -- JSON
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_timestamp, event_type, event_severity, event_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Data lineage (where data comes from, how it's transformed)
CREATE TABLE IF NOT EXISTS audit_data_lineage (
    lineage_id String,  -- UUID as String
    
    -- Source
    source_type String,  -- 'kafka', 'api', 'database', 'file', 'ml_model'
    source_identifier String,
    source_record_id String,  -- UUID as String
    
    -- Destination
    destination_type String,  -- 'database', 'ml_model', 'api', 'file'
    destination_identifier String,
    destination_record_id String,  -- UUID as String
    
    -- Transformation
    transformation_type String,  -- 'aggregation', 'filter', 'join', etc.
    transformation_details String,  -- JSON
    
    -- When
    transformation_timestamp DateTime DEFAULT now(),
    transformation_date Date MATERIALIZED toDate(transformation_timestamp),
    
    -- Context
    pipeline_id String,
    batch_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (transformation_timestamp, source_type, destination_type, lineage_id)
PARTITION BY toYYYYMM(transformation_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

-- Compliance events (GDPR, data retention, etc.)
CREATE TABLE IF NOT EXISTS audit_compliance_events (
    event_id String,  -- UUID as String
    
    -- What
    compliance_type String,  -- 'GDPR_ACCESS_REQUEST', 'GDPR_DELETION_REQUEST', etc.
    event_type String,  -- 'REQUEST', 'PROCESSED', 'COMPLETED', 'FAILED'
    description String,
    
    -- Who
    user_id String,  -- UUID as String
    requested_by String,  -- UUID as String
    processed_by String,  -- UUID as String
    
    -- What data
    resource_type String,
    resource_ids String,  -- JSON array
    data_scope String,  -- JSON
    
    -- Status
    status String,  -- 'PENDING', 'PROCESSING', 'COMPLETED', etc.
    status_message String,
    
    -- When
    requested_at DateTime,
    processed_at DateTime,
    completed_at DateTime,
    event_timestamp DateTime DEFAULT now(),
    event_date Date MATERIALIZED toDate(event_timestamp),
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Results
    records_affected UInt32,
    data_export_url String,
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (event_id)
PARTITION BY toYYYYMM(event_timestamp)
TTL created_at + INTERVAL 1 YEAR
SETTINGS 
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400;

