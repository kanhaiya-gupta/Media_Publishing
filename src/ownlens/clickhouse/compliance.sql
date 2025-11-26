-- ============================================================================
-- OwnLens - Compliance & Data Privacy Schema (ClickHouse)
-- ============================================================================
-- GDPR compliance, data privacy, consent management, data retention analytics
-- ============================================================================

USE ownlens_analytics;

-- User consent tracking (GDPR compliance) - analytics only
CREATE TABLE IF NOT EXISTS compliance_user_consent (
    consent_id String,  -- UUID as String
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Consent details
    consent_type String,  -- 'analytics', 'marketing', 'personalization', etc.
    consent_status String,  -- 'granted', 'denied', 'withdrawn', 'expired'
    consent_method String,  -- 'explicit', 'implicit', 'opt_in', 'opt_out'
    
    -- Consent metadata
    consent_version String,
    ip_address String,
    
    -- Timestamps
    granted_at DateTime,
    withdrawn_at DateTime,
    expires_at DateTime,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    
    -- Metadata
    metadata String  -- JSON
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (consent_id)
SETTINGS index_granularity = 8192;

-- Data subject requests (GDPR) - analytics
CREATE TABLE IF NOT EXISTS compliance_data_subject_requests (
    request_id String,  -- UUID as String
    user_id String,  -- UUID as String
    external_user_id String,
    
    -- Request details
    request_type String,  -- 'ACCESS', 'DELETION', 'RECTIFICATION', etc.
    request_status String,  -- 'PENDING', 'VERIFIED', 'PROCESSING', etc.
    request_priority String,  -- 'LOW', 'NORMAL', 'HIGH', 'URGENT'
    
    -- Requestor information
    requestor_email String,
    verification_status String,  -- 'PENDING', 'VERIFIED', 'FAILED'
    
    -- Processing
    requested_at DateTime DEFAULT now(),
    verified_at DateTime,
    processing_started_at DateTime,
    completed_at DateTime,
    due_date DateTime,
    
    -- Results
    records_found UInt32,
    records_processed UInt32,
    records_deleted UInt32,
    
    -- Rejection details
    rejection_reason String,
    rejected_at DateTime,
    rejected_by String,  -- UUID as String
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (request_id)
SETTINGS index_granularity = 8192;

-- Data retention execution log (analytics)
CREATE TABLE IF NOT EXISTS compliance_retention_executions (
    execution_id String,  -- UUID as String
    policy_id String,  -- UUID as String
    
    -- Execution details
    execution_status String,  -- 'SUCCESS', 'FAILED', 'PARTIAL'
    execution_started_at DateTime,
    execution_completed_at DateTime,
    execution_duration_sec UInt32,
    
    -- Results
    records_processed UInt64 DEFAULT 0,
    records_archived UInt64 DEFAULT 0,
    records_deleted UInt64 DEFAULT 0,
    records_anonymized UInt64 DEFAULT 0,
    
    -- Errors
    error_message String,
    error_count UInt32 DEFAULT 0,
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (execution_id, execution_started_at)
PARTITION BY toYYYYMM(execution_started_at)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Data retention policies
CREATE TABLE IF NOT EXISTS compliance_retention_policies (
    policy_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    policy_name String,
    policy_type String,  -- 'customer', 'editorial', 'company', 'audit', 'ml_models'
    table_name String,  -- Specific table (NULL for all tables of type)
    retention_period_days UInt32,  -- Days to retain data
    retention_period_months UInt32,  -- Alternative: months
    retention_period_years UInt32,  -- Alternative: years
    archive_before_delete UInt8 DEFAULT 0,  -- Boolean
    archive_location String,  -- Archive storage location
    delete_after_retention UInt8 DEFAULT 1,  -- Boolean
    deletion_method String,  -- 'SOFT_DELETE', 'HARD_DELETE', 'ANONYMIZE'
    is_active UInt8 DEFAULT 1,  -- Boolean
    last_executed_at DateTime,
    next_execution_at DateTime,
    description String,
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (policy_id)
SETTINGS index_granularity = 8192;

-- Track anonymized data
CREATE TABLE IF NOT EXISTS compliance_anonymized_data (
    anonymization_id String,  -- UUID as String
    original_table_name String,
    original_record_id String,  -- UUID as String
    original_user_id String,  -- UUID as String
    anonymization_method String,  -- 'HASH', 'MASK', 'RANDOMIZE', 'DELETE'
    anonymization_date DateTime DEFAULT now(),
    anonymized_by String,  -- UUID as String
    anonymization_reason String,  -- 'GDPR_DELETION', 'RETENTION_POLICY', 'USER_REQUEST'
    related_request_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (anonymization_date, original_table_name, anonymization_id)
PARTITION BY toYYYYMM(anonymization_date)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Privacy Impact Assessments (PIA/DPIA)
CREATE TABLE IF NOT EXISTS compliance_privacy_assessments (
    assessment_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    assessment_name String,
    assessment_type String,  -- 'PIA', 'DPIA', 'RISK_ASSESSMENT'
    assessment_status String,  -- 'DRAFT', 'IN_REVIEW', 'APPROVED', 'REJECTED'
    description String,
    scope String,  -- JSON as String
    data_types_processed String,  -- JSON array as String
    legal_basis String,  -- JSON as String
    risks_identified String,  -- JSON as String
    mitigation_measures String,  -- JSON as String
    assessment_date Date,
    reviewed_by String,  -- UUID as String
    reviewed_at DateTime,
    approved_by String,  -- UUID as String
    approved_at DateTime,
    next_review_date Date,
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (assessment_id)
SETTINGS index_granularity = 8192;

-- Data breach incidents (analytics)
CREATE TABLE IF NOT EXISTS compliance_breach_incidents (
    incident_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Incident details
    incident_type String,  -- 'UNAUTHORIZED_ACCESS', 'DATA_LEAK', etc.
    incident_severity String,  -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    description String,
    
    -- Affected data
    data_types_affected String,  -- JSON array
    records_affected UInt32,
    users_affected UInt32,
    personal_data_affected UInt8 DEFAULT 0,  -- Boolean
    
    -- Incident timeline
    discovered_at DateTime,
    occurred_at DateTime,
    contained_at DateTime,
    resolved_at DateTime,
    
    -- Response
    incident_status String,  -- 'DISCOVERED', 'INVESTIGATING', etc.
    response_actions String,  -- JSON
    notification_sent UInt8 DEFAULT 0,  -- Boolean
    notification_date DateTime,
    regulatory_notification UInt8 DEFAULT 0,  -- Boolean
    regulatory_notification_date DateTime,
    
    -- Responsible parties
    discovered_by String,  -- UUID as String
    investigated_by String,  -- UUID as String
    resolved_by String,  -- UUID as String
    
    -- Metadata
    metadata String,  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (incident_id)
SETTINGS index_granularity = 8192;

