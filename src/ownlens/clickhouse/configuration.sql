-- ============================================================================
-- OwnLens - Configuration Management Schema (ClickHouse)
-- ============================================================================
-- Feature flags and system configuration analytics
-- Note: Configuration data is managed in PostgreSQL, this is for analytics
-- ============================================================================

USE ownlens_analytics;

-- Feature flag usage analytics
CREATE TABLE IF NOT EXISTS configuration_feature_flags (
    usage_id String,  -- UUID as String
    flag_id String,  -- UUID as String
    flag_code String,
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Usage details
    was_enabled UInt8,  -- Boolean (whether flag was enabled for this user)
    flag_value String,  -- JSON
    
    -- Timestamps
    checked_at DateTime DEFAULT now(),
    checked_date Date,
    
    -- Metadata
    metadata String  -- JSON
) ENGINE = MergeTree()
ORDER BY (usage_id, checked_at)
PARTITION BY toYYYYMM(checked_at)
TTL checked_at + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;

-- Feature flag change history (analytics)
CREATE TABLE IF NOT EXISTS configuration_feature_flag_history (
    history_id String,  -- UUID as String
    flag_id String,  -- UUID as String
    flag_code String,
    
    -- Change details
    change_type String,  -- 'CREATED', 'ENABLED', 'DISABLED', 'UPDATED', 'DELETED'
    old_value String,  -- JSON
    new_value String,  -- JSON
    
    -- Who and when
    changed_by String,  -- UUID as String
    changed_at DateTime DEFAULT now(),
    
    -- Metadata
    change_reason String,
    metadata String  -- JSON
) ENGINE = MergeTree()
ORDER BY (history_id, changed_at)
PARTITION BY toYYYYMM(changed_at)
TTL changed_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- System configuration settings
CREATE TABLE IF NOT EXISTS configuration_system_settings (
    setting_id String,  -- UUID as String
    setting_name String,
    setting_code String,  -- 'kafka_bootstrap_servers', 'clickhouse_host', etc.
    setting_category String,  -- 'database', 'kafka', 'spark', 'ml', 'api', 'monitoring'
    setting_value String,
    setting_type String,  -- 'STRING', 'INTEGER', 'DECIMAL', 'BOOLEAN', 'JSON'
    default_value String,
    description String,
    is_encrypted UInt8 DEFAULT 0,  -- Boolean
    is_secret UInt8 DEFAULT 0,  -- Boolean
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    environment String,  -- 'development', 'staging', 'production'
    is_active UInt8 DEFAULT 1,  -- Boolean
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String,  -- UUID as String
    updated_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (setting_code, company_id, brand_id, environment)
SETTINGS index_granularity = 8192;

-- System configuration history
CREATE TABLE IF NOT EXISTS configuration_system_settings_history (
    history_id String,  -- UUID as String
    setting_id String,  -- UUID as String
    change_type String,  -- 'CREATED', 'UPDATED', 'DELETED'
    old_value String,
    new_value String,
    changed_by String,  -- UUID as String
    changed_at DateTime DEFAULT now(),
    change_reason String,
    metadata String  -- JSON as String
) ENGINE = MergeTree()
ORDER BY (history_id, changed_at)
PARTITION BY toYYYYMM(changed_at)
TTL changed_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

