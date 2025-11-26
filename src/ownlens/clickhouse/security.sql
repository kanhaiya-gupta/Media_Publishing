-- ============================================================================
-- OwnLens - Security & Access Control Schema (ClickHouse)
-- ============================================================================
-- API key usage tracking and security analytics
-- Note: Roles, permissions, and user sessions are managed in PostgreSQL
-- This schema is for analytics only
-- ============================================================================

USE ownlens_analytics;

-- Security roles (RBAC)
CREATE TABLE IF NOT EXISTS security_roles (
    role_id String,  -- UUID as String
    role_name String,
    role_code String,  -- 'admin', 'data_engineer', 'data_scientist', 'analyst', 'editor', 'auditor'
    description String,
    is_system_role UInt8 DEFAULT 0,  -- Boolean
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    updated_at DateTime DEFAULT toDateTime(0)  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (role_id)
SETTINGS index_granularity = 8192;

-- Security permissions
CREATE TABLE IF NOT EXISTS security_permissions (
    permission_id String,  -- UUID as String
    permission_name String,
    permission_code String,  -- 'read:customer:data', 'write:editorial:content', etc.
    resource_type String,  -- 'customer', 'editorial', 'company', 'ml_models', 'audit'
    action String,  -- 'read', 'write', 'delete', 'execute', 'admin'
    description String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT toDateTime(0)  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (permission_id)
SETTINGS index_granularity = 8192;

-- Role-Permission mapping
CREATE TABLE IF NOT EXISTS security_role_permissions (
    role_permission_id String,  -- UUID as String
    role_id String,  -- UUID as String
    permission_id String,  -- UUID as String
    granted_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    granted_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(granted_at)
ORDER BY (role_id, permission_id)
SETTINGS index_granularity = 8192;

-- User-Role mapping (can have multiple roles)
CREATE TABLE IF NOT EXISTS security_user_roles (
    user_role_id String,  -- UUID as String
    user_id String,  -- UUID as String
    role_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    granted_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    granted_by String,  -- UUID as String
    expires_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    is_active UInt8 DEFAULT 1  -- Boolean
) ENGINE = ReplacingMergeTree(granted_at)
ORDER BY (user_id, role_id, company_id, brand_id)
SETTINGS index_granularity = 8192;

-- API keys for service-to-service authentication
CREATE TABLE IF NOT EXISTS security_api_keys (
    api_key_id String,  -- UUID as String
    api_key_hash String,  -- Hashed API key
    api_key_name String,
    user_id String,  -- UUID as String
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    permissions String,  -- JSON array as String
    is_active UInt8 DEFAULT 1,  -- Boolean
    last_used_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    expires_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    rate_limit_per_minute UInt32 DEFAULT 1000,
    rate_limit_per_hour UInt32 DEFAULT 10000,
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    updated_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (api_key_id)
SETTINGS index_granularity = 8192;

-- API key usage tracking
CREATE TABLE IF NOT EXISTS security_api_key_usage (
    usage_id String,  -- UUID as String
    api_key_id String,  -- UUID as String
    endpoint String,
    method String,  -- 'GET', 'POST', 'PUT', 'DELETE'
    status_code UInt16,
    response_time_ms UInt32,
    request_size_bytes UInt32,
    response_size_bytes UInt32,
    ip_address String,
    user_agent String,
    created_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    usage_date Date
) ENGINE = MergeTree()
ORDER BY (usage_id, created_at)
PARTITION BY toYYYYMM(created_at)
TTL created_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- User sessions for authentication
CREATE TABLE IF NOT EXISTS security_user_sessions (
    session_id String,  -- UUID as String
    user_id String,  -- UUID as String
    session_token String,  -- JWT or session token
    refresh_token String,
    ip_address String,
    user_agent String,
    device_fingerprint String,
    is_active UInt8 DEFAULT 1,  -- Boolean
    expires_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    last_activity_at DateTime DEFAULT toDateTime(0),  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
    metadata String,  -- JSON as String
    created_at DateTime DEFAULT toDateTime(0)  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
) ENGINE = ReplacingMergeTree(last_activity_at)
ORDER BY (session_id)
SETTINGS index_granularity = 8192;

-- API key usage summary (aggregated by date)
CREATE TABLE IF NOT EXISTS security_api_key_usage_summary (
    usage_date Date,
    api_key_id String,  -- UUID as String
    endpoint String,
    
    total_requests UInt32,
    total_errors UInt32,
    avg_response_time_ms Float32,
    p95_response_time_ms Float32,
    p99_response_time_ms Float32,
    total_request_bytes UInt64,
    total_response_bytes UInt64,
    
    created_at DateTime DEFAULT toDateTime(0)  -- Use epoch (1970-01-01) as default for NULL (placeholder in code uses 1970-01-01)
) ENGINE = SummingMergeTree()
ORDER BY (usage_date, api_key_id, endpoint)
PARTITION BY toYYYYMM(usage_date)
TTL created_at + INTERVAL 1 YEAR;

