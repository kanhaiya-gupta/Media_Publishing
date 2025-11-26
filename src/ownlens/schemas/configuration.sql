-- ============================================================================
-- OwnLens - Configuration Management Schema
-- ============================================================================
-- Feature flags, system configuration, settings management
-- ============================================================================

-- ============================================================================
-- FEATURE FLAGS
-- ============================================================================

-- Feature flags for feature toggling
CREATE TABLE IF NOT EXISTS configuration_feature_flags (
    flag_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Flag identification
    flag_name VARCHAR(255) NOT NULL UNIQUE,
    flag_code VARCHAR(100) NOT NULL UNIQUE, -- 'enable_churn_prediction', 'enable_editorial_analytics', etc.
    flag_type VARCHAR(50) NOT NULL, -- 'BOOLEAN', 'PERCENTAGE', 'USER_LIST', 'CUSTOM'
    
    -- Flag status
    is_enabled BOOLEAN DEFAULT FALSE,
    enabled_percentage DECIMAL(5, 4) DEFAULT 0.0, -- For percentage-based flags (0-1)
    enabled_user_ids UUID[], -- Array of user IDs (for user list flags)
    enabled_company_ids UUID[], -- Array of company IDs
    enabled_brand_ids UUID[], -- Array of brand IDs
    
    -- Flag configuration
    flag_value JSONB DEFAULT '{}'::jsonb, -- Custom flag value (for custom type)
    default_value JSONB DEFAULT '{}'::jsonb, -- Default value
    
    -- Flag metadata
    description TEXT,
    category VARCHAR(100), -- 'customer', 'editorial', 'company', 'ml', 'infrastructure'
    tags VARCHAR(100)[],
    
    -- Rollout
    rollout_start_date DATE,
    rollout_end_date DATE,
    rollout_percentage DECIMAL(5, 4), -- Gradual rollout percentage
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_experimental BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id)
);

-- Feature flag history (track changes)
CREATE TABLE IF NOT EXISTS configuration_feature_flag_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flag_id UUID NOT NULL REFERENCES configuration_feature_flags(flag_id) ON DELETE CASCADE,
    
    -- Change details
    change_type VARCHAR(50) NOT NULL, -- 'CREATED', 'ENABLED', 'DISABLED', 'UPDATED', 'DELETED'
    old_value JSONB,
    new_value JSONB,
    
    -- Who and when
    changed_by UUID REFERENCES users(user_id),
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    change_reason TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- ============================================================================
-- SYSTEM CONFIGURATION
-- ============================================================================

-- System configuration settings
CREATE TABLE IF NOT EXISTS configuration_system_settings (
    setting_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Setting identification
    setting_name VARCHAR(255) NOT NULL UNIQUE,
    setting_code VARCHAR(100) NOT NULL UNIQUE, -- 'kafka_bootstrap_servers', 'clickhouse_host', etc.
    setting_category VARCHAR(100) NOT NULL, -- 'database', 'kafka', 'spark', 'ml', 'api', 'monitoring'
    
    -- Setting value
    setting_value TEXT NOT NULL,
    setting_type VARCHAR(50) NOT NULL, -- 'STRING', 'INTEGER', 'DECIMAL', 'BOOLEAN', 'JSON'
    default_value TEXT,
    
    -- Setting metadata
    description TEXT,
    is_encrypted BOOLEAN DEFAULT FALSE, -- Whether value is encrypted
    is_secret BOOLEAN DEFAULT FALSE, -- Whether value is sensitive
    
    -- Scope
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    environment VARCHAR(50), -- 'development', 'staging', 'production'
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id),
    updated_by UUID REFERENCES users(user_id),
    
    CONSTRAINT unique_setting_scope UNIQUE (setting_code, company_id, brand_id, environment)
);

-- System configuration history
CREATE TABLE IF NOT EXISTS configuration_system_settings_history (
    history_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    setting_id UUID NOT NULL REFERENCES configuration_system_settings(setting_id) ON DELETE CASCADE,
    
    -- Change details
    change_type VARCHAR(50) NOT NULL, -- 'CREATED', 'UPDATED', 'DELETED'
    old_value TEXT,
    new_value TEXT,
    
    -- Who and when
    changed_by UUID REFERENCES users(user_id),
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    change_reason TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Feature flags indexes
CREATE INDEX IF NOT EXISTS idx_feature_flags_code ON configuration_feature_flags(flag_code);
CREATE INDEX IF NOT EXISTS idx_feature_flags_enabled ON configuration_feature_flags(is_enabled) WHERE is_enabled = TRUE;
CREATE INDEX IF NOT EXISTS idx_feature_flags_active ON configuration_feature_flags(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_feature_flags_category ON configuration_feature_flags(category);

-- Feature flag history indexes
CREATE INDEX IF NOT EXISTS idx_feature_flag_history_flag_id ON configuration_feature_flag_history(flag_id);
CREATE INDEX IF NOT EXISTS idx_feature_flag_history_changed_at ON configuration_feature_flag_history(changed_at);

-- System settings indexes
CREATE INDEX IF NOT EXISTS idx_system_settings_code ON configuration_system_settings(setting_code);
CREATE INDEX IF NOT EXISTS idx_system_settings_category ON configuration_system_settings(setting_category);
CREATE INDEX IF NOT EXISTS idx_system_settings_company ON configuration_system_settings(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_system_settings_brand ON configuration_system_settings(brand_id) WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_system_settings_environment ON configuration_system_settings(environment);
CREATE INDEX IF NOT EXISTS idx_system_settings_active ON configuration_system_settings(is_active) WHERE is_active = TRUE;

-- System settings history indexes
CREATE INDEX IF NOT EXISTS idx_system_settings_history_setting_id ON configuration_system_settings_history(setting_id);
CREATE INDEX IF NOT EXISTS idx_system_settings_history_changed_at ON configuration_system_settings_history(changed_at);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_feature_flags_updated_at BEFORE UPDATE ON configuration_feature_flags
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_system_settings_updated_at BEFORE UPDATE ON configuration_system_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE configuration_feature_flags IS 'Feature flags for feature toggling';
COMMENT ON TABLE configuration_feature_flag_history IS 'Feature flag change history';
COMMENT ON TABLE configuration_system_settings IS 'System configuration settings';
COMMENT ON TABLE configuration_system_settings_history IS 'System configuration change history';

