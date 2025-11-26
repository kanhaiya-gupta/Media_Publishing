-- ============================================================================
-- OwnLens - Security & Access Control Schema
-- ============================================================================
-- Role-Based Access Control (RBAC), permissions, and security
-- ============================================================================

-- ============================================================================
-- ROLES & PERMISSIONS
-- ============================================================================

-- Roles table (RBAC)
CREATE TABLE IF NOT EXISTS security_roles (
    role_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_name VARCHAR(100) NOT NULL UNIQUE,
    role_code VARCHAR(50) NOT NULL UNIQUE, -- 'admin', 'data_engineer', 'data_scientist', 'analyst', 'editor', 'auditor'
    description TEXT,
    is_system_role BOOLEAN DEFAULT FALSE, -- System roles cannot be deleted
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Permissions table
CREATE TABLE IF NOT EXISTS security_permissions (
    permission_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    permission_name VARCHAR(255) NOT NULL UNIQUE,
    permission_code VARCHAR(100) NOT NULL UNIQUE, -- 'read:customer:data', 'write:editorial:content', etc.
    resource_type VARCHAR(100) NOT NULL, -- 'customer', 'editorial', 'company', 'ml_models', 'audit'
    action VARCHAR(50) NOT NULL, -- 'read', 'write', 'delete', 'execute', 'admin'
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Role-Permission mapping
CREATE TABLE IF NOT EXISTS security_role_permissions (
    role_permission_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_id UUID NOT NULL REFERENCES security_roles(role_id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES security_permissions(permission_id) ON DELETE CASCADE,
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    granted_by UUID REFERENCES users(user_id),
    CONSTRAINT unique_role_permission UNIQUE (role_id, permission_id)
);

-- User-Role mapping (can have multiple roles)
CREATE TABLE IF NOT EXISTS security_user_roles (
    user_role_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES security_roles(role_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    granted_by UUID REFERENCES users(user_id),
    expires_at TIMESTAMP WITH TIME ZONE, -- Optional expiration
    is_active BOOLEAN DEFAULT TRUE,
    CONSTRAINT unique_user_role_scope UNIQUE (user_id, role_id, company_id, brand_id)
);

-- ============================================================================
-- API KEYS & AUTHENTICATION
-- ============================================================================

-- API keys for service-to-service authentication
CREATE TABLE IF NOT EXISTS security_api_keys (
    api_key_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_key_hash VARCHAR(255) NOT NULL UNIQUE, -- Hashed API key
    api_key_name VARCHAR(255) NOT NULL,
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Permissions
    permissions JSONB DEFAULT '[]'::jsonb, -- Array of permission codes
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_used_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Rate limiting
    rate_limit_per_minute INTEGER DEFAULT 1000,
    rate_limit_per_hour INTEGER DEFAULT 10000,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id)
);

-- API key usage tracking
CREATE TABLE IF NOT EXISTS security_api_key_usage (
    usage_id UUID DEFAULT uuid_generate_v4(),
    api_key_id UUID NOT NULL REFERENCES security_api_keys(api_key_id) ON DELETE CASCADE,
    endpoint VARCHAR(500),
    method VARCHAR(10), -- 'GET', 'POST', 'PUT', 'DELETE'
    status_code INTEGER,
    response_time_ms INTEGER,
    request_size_bytes INTEGER,
    response_size_bytes INTEGER,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usage_date DATE NOT NULL,
    PRIMARY KEY (usage_id, usage_date)
) PARTITION BY RANGE (usage_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS security_api_key_usage_2024_01 PARTITION OF security_api_key_usage
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- SESSION MANAGEMENT
-- ============================================================================

-- User sessions for authentication
CREATE TABLE IF NOT EXISTS security_user_sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    session_token VARCHAR(500) NOT NULL UNIQUE, -- JWT or session token
    refresh_token VARCHAR(500),
    
    -- Session metadata
    ip_address INET,
    user_agent TEXT,
    device_fingerprint VARCHAR(255),
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Roles indexes
CREATE INDEX IF NOT EXISTS idx_security_roles_code ON security_roles(role_code);
CREATE INDEX IF NOT EXISTS idx_security_roles_active ON security_roles(is_active);

-- Permissions indexes
CREATE INDEX IF NOT EXISTS idx_security_permissions_code ON security_permissions(permission_code);
CREATE INDEX IF NOT EXISTS idx_security_permissions_resource ON security_permissions(resource_type, action);
CREATE INDEX IF NOT EXISTS idx_security_permissions_active ON security_permissions(is_active);

-- Role permissions indexes
CREATE INDEX IF NOT EXISTS idx_role_permissions_role_id ON security_role_permissions(role_id);
CREATE INDEX IF NOT EXISTS idx_role_permissions_permission_id ON security_role_permissions(permission_id);

-- User roles indexes
CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON security_user_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON security_user_roles(role_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_company_id ON security_user_roles(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_roles_brand_id ON security_user_roles(brand_id) WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_roles_active ON security_user_roles(user_id, is_active) WHERE is_active = TRUE;

-- API keys indexes
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON security_api_keys(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_company_id ON security_api_keys(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_brand_id ON security_api_keys(brand_id) WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON security_api_keys(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON security_api_keys(api_key_hash);

-- API key usage indexes
CREATE INDEX IF NOT EXISTS idx_api_key_usage_api_key_id ON security_api_key_usage(api_key_id);
CREATE INDEX IF NOT EXISTS idx_api_key_usage_date ON security_api_key_usage(usage_date);
CREATE INDEX IF NOT EXISTS idx_api_key_usage_endpoint ON security_api_key_usage(endpoint);

-- User sessions indexes
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON security_user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON security_user_sessions(session_token);
CREATE INDEX IF NOT EXISTS idx_user_sessions_active ON security_user_sessions(user_id, is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires ON security_user_sessions(expires_at) WHERE is_active = TRUE;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_security_roles_updated_at BEFORE UPDATE ON security_roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_api_keys_updated_at BEFORE UPDATE ON security_api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Trigger to populate usage_date from created_at
CREATE OR REPLACE FUNCTION set_usage_date_from_created_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.usage_date = DATE(NEW.created_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_security_api_key_usage_date 
    BEFORE INSERT OR UPDATE ON security_api_key_usage
    FOR EACH ROW 
    WHEN (NEW.usage_date IS NULL)
    EXECUTE FUNCTION set_usage_date_from_created_at();

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert default roles
INSERT INTO security_roles (role_name, role_code, description, is_system_role) VALUES
    ('Administrator', 'admin', 'Full system access', TRUE),
    ('Data Engineer', 'data_engineer', 'Data pipeline and infrastructure access', TRUE),
    ('Data Scientist', 'data_scientist', 'ML models and analytics access', TRUE),
    ('Analyst', 'analyst', 'Read-only analytics access', TRUE),
    ('Editor', 'editor', 'Editorial content and analytics access', TRUE),
    ('Senior Editor', 'senior_editor', 'Editorial content, analytics, and strategy access', TRUE),
    ('Auditor', 'auditor', 'Audit logs and compliance access', TRUE),
    ('Company Admin', 'company_admin', 'Company domain admin access', TRUE)
ON CONFLICT (role_code) DO NOTHING;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE security_roles IS 'RBAC roles for access control';
COMMENT ON TABLE security_permissions IS 'Permissions for resources and actions';
COMMENT ON TABLE security_role_permissions IS 'Role-permission mappings';
COMMENT ON TABLE security_user_roles IS 'User-role assignments (can be scoped to company/brand)';
COMMENT ON TABLE security_api_keys IS 'API keys for service-to-service authentication';
COMMENT ON TABLE security_api_key_usage IS 'API key usage tracking (partitioned by date)';
COMMENT ON TABLE security_user_sessions IS 'User authentication sessions';

