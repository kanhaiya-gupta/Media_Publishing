-- ============================================================================
-- OwnLens - Audit & Logging Schema
-- ============================================================================
-- Comprehensive audit logging for compliance, security, and data lineage
-- ============================================================================

-- ============================================================================
-- AUDIT LOGS
-- ============================================================================

-- Main audit log table (all actions)
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id UUID DEFAULT uuid_generate_v4(),
    
    -- Who
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    api_key_id UUID REFERENCES security_api_keys(api_key_id) ON DELETE SET NULL,
    session_id UUID REFERENCES security_user_sessions(session_id) ON DELETE SET NULL,
    
    -- What
    action VARCHAR(50) NOT NULL, -- 'READ', 'WRITE', 'DELETE', 'UPDATE', 'EXPORT', 'EXECUTE', 'LOGIN', 'LOGOUT'
    resource_type VARCHAR(100) NOT NULL, -- 'customer', 'editorial', 'company', 'ml_model', 'user', 'role', etc.
    resource_id UUID, -- ID of the resource (if applicable)
    resource_identifier VARCHAR(255), -- Human-readable identifier
    
    -- Where
    ip_address INET,
    user_agent TEXT,
    endpoint VARCHAR(500), -- API endpoint
    method VARCHAR(10), -- HTTP method
    
    -- When
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_date DATE NOT NULL,
    
    -- Result
    success BOOLEAN NOT NULL,
    status_code INTEGER, -- HTTP status code
    error_message TEXT,
    error_code VARCHAR(50),
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Request/Response details
    request_data JSONB, -- Request payload (sanitized)
    response_data JSONB, -- Response data (sanitized)
    request_size_bytes INTEGER,
    response_size_bytes INTEGER,
    response_time_ms INTEGER,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (log_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_logs_2024_01 PARTITION OF audit_logs
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS audit_logs_2024_02 PARTITION OF audit_logs
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- Add more partitions as needed

-- ============================================================================
-- DATA CHANGE LOGS
-- ============================================================================

-- Track all data changes (INSERT, UPDATE, DELETE)
CREATE TABLE IF NOT EXISTS audit_data_changes (
    change_id UUID DEFAULT uuid_generate_v4(),
    
    -- Who
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    api_key_id UUID REFERENCES security_api_keys(api_key_id) ON DELETE SET NULL,
    
    -- What
    table_name VARCHAR(255) NOT NULL,
    record_id UUID, -- Primary key of changed record
    change_type VARCHAR(20) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    
    -- Changes
    old_values JSONB, -- Previous values (for UPDATE/DELETE)
    new_values JSONB, -- New values (for INSERT/UPDATE)
    changed_fields TEXT[], -- Array of changed field names
    
    -- When
    change_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    change_date DATE NOT NULL,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Transaction info
    transaction_id VARCHAR(255), -- Database transaction ID
    batch_id UUID, -- Batch processing ID
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (change_id, change_date)
) PARTITION BY RANGE (change_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_data_changes_2024_01 PARTITION OF audit_data_changes
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- ACCESS LOGS
-- ============================================================================

-- Track data access (who accessed what data)
CREATE TABLE IF NOT EXISTS audit_data_access (
    access_id UUID DEFAULT uuid_generate_v4(),
    
    -- Who
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    api_key_id UUID REFERENCES security_api_keys(api_key_id) ON DELETE SET NULL,
    
    -- What
    resource_type VARCHAR(100) NOT NULL,
    resource_id UUID,
    resource_identifier VARCHAR(255),
    access_type VARCHAR(50) NOT NULL, -- 'QUERY', 'EXPORT', 'API_READ', 'FILE_DOWNLOAD'
    
    -- Query details
    query_text TEXT, -- SQL query or API query
    query_params JSONB, -- Query parameters
    rows_returned INTEGER,
    execution_time_ms INTEGER,
    
    -- When
    access_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    access_date DATE NOT NULL,
    
    -- Where
    ip_address INET,
    user_agent TEXT,
    endpoint VARCHAR(500),
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Compliance
    data_classification VARCHAR(50), -- 'public', 'internal', 'confidential', 'restricted'
    gdpr_relevant BOOLEAN DEFAULT FALSE, -- Is this GDPR-relevant data?
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (access_id, access_date)
) PARTITION BY RANGE (access_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_data_access_2024_01 PARTITION OF audit_data_access
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- SECURITY EVENTS
-- ============================================================================

-- Track security-related events
CREATE TABLE IF NOT EXISTS audit_security_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    
    -- Who
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    ip_address INET,
    user_agent TEXT,
    
    -- What
    event_type VARCHAR(100) NOT NULL, -- 'LOGIN_SUCCESS', 'LOGIN_FAILED', 'LOGOUT', 'PASSWORD_CHANGE', 'ROLE_CHANGE', 'PERMISSION_DENIED', 'SUSPICIOUS_ACTIVITY'
    event_severity VARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    description TEXT,
    
    -- Result
    success BOOLEAN NOT NULL,
    failure_reason TEXT,
    
    -- When
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_date DATE NOT NULL,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    session_id UUID REFERENCES security_user_sessions(session_id) ON DELETE SET NULL,
    
    -- Additional data
    event_data JSONB DEFAULT '{}'::jsonb,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_security_events_2024_01 PARTITION OF audit_security_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- DATA LINEAGE
-- ============================================================================

-- Track data lineage (where data comes from, how it's transformed)
CREATE TABLE IF NOT EXISTS audit_data_lineage (
    lineage_id UUID DEFAULT uuid_generate_v4(),
    
    -- Source
    source_type VARCHAR(100) NOT NULL, -- 'kafka', 'api', 'database', 'file', 'ml_model'
    source_identifier VARCHAR(500) NOT NULL, -- Kafka topic, table name, file path, etc.
    source_record_id UUID, -- ID of source record
    
    -- Destination
    destination_type VARCHAR(100) NOT NULL, -- 'database', 'ml_model', 'api', 'file'
    destination_identifier VARCHAR(500) NOT NULL,
    destination_record_id UUID, -- ID of destination record
    
    -- Transformation
    transformation_type VARCHAR(100), -- 'aggregation', 'filter', 'join', 'ml_prediction', 'enrichment'
    transformation_details JSONB, -- Details of transformation
    
    -- When
    transformation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transformation_date DATE NOT NULL,
    
    -- Context
    pipeline_id VARCHAR(255), -- Airflow DAG ID, Spark job ID, etc.
    batch_id UUID,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (lineage_id, transformation_date)
) PARTITION BY RANGE (transformation_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_data_lineage_2024_01 PARTITION OF audit_data_lineage
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- COMPLIANCE LOGS
-- ============================================================================

-- Track compliance-related events (GDPR, data retention, etc.)
CREATE TABLE IF NOT EXISTS audit_compliance_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    
    -- What
    compliance_type VARCHAR(100) NOT NULL, -- 'GDPR_ACCESS_REQUEST', 'GDPR_DELETION_REQUEST', 'DATA_RETENTION', 'DATA_EXPORT', 'CONSENT_UPDATE'
    event_type VARCHAR(100) NOT NULL, -- 'REQUEST', 'PROCESSED', 'COMPLETED', 'FAILED'
    description TEXT,
    
    -- Who
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    requested_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    processed_by UUID REFERENCES users(user_id) ON DELETE SET NULL,
    
    -- What data
    resource_type VARCHAR(100),
    resource_ids UUID[], -- Array of affected resource IDs
    data_scope JSONB, -- Scope of data affected
    
    -- Status
    status VARCHAR(50) NOT NULL, -- 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'
    status_message TEXT,
    
    -- When
    requested_at TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_date DATE NOT NULL,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Results
    records_affected INTEGER,
    data_export_url VARCHAR(1000), -- URL to exported data (if applicable)
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS audit_compliance_events_2024_01 PARTITION OF audit_compliance_events
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Audit logs indexes
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_logs_date ON audit_logs(event_date);
CREATE INDEX IF NOT EXISTS idx_audit_logs_company ON audit_logs(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_logs_brand ON audit_logs(brand_id) WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_logs_success ON audit_logs(success) WHERE success = FALSE;

-- Data changes indexes
CREATE INDEX IF NOT EXISTS idx_data_changes_table ON audit_data_changes(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_data_changes_type ON audit_data_changes(change_type);
CREATE INDEX IF NOT EXISTS idx_data_changes_timestamp ON audit_data_changes(change_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_changes_date ON audit_data_changes(change_date);
CREATE INDEX IF NOT EXISTS idx_data_changes_user ON audit_data_changes(user_id) WHERE user_id IS NOT NULL;

-- Data access indexes
CREATE INDEX IF NOT EXISTS idx_data_access_user_id ON audit_data_access(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_data_access_resource ON audit_data_access(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_data_access_timestamp ON audit_data_access(access_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_access_date ON audit_data_access(access_date);
CREATE INDEX IF NOT EXISTS idx_data_access_gdpr ON audit_data_access(gdpr_relevant) WHERE gdpr_relevant = TRUE;

-- Security events indexes
CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON audit_security_events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_security_events_type ON audit_security_events(event_type);
CREATE INDEX IF NOT EXISTS idx_security_events_severity ON audit_security_events(event_severity);
CREATE INDEX IF NOT EXISTS idx_security_events_timestamp ON audit_security_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_security_events_date ON audit_security_events(event_date);
CREATE INDEX IF NOT EXISTS idx_security_events_success ON audit_security_events(success) WHERE success = FALSE;

-- Data lineage indexes
CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON audit_data_lineage(source_type, source_identifier);
CREATE INDEX IF NOT EXISTS idx_data_lineage_destination ON audit_data_lineage(destination_type, destination_identifier);
CREATE INDEX IF NOT EXISTS idx_data_lineage_timestamp ON audit_data_lineage(transformation_timestamp);
CREATE INDEX IF NOT EXISTS idx_data_lineage_date ON audit_data_lineage(transformation_date);
CREATE INDEX IF NOT EXISTS idx_data_lineage_pipeline ON audit_data_lineage(pipeline_id) WHERE pipeline_id IS NOT NULL;

-- Compliance events indexes
CREATE INDEX IF NOT EXISTS idx_compliance_events_type ON audit_compliance_events(compliance_type);
CREATE INDEX IF NOT EXISTS idx_compliance_events_status ON audit_compliance_events(status);
CREATE INDEX IF NOT EXISTS idx_compliance_events_user_id ON audit_compliance_events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_compliance_events_timestamp ON audit_compliance_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_compliance_events_date ON audit_compliance_events(event_date);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Triggers to populate date columns from timestamp columns
CREATE OR REPLACE FUNCTION set_audit_date_from_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'audit_logs' THEN
        NEW.event_date = DATE(NEW.event_timestamp);
    ELSIF TG_TABLE_NAME = 'audit_data_changes' THEN
        NEW.change_date = DATE(NEW.change_timestamp);
    ELSIF TG_TABLE_NAME = 'audit_data_access' THEN
        NEW.access_date = DATE(NEW.access_timestamp);
    ELSIF TG_TABLE_NAME = 'audit_security_events' THEN
        NEW.event_date = DATE(NEW.event_timestamp);
    ELSIF TG_TABLE_NAME = 'audit_data_lineage' THEN
        NEW.transformation_date = DATE(NEW.transformation_timestamp);
    ELSIF TG_TABLE_NAME = 'audit_compliance_events' THEN
        NEW.event_date = DATE(NEW.event_timestamp);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_audit_logs_event_date 
    BEFORE INSERT OR UPDATE ON audit_logs
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER set_audit_data_changes_date 
    BEFORE INSERT OR UPDATE ON audit_data_changes
    FOR EACH ROW 
    WHEN (NEW.change_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER set_audit_data_access_date 
    BEFORE INSERT OR UPDATE ON audit_data_access
    FOR EACH ROW 
    WHEN (NEW.access_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER set_audit_security_events_date 
    BEFORE INSERT OR UPDATE ON audit_security_events
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER set_audit_data_lineage_date 
    BEFORE INSERT OR UPDATE ON audit_data_lineage
    FOR EACH ROW 
    WHEN (NEW.transformation_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER set_audit_compliance_events_date 
    BEFORE INSERT OR UPDATE ON audit_compliance_events
    FOR EACH ROW 
    WHEN (NEW.event_date IS NULL)
    EXECUTE FUNCTION set_audit_date_from_timestamp();

CREATE TRIGGER update_compliance_events_updated_at BEFORE UPDATE ON audit_compliance_events
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE audit_logs IS 'Comprehensive audit log for all system actions (partitioned by date)';
COMMENT ON TABLE audit_data_changes IS 'Track all data changes (INSERT, UPDATE, DELETE) (partitioned by date)';
COMMENT ON TABLE audit_data_access IS 'Track data access for compliance (partitioned by date)';
COMMENT ON TABLE audit_security_events IS 'Track security-related events (partitioned by date)';
COMMENT ON TABLE audit_data_lineage IS 'Track data lineage and transformations (partitioned by date)';
COMMENT ON TABLE audit_compliance_events IS 'Track compliance events (GDPR, data retention, etc.) (partitioned by date)';

