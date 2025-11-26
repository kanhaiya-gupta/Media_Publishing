-- ============================================================================
-- OwnLens - Compliance & Data Privacy Schema
-- ============================================================================
-- GDPR compliance, data privacy, consent management, data retention
-- ============================================================================

-- ============================================================================
-- USER CONSENT
-- ============================================================================

-- User consent tracking (GDPR compliance)
CREATE TABLE IF NOT EXISTS compliance_user_consent (
    consent_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Consent details
    consent_type VARCHAR(100) NOT NULL, -- 'analytics', 'marketing', 'personalization', 'cookies', 'data_sharing'
    consent_status VARCHAR(50) NOT NULL, -- 'granted', 'denied', 'withdrawn', 'expired'
    consent_method VARCHAR(50), -- 'explicit', 'implicit', 'opt_in', 'opt_out'
    
    -- Consent metadata
    consent_text TEXT, -- Text shown to user
    consent_version VARCHAR(50), -- Version of consent text
    ip_address INET,
    user_agent TEXT,
    
    -- Timestamps
    granted_at TIMESTAMP WITH TIME ZONE,
    withdrawn_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    
    CONSTRAINT unique_user_consent UNIQUE (user_id, company_id, brand_id, consent_type)
);

-- ============================================================================
-- DATA SUBJECT REQUESTS (GDPR)
-- ============================================================================

-- Data subject access/deletion requests (GDPR Article 15, 17)
CREATE TABLE IF NOT EXISTS compliance_data_subject_requests (
    request_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    external_user_id VARCHAR(255), -- User ID from external system
    
    -- Request details
    request_type VARCHAR(50) NOT NULL, -- 'ACCESS', 'DELETION', 'RECTIFICATION', 'PORTABILITY', 'RESTRICTION'
    request_status VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'VERIFIED', 'PROCESSING', 'COMPLETED', 'REJECTED', 'CANCELLED'
    request_priority VARCHAR(20) DEFAULT 'NORMAL', -- 'LOW', 'NORMAL', 'HIGH', 'URGENT'
    
    -- Requestor information
    requestor_email VARCHAR(255),
    requestor_name VARCHAR(255),
    requestor_phone VARCHAR(50),
    verification_method VARCHAR(50), -- 'email', 'phone', 'id_document'
    verification_status VARCHAR(50) DEFAULT 'PENDING', -- 'PENDING', 'VERIFIED', 'FAILED'
    verification_code VARCHAR(100),
    verification_expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Request details
    request_description TEXT,
    data_scope JSONB, -- Scope of data requested (all, specific tables, etc.)
    
    -- Processing
    requested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    verified_at TIMESTAMP WITH TIME ZONE,
    processing_started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    due_date TIMESTAMP WITH TIME ZONE, -- GDPR requires response within 30 days
    
    -- Results
    records_found INTEGER,
    records_processed INTEGER,
    records_deleted INTEGER, -- For deletion requests
    data_export_url VARCHAR(1000), -- URL to exported data (for access requests)
    data_export_expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Rejection details (if rejected)
    rejection_reason TEXT,
    rejected_at TIMESTAMP WITH TIME ZONE,
    rejected_by UUID REFERENCES users(user_id),
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DATA RETENTION POLICIES
-- ============================================================================

-- Data retention policies
CREATE TABLE IF NOT EXISTS compliance_retention_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Policy details
    policy_name VARCHAR(255) NOT NULL,
    policy_type VARCHAR(100) NOT NULL, -- 'customer', 'editorial', 'company', 'audit', 'ml_models'
    table_name VARCHAR(255), -- Specific table (NULL for all tables of type)
    
    -- Retention rules
    retention_period_days INTEGER NOT NULL, -- Days to retain data
    retention_period_months INTEGER, -- Alternative: months
    retention_period_years INTEGER, -- Alternative: years
    
    -- Archival rules
    archive_before_delete BOOLEAN DEFAULT FALSE,
    archive_location VARCHAR(500), -- Archive storage location
    
    -- Deletion rules
    delete_after_retention BOOLEAN DEFAULT TRUE,
    deletion_method VARCHAR(50), -- 'SOFT_DELETE', 'HARD_DELETE', 'ANONYMIZE'
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_executed_at TIMESTAMP WITH TIME ZONE,
    next_execution_at TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    description TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id)
);

-- Data retention execution log
CREATE TABLE IF NOT EXISTS compliance_retention_executions (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    policy_id UUID NOT NULL REFERENCES compliance_retention_policies(policy_id) ON DELETE CASCADE,
    
    -- Execution details
    execution_status VARCHAR(50) NOT NULL, -- 'SUCCESS', 'FAILED', 'PARTIAL'
    execution_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_completed_at TIMESTAMP WITH TIME ZONE,
    execution_duration_sec INTEGER,
    
    -- Results
    records_processed INTEGER DEFAULT 0,
    records_archived INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0,
    records_anonymized INTEGER DEFAULT 0,
    
    -- Errors
    error_message TEXT,
    error_count INTEGER DEFAULT 0,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DATA ANONYMIZATION
-- ============================================================================

-- Track anonymized data
CREATE TABLE IF NOT EXISTS compliance_anonymized_data (
    anonymization_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Original data
    original_table_name VARCHAR(255) NOT NULL,
    original_record_id UUID NOT NULL,
    original_user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    
    -- Anonymization details
    anonymization_method VARCHAR(100), -- 'HASH', 'MASK', 'RANDOMIZE', 'DELETE'
    anonymization_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    anonymized_by UUID REFERENCES users(user_id),
    
    -- Reason
    anonymization_reason VARCHAR(100), -- 'GDPR_DELETION', 'RETENTION_POLICY', 'USER_REQUEST'
    related_request_id UUID REFERENCES compliance_data_subject_requests(request_id) ON DELETE SET NULL,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PRIVACY IMPACT ASSESSMENTS
-- ============================================================================

-- Privacy Impact Assessments (PIA/DPIA)
CREATE TABLE IF NOT EXISTS compliance_privacy_assessments (
    assessment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Assessment details
    assessment_name VARCHAR(255) NOT NULL,
    assessment_type VARCHAR(50) NOT NULL, -- 'PIA', 'DPIA', 'RISK_ASSESSMENT'
    description TEXT,
    
    -- Scope
    data_types_processed TEXT[], -- Types of data processed
    processing_purposes TEXT[], -- Purposes of processing
    data_subjects_affected INTEGER, -- Number of data subjects
    
    -- Risk assessment
    risk_level VARCHAR(20), -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    risk_factors JSONB, -- Risk factors identified
    mitigation_measures JSONB, -- Mitigation measures
    
    -- Status
    assessment_status VARCHAR(50) DEFAULT 'DRAFT', -- 'DRAFT', 'REVIEW', 'APPROVED', 'REJECTED'
    assessed_by UUID REFERENCES users(user_id),
    reviewed_by UUID REFERENCES users(user_id),
    approved_by UUID REFERENCES users(user_id),
    
    -- Timestamps
    assessment_date DATE,
    review_date DATE,
    approval_date DATE,
    next_review_date DATE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- DATA BREACH INCIDENTS
-- ============================================================================

-- Data breach incident tracking
CREATE TABLE IF NOT EXISTS compliance_breach_incidents (
    incident_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Incident details
    incident_type VARCHAR(100) NOT NULL, -- 'UNAUTHORIZED_ACCESS', 'DATA_LEAK', 'LOSS', 'THEFT', 'HACKING'
    incident_severity VARCHAR(20) NOT NULL, -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    description TEXT NOT NULL,
    
    -- Affected data
    data_types_affected TEXT[], -- Types of data affected
    records_affected INTEGER,
    users_affected INTEGER,
    personal_data_affected BOOLEAN DEFAULT FALSE,
    
    -- Incident timeline
    discovered_at TIMESTAMP WITH TIME ZONE NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE, -- When incident occurred
    contained_at TIMESTAMP WITH TIME ZONE, -- When incident was contained
    resolved_at TIMESTAMP WITH TIME ZONE, -- When incident was resolved
    
    -- Response
    incident_status VARCHAR(50) DEFAULT 'DISCOVERED', -- 'DISCOVERED', 'INVESTIGATING', 'CONTAINED', 'RESOLVED', 'CLOSED'
    response_actions JSONB, -- Actions taken
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_date TIMESTAMP WITH TIME ZONE,
    regulatory_notification BOOLEAN DEFAULT FALSE, -- GDPR Article 33 (72 hours)
    regulatory_notification_date TIMESTAMP WITH TIME ZONE,
    
    -- Responsible parties
    discovered_by UUID REFERENCES users(user_id),
    investigated_by UUID REFERENCES users(user_id),
    resolved_by UUID REFERENCES users(user_id),
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- User consent indexes
CREATE INDEX IF NOT EXISTS idx_user_consent_user_id ON compliance_user_consent(user_id);
CREATE INDEX IF NOT EXISTS idx_user_consent_company_brand ON compliance_user_consent(company_id, brand_id);
CREATE INDEX IF NOT EXISTS idx_user_consent_type ON compliance_user_consent(consent_type);
CREATE INDEX IF NOT EXISTS idx_user_consent_status ON compliance_user_consent(consent_status);
CREATE INDEX IF NOT EXISTS idx_user_consent_expires ON compliance_user_consent(expires_at) WHERE expires_at IS NOT NULL;

-- Data subject requests indexes
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_user_id ON compliance_data_subject_requests(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_type ON compliance_data_subject_requests(request_type);
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_status ON compliance_data_subject_requests(request_status);
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_due_date ON compliance_data_subject_requests(due_date) WHERE due_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_company ON compliance_data_subject_requests(company_id);
CREATE INDEX IF NOT EXISTS idx_data_subject_requests_pending ON compliance_data_subject_requests(request_status, due_date) WHERE request_status IN ('PENDING', 'PROCESSING');

-- Retention policies indexes
CREATE INDEX IF NOT EXISTS idx_retention_policies_company ON compliance_retention_policies(company_id);
CREATE INDEX IF NOT EXISTS idx_retention_policies_brand ON compliance_retention_policies(brand_id);
CREATE INDEX IF NOT EXISTS idx_retention_policies_type ON compliance_retention_policies(policy_type);
CREATE INDEX IF NOT EXISTS idx_retention_policies_active ON compliance_retention_policies(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_retention_policies_next_execution ON compliance_retention_policies(next_execution_at) WHERE is_active = TRUE;

-- Retention executions indexes
CREATE INDEX IF NOT EXISTS idx_retention_executions_policy_id ON compliance_retention_executions(policy_id);
CREATE INDEX IF NOT EXISTS idx_retention_executions_status ON compliance_retention_executions(execution_status);
CREATE INDEX IF NOT EXISTS idx_retention_executions_started ON compliance_retention_executions(execution_started_at);

-- Anonymized data indexes
CREATE INDEX IF NOT EXISTS idx_anonymized_data_table_record ON compliance_anonymized_data(original_table_name, original_record_id);
CREATE INDEX IF NOT EXISTS idx_anonymized_data_user_id ON compliance_anonymized_data(original_user_id) WHERE original_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_anonymized_data_date ON compliance_anonymized_data(anonymization_date);
CREATE INDEX IF NOT EXISTS idx_anonymized_data_request_id ON compliance_anonymized_data(related_request_id) WHERE related_request_id IS NOT NULL;

-- Privacy assessments indexes
CREATE INDEX IF NOT EXISTS idx_privacy_assessments_company ON compliance_privacy_assessments(company_id);
CREATE INDEX IF NOT EXISTS idx_privacy_assessments_brand ON compliance_privacy_assessments(brand_id);
CREATE INDEX IF NOT EXISTS idx_privacy_assessments_status ON compliance_privacy_assessments(assessment_status);
CREATE INDEX IF NOT EXISTS idx_privacy_assessments_risk ON compliance_privacy_assessments(risk_level);

-- Breach incidents indexes
CREATE INDEX IF NOT EXISTS idx_breach_incidents_company ON compliance_breach_incidents(company_id);
CREATE INDEX IF NOT EXISTS idx_breach_incidents_brand ON compliance_breach_incidents(brand_id);
CREATE INDEX IF NOT EXISTS idx_breach_incidents_severity ON compliance_breach_incidents(incident_severity);
CREATE INDEX IF NOT EXISTS idx_breach_incidents_status ON compliance_breach_incidents(incident_status);
CREATE INDEX IF NOT EXISTS idx_breach_incidents_discovered ON compliance_breach_incidents(discovered_at);
CREATE INDEX IF NOT EXISTS idx_breach_incidents_critical ON compliance_breach_incidents(incident_severity, incident_status) WHERE incident_severity IN ('HIGH', 'CRITICAL');

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_user_consent_updated_at BEFORE UPDATE ON compliance_user_consent
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_subject_requests_updated_at BEFORE UPDATE ON compliance_data_subject_requests
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_retention_policies_updated_at BEFORE UPDATE ON compliance_retention_policies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_breach_incidents_updated_at BEFORE UPDATE ON compliance_breach_incidents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_privacy_assessments_updated_at BEFORE UPDATE ON compliance_privacy_assessments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE compliance_user_consent IS 'User consent tracking for GDPR compliance';
COMMENT ON TABLE compliance_data_subject_requests IS 'GDPR data subject requests (access, deletion, etc.)';
COMMENT ON TABLE compliance_retention_policies IS 'Data retention policies and rules';
COMMENT ON TABLE compliance_retention_executions IS 'Data retention policy execution log';
COMMENT ON TABLE compliance_anonymized_data IS 'Track anonymized data for compliance';
COMMENT ON TABLE compliance_privacy_assessments IS 'Privacy Impact Assessments (PIA/DPIA)';
COMMENT ON TABLE compliance_breach_incidents IS 'Data breach incident tracking and response';

