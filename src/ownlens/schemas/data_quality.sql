-- ============================================================================
-- OwnLens - Data Quality Schema
-- ============================================================================
-- Data quality checks, validation rules, data quality metrics
-- ============================================================================

-- ============================================================================
-- DATA QUALITY RULES
-- ============================================================================

-- Data quality validation rules
CREATE TABLE IF NOT EXISTS data_quality_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Rule identification
    rule_name VARCHAR(255) NOT NULL,
    rule_code VARCHAR(100) NOT NULL UNIQUE,
    rule_type VARCHAR(100) NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'validity', 'timeliness', 'uniqueness'
    
    -- Rule scope
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255), -- NULL for table-level rules
    domain VARCHAR(50), -- 'customer', 'editorial', 'company'
    
    -- Rule definition
    rule_expression TEXT NOT NULL, -- SQL expression or validation logic
    rule_threshold DECIMAL(10, 6), -- Threshold for rule (e.g., 0.95 for 95% completeness)
    rule_severity VARCHAR(20) DEFAULT 'WARNING', -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    
    -- Rule status
    is_active BOOLEAN DEFAULT TRUE,
    is_enforced BOOLEAN DEFAULT FALSE, -- Whether to block data if rule fails
    
    -- Rule metadata
    description TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id)
);

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Data quality check executions
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id UUID DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES data_quality_rules(rule_id) ON DELETE CASCADE,
    
    -- Check execution
    check_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    check_date DATE NOT NULL,
    check_status VARCHAR(50) NOT NULL, -- 'PASSED', 'FAILED', 'WARNING', 'ERROR'
    
    -- Check results
    records_checked INTEGER DEFAULT 0,
    records_passed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    pass_rate DECIMAL(5, 4), -- Pass rate (0-1)
    quality_score DECIMAL(5, 4), -- Quality score (0-1)
    
    -- Check details
    check_result JSONB DEFAULT '{}'::jsonb, -- Detailed check results
    failed_records JSONB DEFAULT '[]'::jsonb, -- Sample of failed records
    error_message TEXT,
    
    -- Execution context
    execution_job_id VARCHAR(255), -- Airflow DAG run ID, etc.
    execution_duration_sec INTEGER,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (check_id, check_date)
) PARTITION BY RANGE (check_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS data_quality_checks_2024_01 PARTITION OF data_quality_checks
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- DATA QUALITY METRICS
-- ============================================================================

-- Data quality metrics (aggregated by date)
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Metric scope
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255), -- NULL for table-level metrics
    domain VARCHAR(50), -- 'customer', 'editorial', 'company'
    metric_date DATE NOT NULL,
    
    -- Completeness metrics
    total_records INTEGER DEFAULT 0,
    null_count INTEGER DEFAULT 0,
    null_percentage DECIMAL(5, 4), -- Percentage of null values
    completeness_score DECIMAL(5, 4), -- Completeness score (0-1)
    
    -- Accuracy metrics
    accuracy_score DECIMAL(5, 4), -- Accuracy score (0-1)
    invalid_count INTEGER DEFAULT 0,
    invalid_percentage DECIMAL(5, 4), -- Percentage of invalid values
    
    -- Consistency metrics
    consistency_score DECIMAL(5, 4), -- Consistency score (0-1)
    duplicate_count INTEGER DEFAULT 0,
    duplicate_percentage DECIMAL(5, 4), -- Percentage of duplicates
    
    -- Validity metrics
    validity_score DECIMAL(5, 4), -- Validity score (0-1)
    out_of_range_count INTEGER DEFAULT 0,
    format_errors INTEGER DEFAULT 0,
    
    -- Timeliness metrics
    timeliness_score DECIMAL(5, 4), -- Timeliness score (0-1)
    stale_records INTEGER DEFAULT 0, -- Records older than expected
    avg_data_age_hours DECIMAL(10, 2), -- Average age of data in hours
    
    -- Uniqueness metrics
    uniqueness_score DECIMAL(5, 4), -- Uniqueness score (0-1)
    unique_count INTEGER DEFAULT 0,
    
    -- Overall quality
    overall_quality_score DECIMAL(5, 4), -- Overall quality score (0-1)
    quality_grade VARCHAR(10), -- 'A', 'B', 'C', 'D', 'F'
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_table_column_date UNIQUE (table_name, column_name, metric_date)
);

-- ============================================================================
-- DATA QUALITY ALERTS
-- ============================================================================

-- Data quality alerts (when quality drops below threshold)
CREATE TABLE IF NOT EXISTS data_quality_alerts (
    alert_id UUID DEFAULT uuid_generate_v4(),
    rule_id UUID REFERENCES data_quality_rules(rule_id) ON DELETE SET NULL,
    check_id UUID, -- References data_quality_checks(check_id) - note: composite PK prevents FK constraint
    
    -- Alert details
    alert_type VARCHAR(100) NOT NULL, -- 'QUALITY_DROP', 'RULE_FAILURE', 'THRESHOLD_BREACH'
    alert_severity VARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    alert_title VARCHAR(255) NOT NULL,
    alert_message TEXT NOT NULL,
    
    -- Alert status
    alert_status VARCHAR(50) DEFAULT 'OPEN', -- 'OPEN', 'ACKNOWLEDGED', 'RESOLVED', 'CLOSED'
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by UUID REFERENCES users(user_id),
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by UUID REFERENCES users(user_id),
    
    -- Alert context
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    domain VARCHAR(50),
    quality_score DECIMAL(5, 4),
    threshold DECIMAL(5, 4),
    
    -- Timestamps
    alert_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    alert_date DATE NOT NULL,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id, alert_date)
) PARTITION BY RANGE (alert_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS data_quality_alerts_2024_01 PARTITION OF data_quality_alerts
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- DATA VALIDATION
-- ============================================================================

-- Data validation results (for individual records)
CREATE TABLE IF NOT EXISTS data_validation_results (
    validation_id UUID DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES data_quality_rules(rule_id) ON DELETE CASCADE,
    
    -- Record identification
    table_name VARCHAR(255) NOT NULL,
    record_id UUID NOT NULL, -- Primary key of validated record
    column_name VARCHAR(255),
    
    -- Validation result
    validation_status VARCHAR(50) NOT NULL, -- 'PASSED', 'FAILED', 'WARNING'
    validation_message TEXT,
    validation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validation_date DATE NOT NULL,
    
    -- Record data (snapshot)
    record_data JSONB, -- Snapshot of record data
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    batch_id UUID,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (validation_id, validation_date)
) PARTITION BY RANGE (validation_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS data_validation_results_2024_01 PARTITION OF data_validation_results
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Data quality rules indexes
CREATE INDEX IF NOT EXISTS idx_quality_rules_code ON data_quality_rules(rule_code);
CREATE INDEX IF NOT EXISTS idx_quality_rules_table ON data_quality_rules(table_name, column_name);
CREATE INDEX IF NOT EXISTS idx_quality_rules_type ON data_quality_rules(rule_type);
CREATE INDEX IF NOT EXISTS idx_quality_rules_active ON data_quality_rules(is_active) WHERE is_active = TRUE;

-- Data quality checks indexes
CREATE INDEX IF NOT EXISTS idx_quality_checks_rule_id ON data_quality_checks(rule_id);
CREATE INDEX IF NOT EXISTS idx_quality_checks_status ON data_quality_checks(check_status);
CREATE INDEX IF NOT EXISTS idx_quality_checks_date ON data_quality_checks(check_date);
CREATE INDEX IF NOT EXISTS idx_quality_checks_timestamp ON data_quality_checks(check_timestamp);
CREATE INDEX IF NOT EXISTS idx_quality_checks_failed ON data_quality_checks(check_status) WHERE check_status = 'FAILED';

-- Data quality metrics indexes
CREATE INDEX IF NOT EXISTS idx_quality_metrics_table ON data_quality_metrics(table_name, column_name);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_date ON data_quality_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_domain ON data_quality_metrics(domain);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_score ON data_quality_metrics(overall_quality_score);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_table_date ON data_quality_metrics(table_name, metric_date);

-- Data quality alerts indexes
CREATE INDEX IF NOT EXISTS idx_quality_alerts_rule_id ON data_quality_alerts(rule_id) WHERE rule_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_quality_alerts_status ON data_quality_alerts(alert_status);
CREATE INDEX IF NOT EXISTS idx_quality_alerts_severity ON data_quality_alerts(alert_severity);
CREATE INDEX IF NOT EXISTS idx_quality_alerts_date ON data_quality_alerts(alert_date);
CREATE INDEX IF NOT EXISTS idx_quality_alerts_open ON data_quality_alerts(alert_status) WHERE alert_status = 'OPEN';

-- Data validation results indexes
CREATE INDEX IF NOT EXISTS idx_validation_results_rule_id ON data_validation_results(rule_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_table_record ON data_validation_results(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_status ON data_validation_results(validation_status);
CREATE INDEX IF NOT EXISTS idx_validation_results_date ON data_validation_results(validation_date);
CREATE INDEX IF NOT EXISTS idx_validation_results_failed ON data_validation_results(validation_status) WHERE validation_status = 'FAILED';

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Triggers to populate date columns from timestamp columns
CREATE OR REPLACE FUNCTION set_data_quality_date_from_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'data_quality_checks' THEN
        NEW.check_date = DATE(NEW.check_timestamp);
    ELSIF TG_TABLE_NAME = 'data_quality_alerts' THEN
        NEW.alert_date = DATE(NEW.alert_timestamp);
    ELSIF TG_TABLE_NAME = 'data_validation_results' THEN
        NEW.validation_date = DATE(NEW.validation_timestamp);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_data_quality_checks_date 
    BEFORE INSERT OR UPDATE ON data_quality_checks
    FOR EACH ROW 
    WHEN (NEW.check_date IS NULL)
    EXECUTE FUNCTION set_data_quality_date_from_timestamp();

CREATE TRIGGER set_data_quality_alerts_date 
    BEFORE INSERT OR UPDATE ON data_quality_alerts
    FOR EACH ROW 
    WHEN (NEW.alert_date IS NULL)
    EXECUTE FUNCTION set_data_quality_date_from_timestamp();

CREATE TRIGGER set_data_validation_results_date 
    BEFORE INSERT OR UPDATE ON data_validation_results
    FOR EACH ROW 
    WHEN (NEW.validation_date IS NULL)
    EXECUTE FUNCTION set_data_quality_date_from_timestamp();

CREATE TRIGGER update_quality_rules_updated_at BEFORE UPDATE ON data_quality_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_quality_metrics_updated_at BEFORE UPDATE ON data_quality_metrics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_quality_alerts_updated_at BEFORE UPDATE ON data_quality_alerts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE data_quality_rules IS 'Data quality validation rules and thresholds';
COMMENT ON TABLE data_quality_checks IS 'Data quality check executions (partitioned by date)';
COMMENT ON TABLE data_quality_metrics IS 'Data quality metrics aggregated by date';
COMMENT ON TABLE data_quality_alerts IS 'Data quality alerts when quality drops (partitioned by date)';
COMMENT ON TABLE data_validation_results IS 'Individual record validation results (partitioned by date)';

