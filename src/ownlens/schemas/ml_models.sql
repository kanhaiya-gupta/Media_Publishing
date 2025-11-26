-- ============================================================================
-- OwnLens - ML Model Management Schema
-- ============================================================================
-- ML model registry, versioning, training, deployment, and monitoring
-- ============================================================================

-- ============================================================================
-- ML MODEL REGISTRY
-- ============================================================================

-- ML model registry (all models)
CREATE TABLE IF NOT EXISTS ml_model_registry (
    model_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Model identification
    model_name VARCHAR(255) NOT NULL,
    model_code VARCHAR(100) NOT NULL, -- 'churn_prediction', 'article_performance', etc.
    model_version VARCHAR(50) NOT NULL, -- Semantic versioning: '1.0.0', '1.1.0', etc.
    model_type VARCHAR(100) NOT NULL, -- 'customer', 'editorial', 'company'
    domain VARCHAR(50) NOT NULL, -- 'customer', 'editorial', 'company'
    
    -- Model details
    algorithm VARCHAR(100) NOT NULL, -- 'XGBoost', 'LightGBM', 'K-Means', 'NMF', etc.
    model_framework VARCHAR(50), -- 'scikit-learn', 'xgboost', 'lightgbm', 'pytorch', 'tensorflow'
    model_format VARCHAR(50), -- 'pickle', 'joblib', 'onnx', 'h5', 'pb'
    
    -- Model location
    model_path VARCHAR(1000), -- Path to model file
    model_storage_type VARCHAR(50), -- 's3', 'minio', 'local', 'mlflow'
    model_storage_url VARCHAR(1000), -- Full URL to model
    
    -- Model metadata
    description TEXT,
    tags VARCHAR(100)[], -- Array of tags
    metadata JSONB DEFAULT '{}'::jsonb, -- Additional model metadata
    
    -- Training information
    training_dataset_path VARCHAR(1000),
    training_dataset_size INTEGER, -- Number of samples
    training_date TIMESTAMP WITH TIME ZONE,
    training_duration_sec INTEGER,
    training_environment VARCHAR(255), -- Training environment details
    
    -- Model performance
    performance_metrics JSONB DEFAULT '{}'::jsonb, -- {accuracy: 0.95, precision: 0.92, recall: 0.88, f1: 0.90}
    feature_importance JSONB DEFAULT '{}'::jsonb, -- Top features and importance scores
    evaluation_dataset_metrics JSONB DEFAULT '{}'::jsonb, -- Metrics on evaluation set
    
    -- Model status
    model_status VARCHAR(50) DEFAULT 'TRAINING', -- 'TRAINING', 'VALIDATED', 'STAGING', 'PRODUCTION', 'DEPRECATED', 'ARCHIVED'
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Deployment
    deployed_at TIMESTAMP WITH TIME ZONE,
    deployed_by UUID REFERENCES users(user_id),
    deployment_environment VARCHAR(50), -- 'development', 'staging', 'production'
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Versioning
    parent_model_id UUID REFERENCES ml_model_registry(model_id) ON DELETE SET NULL, -- Previous version
    is_latest_version BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID REFERENCES users(user_id),
    
    CONSTRAINT unique_model_version UNIQUE (model_code, model_version)
);

-- ============================================================================
-- MODEL FEATURES
-- ============================================================================

-- Model features (input features for each model)
CREATE TABLE IF NOT EXISTS ml_model_features (
    feature_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    
    -- Feature details
    feature_name VARCHAR(255) NOT NULL,
    feature_code VARCHAR(100) NOT NULL, -- 'total_sessions', 'avg_engagement_score', etc.
    feature_type VARCHAR(50) NOT NULL, -- 'numerical', 'categorical', 'boolean', 'datetime', 'text'
    feature_data_type VARCHAR(50), -- 'INTEGER', 'DECIMAL', 'VARCHAR', 'BOOLEAN', etc.
    
    -- Feature metadata
    description TEXT,
    is_required BOOLEAN DEFAULT TRUE,
    default_value VARCHAR(255), -- Default value if missing
    feature_importance DECIMAL(10, 6), -- Feature importance score
    feature_rank INTEGER, -- Rank by importance
    
    -- Feature statistics
    min_value DECIMAL(20, 6),
    max_value DECIMAL(20, 6),
    mean_value DECIMAL(20, 6),
    std_value DECIMAL(20, 6),
    null_percentage DECIMAL(5, 4), -- Percentage of null values
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_model_feature UNIQUE (model_id, feature_code)
);

-- ============================================================================
-- MODEL TRAINING RUNS
-- ============================================================================

-- Model training runs (track each training execution)
CREATE TABLE IF NOT EXISTS ml_model_training_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    
    -- Run identification
    run_name VARCHAR(255),
    run_code VARCHAR(100) UNIQUE, -- Unique run identifier
    experiment_name VARCHAR(255), -- MLflow experiment name
    
    -- Training details
    training_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    training_completed_at TIMESTAMP WITH TIME ZONE,
    training_duration_sec INTEGER,
    training_status VARCHAR(50) DEFAULT 'RUNNING', -- 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'
    
    -- Training configuration
    hyperparameters JSONB DEFAULT '{}'::jsonb, -- Model hyperparameters
    training_config JSONB DEFAULT '{}'::jsonb, -- Training configuration
    dataset_config JSONB DEFAULT '{}'::jsonb, -- Dataset configuration
    
    -- Training metrics
    training_metrics JSONB DEFAULT '{}'::jsonb, -- Training set metrics
    validation_metrics JSONB DEFAULT '{}'::jsonb, -- Validation set metrics
    test_metrics JSONB DEFAULT '{}'::jsonb, -- Test set metrics
    
    -- Training results
    model_artifact_path VARCHAR(1000), -- Path to trained model
    training_logs_path VARCHAR(1000), -- Path to training logs
    training_plots_path VARCHAR(1000), -- Path to training plots
    
    -- Error handling
    error_message TEXT,
    error_traceback TEXT,
    
    -- Context
    trained_by UUID REFERENCES users(user_id),
    training_environment VARCHAR(255),
    training_job_id VARCHAR(255), -- Airflow DAG run ID, Spark job ID, etc.
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- MODEL PREDICTIONS
-- ============================================================================

-- Model predictions (track all predictions made)
CREATE TABLE IF NOT EXISTS ml_model_predictions (
    prediction_id UUID DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    
    -- Prediction details
    prediction_type VARCHAR(100) NOT NULL, -- 'churn', 'conversion', 'article_performance', etc.
    entity_id UUID, -- ID of entity being predicted (user_id, article_id, etc.)
    entity_type VARCHAR(100), -- 'user', 'article', 'content', etc.
    
    -- Prediction results
    prediction_value DECIMAL(20, 10), -- Predicted value
    prediction_probability DECIMAL(5, 4), -- Prediction probability (0-1)
    prediction_class VARCHAR(100), -- Predicted class (for classification)
    prediction_confidence DECIMAL(5, 4), -- Model confidence (0-1)
    
    -- Input features (snapshot at prediction time)
    input_features JSONB DEFAULT '{}'::jsonb, -- Input features used
    
    -- Actual outcome (for validation)
    actual_value DECIMAL(20, 10),
    actual_class VARCHAR(100),
    is_correct BOOLEAN, -- Whether prediction was correct
    
    -- Timestamps
    predicted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prediction_date DATE NOT NULL,
    actual_observed_at TIMESTAMP WITH TIME ZONE, -- When actual outcome was observed
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    batch_id UUID, -- Batch processing ID
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (prediction_id, prediction_date)
) PARTITION BY RANGE (prediction_date);

-- Create monthly partitions
CREATE TABLE IF NOT EXISTS ml_model_predictions_2024_01 PARTITION OF ml_model_predictions
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- ============================================================================
-- MODEL MONITORING
-- ============================================================================

-- Model performance monitoring (track model performance over time)
CREATE TABLE IF NOT EXISTS ml_model_monitoring (
    monitoring_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    
    -- Monitoring period
    monitoring_date DATE NOT NULL,
    monitoring_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    monitoring_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Prediction metrics
    total_predictions INTEGER DEFAULT 0,
    predictions_per_hour DECIMAL(10, 2),
    avg_prediction_latency_ms DECIMAL(10, 2),
    p95_prediction_latency_ms DECIMAL(10, 2),
    p99_prediction_latency_ms DECIMAL(10, 2),
    
    -- Accuracy metrics (if actuals available)
    accuracy DECIMAL(5, 4),
    precision DECIMAL(5, 4),
    recall DECIMAL(5, 4),
    f1_score DECIMAL(5, 4),
    auc_roc DECIMAL(5, 4),
    
    -- Data drift detection
    data_drift_score DECIMAL(5, 4), -- Data drift score (0-1, higher = more drift)
    feature_drift_scores JSONB DEFAULT '{}'::jsonb, -- Drift scores per feature
    drift_detected BOOLEAN DEFAULT FALSE,
    
    -- Model performance drift
    performance_drift_score DECIMAL(5, 4), -- Performance drift score
    performance_drift_detected BOOLEAN DEFAULT FALSE,
    
    -- Prediction distribution
    prediction_distribution JSONB DEFAULT '{}'::jsonb, -- Distribution of predictions
    confidence_distribution JSONB DEFAULT '{}'::jsonb, -- Distribution of confidence scores
    
    -- Error metrics
    error_count INTEGER DEFAULT 0,
    error_rate DECIMAL(5, 4), -- Error rate (0-1)
    error_types JSONB DEFAULT '{}'::jsonb, -- Types of errors encountered
    
    -- Alerts
    alerts_triggered INTEGER DEFAULT 0,
    alert_types VARCHAR(100)[], -- Array of alert types
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_model_date UNIQUE (model_id, monitoring_date)
);

-- ============================================================================
-- MODEL A/B TESTS
-- ============================================================================

-- Model A/B testing (compare model versions)
CREATE TABLE IF NOT EXISTS ml_model_ab_tests (
    test_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Test identification
    test_name VARCHAR(255) NOT NULL,
    test_code VARCHAR(100) UNIQUE NOT NULL,
    
    -- Models being tested
    model_a_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    model_b_id UUID NOT NULL REFERENCES ml_model_registry(model_id) ON DELETE CASCADE,
    
    -- Test configuration
    traffic_split_a DECIMAL(5, 4) DEFAULT 0.5, -- Traffic percentage for model A (0-1)
    traffic_split_b DECIMAL(5, 4) DEFAULT 0.5, -- Traffic percentage for model B (0-1)
    test_start_date DATE NOT NULL,
    test_end_date DATE,
    min_sample_size INTEGER, -- Minimum sample size for statistical significance
    
    -- Test status
    test_status VARCHAR(50) DEFAULT 'PLANNED', -- 'PLANNED', 'RUNNING', 'COMPLETED', 'CANCELLED'
    
    -- Test results
    model_a_predictions INTEGER DEFAULT 0,
    model_b_predictions INTEGER DEFAULT 0,
    model_a_accuracy DECIMAL(5, 4),
    model_b_accuracy DECIMAL(5, 4),
    model_a_metric DECIMAL(10, 6), -- Primary metric (e.g., F1 score)
    model_b_metric DECIMAL(10, 6),
    statistical_significance DECIMAL(5, 4), -- P-value
    confidence_level DECIMAL(5, 4), -- Confidence level (0-1)
    
    -- Winner
    winning_model_id UUID REFERENCES ml_model_registry(model_id),
    winner_determined_at TIMESTAMP WITH TIME ZONE,
    
    -- Context
    company_id UUID NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
    brand_id UUID NOT NULL REFERENCES brands(brand_id) ON DELETE CASCADE,
    created_by UUID REFERENCES users(user_id),
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Model registry indexes
CREATE INDEX IF NOT EXISTS idx_model_registry_code ON ml_model_registry(model_code);
CREATE INDEX IF NOT EXISTS idx_model_registry_version ON ml_model_registry(model_code, model_version);
CREATE INDEX IF NOT EXISTS idx_model_registry_type ON ml_model_registry(model_type, domain);
CREATE INDEX IF NOT EXISTS idx_model_registry_status ON ml_model_registry(model_status);
CREATE INDEX IF NOT EXISTS idx_model_registry_active ON ml_model_registry(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_model_registry_latest ON ml_model_registry(model_code, is_latest_version) WHERE is_latest_version = TRUE;
CREATE INDEX IF NOT EXISTS idx_model_registry_company ON ml_model_registry(company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_model_registry_brand ON ml_model_registry(brand_id) WHERE brand_id IS NOT NULL;

-- Model features indexes
CREATE INDEX IF NOT EXISTS idx_model_features_model_id ON ml_model_features(model_id);
CREATE INDEX IF NOT EXISTS idx_model_features_code ON ml_model_features(model_id, feature_code);
CREATE INDEX IF NOT EXISTS idx_model_features_importance ON ml_model_features(model_id, feature_importance DESC NULLS LAST);

-- Training runs indexes
CREATE INDEX IF NOT EXISTS idx_training_runs_model_id ON ml_model_training_runs(model_id);
CREATE INDEX IF NOT EXISTS idx_training_runs_status ON ml_model_training_runs(training_status);
CREATE INDEX IF NOT EXISTS idx_training_runs_started ON ml_model_training_runs(training_started_at);
CREATE INDEX IF NOT EXISTS idx_training_runs_code ON ml_model_training_runs(run_code) WHERE run_code IS NOT NULL;

-- Predictions indexes
CREATE INDEX IF NOT EXISTS idx_model_predictions_model_id ON ml_model_predictions(model_id);
CREATE INDEX IF NOT EXISTS idx_model_predictions_type ON ml_model_predictions(prediction_type);
CREATE INDEX IF NOT EXISTS idx_model_predictions_entity ON ml_model_predictions(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_model_predictions_date ON ml_model_predictions(prediction_date);
CREATE INDEX IF NOT EXISTS idx_model_predictions_timestamp ON ml_model_predictions(predicted_at);

-- Model monitoring indexes
CREATE INDEX IF NOT EXISTS idx_model_monitoring_model_id ON ml_model_monitoring(model_id);
CREATE INDEX IF NOT EXISTS idx_model_monitoring_date ON ml_model_monitoring(monitoring_date);
CREATE INDEX IF NOT EXISTS idx_model_monitoring_drift ON ml_model_monitoring(drift_detected) WHERE drift_detected = TRUE;
CREATE INDEX IF NOT EXISTS idx_model_monitoring_model_date ON ml_model_monitoring(model_id, monitoring_date);

-- A/B tests indexes
CREATE INDEX IF NOT EXISTS idx_ab_tests_status ON ml_model_ab_tests(test_status);
CREATE INDEX IF NOT EXISTS idx_ab_tests_dates ON ml_model_ab_tests(test_start_date, test_end_date);
CREATE INDEX IF NOT EXISTS idx_ab_tests_running ON ml_model_ab_tests(test_status) WHERE test_status = 'RUNNING';

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger to populate prediction_date from predicted_at
CREATE OR REPLACE FUNCTION set_ml_prediction_date()
RETURNS TRIGGER AS $$
BEGIN
    NEW.prediction_date = DATE(NEW.predicted_at);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_ml_model_predictions_date 
    BEFORE INSERT OR UPDATE ON ml_model_predictions
    FOR EACH ROW 
    WHEN (NEW.prediction_date IS NULL)
    EXECUTE FUNCTION set_ml_prediction_date();

CREATE TRIGGER update_model_registry_updated_at BEFORE UPDATE ON ml_model_registry
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_training_runs_updated_at BEFORE UPDATE ON ml_model_training_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_model_monitoring_updated_at BEFORE UPDATE ON ml_model_monitoring
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ab_tests_updated_at BEFORE UPDATE ON ml_model_ab_tests
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE ml_model_registry IS 'ML model registry with versioning and metadata';
COMMENT ON TABLE ml_model_features IS 'Input features for each ML model';
COMMENT ON TABLE ml_model_training_runs IS 'Model training runs and execution tracking';
COMMENT ON TABLE ml_model_predictions IS 'Model predictions tracking (partitioned by date)';
COMMENT ON TABLE ml_model_monitoring IS 'Model performance monitoring and drift detection';
COMMENT ON TABLE ml_model_ab_tests IS 'Model A/B testing for comparing model versions';

