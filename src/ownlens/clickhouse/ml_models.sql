-- ============================================================================
-- OwnLens - ML Model Management Schema (ClickHouse)
-- ============================================================================
-- ML model predictions, monitoring, and A/B testing
-- ============================================================================

USE ownlens_analytics;

-- ML model registry (all models)
CREATE TABLE IF NOT EXISTS ml_model_registry (
    model_id String,  -- UUID as String
    model_name String,
    model_code String,  -- 'churn_prediction', 'article_performance', etc.
    model_version String,  -- Semantic versioning: '1.0.0', '1.1.0', etc.
    model_type String,  -- 'customer', 'editorial', 'company'
    domain String,  -- 'customer', 'editorial', 'company'
    algorithm String,  -- 'XGBoost', 'LightGBM', 'K-Means', 'NMF', etc.
    model_framework String,  -- 'scikit-learn', 'xgboost', 'lightgbm', 'pytorch', 'tensorflow'
    model_format String,  -- 'pickle', 'joblib', 'onnx', 'h5', 'pb'
    model_path String,
    model_storage_type String,  -- 's3', 'minio', 'local', 'mlflow'
    model_storage_url String,
    description String,
    tags Array(String),  -- Array of tags
    metadata String,  -- JSON as String
    training_dataset_path String,
    training_dataset_size UInt32,
    training_date DateTime,
    training_duration_sec UInt32,
    training_environment String,
    performance_metrics String,  -- JSON as String
    feature_importance String,  -- JSON as String
    evaluation_dataset_metrics String,  -- JSON as String
    model_status String DEFAULT 'TRAINING',  -- 'TRAINING', 'VALIDATED', 'STAGING', 'PRODUCTION', 'DEPRECATED', 'ARCHIVED'
    is_active UInt8 DEFAULT 1,  -- Boolean
    deployed_at DateTime,
    deployed_by String,  -- UUID as String
    deployment_environment String,  -- 'development', 'staging', 'production'
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    parent_model_id String,  -- UUID as String (Previous version)
    is_latest_version UInt8 DEFAULT 1,  -- Boolean
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    created_by String  -- UUID as String
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (model_code, model_version)
SETTINGS index_granularity = 8192;

-- Model features (input features for each model)
CREATE TABLE IF NOT EXISTS ml_model_features (
    feature_id String,  -- UUID as String
    model_id String,  -- UUID as String
    feature_name String,
    feature_code String,  -- 'total_sessions', 'avg_engagement_score', etc.
    feature_type String,  -- 'numerical', 'categorical', 'boolean', 'datetime', 'text'
    feature_data_type String,  -- 'INTEGER', 'DECIMAL', 'VARCHAR', 'BOOLEAN', etc.
    description String,
    is_required UInt8 DEFAULT 1,  -- Boolean
    default_value String,
    feature_importance Float64,
    feature_rank UInt16,
    min_value Float64,
    max_value Float64,
    mean_value Float64,
    std_value Float64,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (model_id, feature_rank)
SETTINGS index_granularity = 8192;

-- Model training runs
CREATE TABLE IF NOT EXISTS ml_model_training_runs (
    run_id String,  -- UUID as String
    model_id String,  -- UUID as String
    run_name String,
    run_type String,  -- 'TRAINING', 'TUNING', 'EVALUATION', 'RETRAINING'
    run_status String,  -- 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'
    started_at DateTime,
    completed_at DateTime,
    duration_sec UInt32,
    training_dataset_path String,
    training_dataset_size UInt32,
    validation_dataset_path String,
    validation_dataset_size UInt32,
    test_dataset_path String,
    test_dataset_size UInt32,
    hyperparameters String,  -- JSON as String
    training_metrics String,  -- JSON as String
    validation_metrics String,  -- JSON as String
    test_metrics String,  -- JSON as String
    feature_importance String,  -- JSON as String
    model_path String,
    model_storage_url String,
    error_message String,
    created_by String,  -- UUID as String
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (model_id, started_at)
SETTINGS index_granularity = 8192;

-- ML model predictions
CREATE TABLE IF NOT EXISTS ml_model_predictions (
    prediction_id String,  -- UUID as String
    model_id String,  -- UUID as String
    
    -- Prediction details
    prediction_type String,  -- 'churn', 'conversion', 'article_performance', etc.
    entity_id String,  -- UUID as String (user_id, article_id, etc.)
    entity_type String,  -- 'user', 'article', 'content', etc.
    
    -- Prediction results
    prediction_value Float64,
    prediction_probability Float32,
    prediction_class String,
    prediction_confidence Float32,
    
    -- Input features (snapshot at prediction time)
    input_features String CODEC(ZSTD(3)),  -- JSON
    
    -- Actual outcome (for validation)
    actual_value Float64,
    actual_class String,
    is_correct UInt8,  -- Boolean
    
    -- Timestamps
    predicted_at DateTime DEFAULT now(),
    prediction_date Date,
    actual_observed_at DateTime,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    batch_id String,  -- UUID as String
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (predicted_at, model_id, entity_id, prediction_id)
PARTITION BY toYYYYMM(predicted_at)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- ML model monitoring (track model performance over time)
CREATE TABLE IF NOT EXISTS ml_model_monitoring (
    monitoring_id String,  -- UUID as String
    model_id String,  -- UUID as String
    
    -- Monitoring period
    monitoring_date Date,
    monitoring_start_time DateTime,
    monitoring_end_time DateTime,
    
    -- Prediction metrics
    total_predictions UInt32 DEFAULT 0,
    predictions_per_hour Float32,
    avg_prediction_latency_ms Float32,
    p95_prediction_latency_ms Float32,
    p99_prediction_latency_ms Float32,
    
    -- Accuracy metrics (if actuals available)
    accuracy Float32,
    precision Float32,
    recall Float32,
    f1_score Float32,
    auc_roc Float32,
    
    -- Data drift detection
    data_drift_score Float32,
    feature_drift_scores String CODEC(ZSTD(3)),  -- JSON
    drift_detected UInt8 DEFAULT 0,  -- Boolean
    
    -- Model performance drift
    performance_drift_score Float32,
    performance_drift_detected UInt8 DEFAULT 0,  -- Boolean
    
    -- Prediction distribution (JSON)
    prediction_distribution String CODEC(ZSTD(3)),  -- JSON
    confidence_distribution String CODEC(ZSTD(3)),  -- JSON
    
    -- Error metrics
    error_count UInt32 DEFAULT 0,
    error_rate Float32,
    error_types String CODEC(ZSTD(3)),  -- JSON
    
    -- Alerts
    alerts_triggered UInt16 DEFAULT 0,
    alert_types String CODEC(ZSTD(3)),  -- JSON array
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (monitoring_date, model_id, brand_id)
PARTITION BY toYYYYMM(monitoring_date)
TTL created_at + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- ML model A/B tests
CREATE TABLE IF NOT EXISTS ml_model_ab_tests (
    test_id String,  -- UUID as String
    
    -- Test identification
    test_name String,
    test_code String,
    
    -- Models being tested
    model_a_id String,  -- UUID as String
    model_b_id String,  -- UUID as String
    
    -- Test configuration
    traffic_split_a Float32 DEFAULT 0.5,
    traffic_split_b Float32 DEFAULT 0.5,
    test_start_date Date,
    test_end_date Date,
    min_sample_size UInt32,
    
    -- Test status
    test_status String,  -- 'PLANNED', 'RUNNING', 'COMPLETED', 'CANCELLED'
    
    -- Test results
    model_a_predictions UInt32 DEFAULT 0,
    model_b_predictions UInt32 DEFAULT 0,
    model_a_accuracy Float32,
    model_b_accuracy Float32,
    model_a_metric Float32,
    model_b_metric Float32,
    statistical_significance Float32,
    confidence_level Float32,
    
    -- Winner
    winning_model_id String,  -- UUID as String
    winner_determined_at DateTime,
    
    -- Context
    company_id String,  -- UUID as String
    brand_id String,  -- UUID as String
    created_by String,  -- UUID as String
    
    -- Metadata
    metadata String CODEC(ZSTD(3)),  -- JSON
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (test_id)
SETTINGS index_granularity = 8192;

